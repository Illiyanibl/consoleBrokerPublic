#!/usr/bin/env python3
"""
Devolution PTY Broker — persistent terminal sessions.

Runs CLI programs (Claude Code, Codex, bash, etc.) in a PTY that survives
SSH disconnects. On reconnect, replays missed output from a ring buffer.

Usage:
    broker.py session <name> [command...] [--cwd PATH] [--suspend-after SEC]
                                                        — connect to session (create if needed)
    broker.py list [--json]                             — list active sessions
    broker.py kill <name>                               — terminate a session

--suspend-after: seconds of idle (no clients) before the child process group
    is SIGSTOP'd; default 300, pass 0 to never suspend (for builds, dev servers).
    Only honored on session creation — ignored if the session already exists.

Session logic:
    - Session doesn't exist        → create + attach
    - Session exists, process alive → attach (command argument ignored)
    - Session exists, process dead  → cleanup, create new + attach

Architecture:
    session → forks a daemon that holds a PTY + Unix socket, then attaches
    daemon  → reads PTY output into ring buffer, serves clients via socket
    attach  → connects to the daemon's socket, bridges to current terminal

Protocol (over Unix socket):
    1. Client connects
    2. Client sends terminal size: 'S' + rows(2B) + cols(2B)
    3. Server sends ring buffer: length(4B) + data
    4. Bidirectional raw I/O (PTY ↔ client terminal)
    5. In-band control: 0x00 prefix byte
       0x00 + 'R' + rows(2B) + cols(2B) = resize PTY
       0x00 + 0x00 = literal 0x00 byte (escape)

Detach: Ctrl+\\ (session keeps running in background)
"""

import os
import sys
import pty
import select
import socket
import signal
import struct
import fcntl
import termios
import tty
import time
import json
import logging
import logging.handlers
from pathlib import Path

# --- Config ---

SOCKET_DIR = Path("/tmp/devolution-broker")
LOG_DIR = Path("/tmp/devolution-broker/logs")
BUFFER_SIZE = 256 * 1024  # 256 KB ring buffer
CTRL_PREFIX = b'\x00'
DETACH_KEY = b'\x1c'  # Ctrl+\ to detach
AI_COMMANDS = {"claude", "codex"}  # commands that produce AI sessions
CLAUDE_PROJECTS_DIR = Path.home() / ".claude" / "projects"
AI_SCAN_INTERVAL = 30  # seconds between AI session scans
SUSPEND_GRACE_SEC = 300  # default grace period (sec) before SIGSTOP'ing a detached session; 0 = never suspend
SOCKET_CHECK_INTERVAL = 5  # seconds between checks that our .sock file still exists
LOG_MAX_BYTES = 10 * 1024 * 1024  # 10 MiB per log file (rotation cap)
LOG_BACKUP_COUNT = 3              # keep 3 rotated backups → ~40 MiB ceiling per session
OVERFLOW_FLUSH_INTERVAL = 1.0     # seconds: aggregate ring-buffer overflow warnings into one line

# Per-CLI overrides applied at child exec to suppress background noise (auto-updaters
# etc.) that would otherwise spam the PTY when nobody is reading. Matched by basename(cmd[0]).
CLI_OVERRIDES: dict[str, dict] = {
    "claude":  {"env": {"DISABLE_AUTOUPDATER": "1"}},
    "copilot": {"args": ["--no-auto-update"]},
}


def apply_cli_overrides(command: list[str], env: dict[str, str]) -> list[str]:
    """Merge env/args overrides for known CLIs into place. Returns the final command."""
    if not command:
        return command
    name = os.path.basename(command[0])
    override = CLI_OVERRIDES.get(name)
    if not override:
        return command

    for k, v in override.get("env", {}).items():
        env[k] = v

    extra_args = [a for a in override.get("args", []) if a not in command]
    if extra_args:
        command = [command[0]] + extra_args + command[1:]
    return command


def setup_logger(name: str) -> logging.Logger:
    """Create a rotating file logger for a session daemon.

    Rotation is mandatory: a runaway WARNING source (e.g. ring buffer overflow
    on a no-reader PTY) can otherwise grow the log file without bound — the
    2026-05-06 incident produced a 74 GiB log that filled the host disk.
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(f"broker.{name}")
    logger.setLevel(logging.DEBUG)
    if not logger.handlers:
        handler = logging.handlers.RotatingFileHandler(
            LOG_DIR / f"{name}.log",
            maxBytes=LOG_MAX_BYTES,
            backupCount=LOG_BACKUP_COUNT,
        )
        handler.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        ))
        logger.addHandler(handler)
    return logger


class RingBuffer:
    """Dual-screen ring buffer for terminal output.

    Maintains separate ring buffers for the normal screen and the alternate
    screen (used by TUI apps like Claude Code, vim, htop). Detects xterm-style
    screen-switch escape sequences in the input stream and routes data to the
    correct buffer. On replay, emits a terminal reset, restores the current
    screen mode, and dumps the relevant buffer.

    This solves the "black screen on reattach" problem for alt-screen TUIs:
    a single buffer would either replay the wrong screen contents or get cut
    mid-redraw, leaving the client with a blank/garbled display.
    """

    # Mode-switch sequences for the alternate screen buffer.
    # \033[?1049h/l: modern xterm (Claude Code, Copilot, recent vim).
    # \033[?47h/l:   legacy alt-screen (older vim builds).
    # \033[?1047h/l: intermediate variant.
    SWITCH_TO_ALT = (b'\033[?1049h', b'\033[?47h', b'\033[?1047h')
    SWITCH_TO_NORMAL = (b'\033[?1049l', b'\033[?47l', b'\033[?1047l')
    SWITCH_SEQUENCES = SWITCH_TO_ALT + SWITCH_TO_NORMAL
    MAX_SWITCH_LEN = max(len(s) for s in SWITCH_SEQUENCES)  # 8

    # Terminal sequences that indicate a "safe" replay point in the normal
    # buffer — after these, the screen is fully cleared, so anything before
    # is irrelevant for replay.
    SAFE_SEQUENCES = (
        b'\033c',            # Full terminal reset (RIS)
        b'\033[2J\033[H',    # Clear screen + cursor home (common combo)
        b'\033[H\033[2J',    # Cursor home + clear screen (alternate order)
    )

    def __init__(self, capacity=BUFFER_SIZE):
        self.capacity = capacity
        self.normal_buf = bytearray()
        self.alt_buf = bytearray()
        self.mode = "normal"  # "normal" or "alt"
        # Last few bytes that *might* be the start of a switch sequence
        # straddling a chunk boundary. Held back until the next write().
        self._carry = b''

    def write(self, data: bytes) -> int:
        """Append data, routing to the correct buffer based on screen mode.

        Returns the number of bytes dropped due to overflow (across both
        buffers). Mode switches are detected inline; the switch escape itself
        is consumed (not stored) — replay re-emits it explicitly.
        """
        if not data:
            return 0

        data = self._carry + data
        self._carry = b''

        # Hold back any tail that could be the start of a switch sequence
        # straddling a chunk boundary (e.g. PTY chunk ends mid-"\033[?1049").
        # Only carry if it's a *strict* prefix — full matches are processed.
        max_n = min(self.MAX_SWITCH_LEN - 1, len(data))
        for n in range(max_n, 0, -1):
            tail = data[-n:]
            if any(seq.startswith(tail) and seq != tail for seq in self.SWITCH_SEQUENCES):
                self._carry = tail
                data = data[:-n]
                break

        if not data:
            return 0

        # Fast path: no escape character at all → no switch possible.
        if b'\033' not in data:
            return self._append_current(data)

        overflow = 0
        pos = 0
        n = len(data)
        while pos < n:
            next_pos = -1
            next_seq = b''
            for seq in self.SWITCH_SEQUENCES:
                idx = data.find(seq, pos)
                if idx != -1 and (next_pos == -1 or idx < next_pos):
                    next_pos = idx
                    next_seq = seq

            if next_pos == -1:
                overflow += self._append_current(data[pos:])
                break

            if next_pos > pos:
                overflow += self._append_current(data[pos:next_pos])

            # Apply mode switch. The escape itself is dropped — replay
            # re-emits a canonical one based on self.mode.
            if next_seq in self.SWITCH_TO_ALT:
                if self.mode == "normal":
                    # New alt-screen session starts blank — drop stale alt frames.
                    self.alt_buf.clear()
                self.mode = "alt"
            else:
                self.mode = "normal"

            pos = next_pos + len(next_seq)

        return overflow

    def _append_current(self, data) -> int:
        """Append to the buffer for the current screen mode, trimming to capacity."""
        buf = self.alt_buf if self.mode == "alt" else self.normal_buf
        buf.extend(data)
        if len(buf) > self.capacity:
            overflow = len(buf) - self.capacity
            del buf[:overflow]
            return overflow
        return 0

    def read_for_replay(self) -> bytes:
        """Build the replay payload for a newly-attached client.

        Always starts with a full reset (\\033c\\033[0m) to guarantee a known
        terminal state. If the session is currently on the alt screen, also
        re-enters alt-screen mode before dumping the alt buffer; otherwise
        dumps the normal buffer from the last safe replay point.
        """
        TERM_RESET = b'\033c\033[0m'
        if self.mode == "alt":
            return TERM_RESET + b'\033[?1049h' + bytes(self.alt_buf)
        return TERM_RESET + self._safe_normal()

    def _safe_normal(self) -> bytes:
        """Trim the normal buffer to the last full-screen-clear point."""
        data = bytes(self.normal_buf)
        if not data:
            return data
        best_pos = -1
        for seq in self.SAFE_SEQUENCES:
            pos = data.rfind(seq)
            if pos > best_pos:
                best_pos = pos
        if best_pos > 0:
            return data[best_pos:]
        return data

    def size(self) -> int:
        return len(self.normal_buf) + len(self.alt_buf)


class SessionDaemon:
    """Daemon process that holds a PTY and serves clients via Unix socket."""

    def __init__(self, name: str, command: list[str], cwd: str | None = None,
                 suspend_after: int = SUSPEND_GRACE_SEC):
        self.name = name
        self.command = command
        self.cwd = cwd or os.getcwd()
        self.socket_path = SOCKET_DIR / f"{name}.sock"
        self.pid_path = SOCKET_DIR / f"{name}.pid"
        self.meta_path = SOCKET_DIR / f"{name}.json"
        self.buffer = RingBuffer()
        self.clients: list[socket.socket] = []
        self.child_pid = -1
        self.master_fd = -1
        self.running = True
        self.is_ai_command = command[0] in AI_COMMANDS if command else False
        self.last_ai_scan = 0.0
        self.log: logging.Logger | None = None  # initialized after fork
        # Auto-suspend state: freeze child's process group when no clients read.
        # suspend_after == 0 disables the mechanism entirely (e.g. builds, dev servers).
        self.suspend_after = suspend_after
        self.was_attached = False
        self.suspended = False
        self.last_disconnect_time: float | None = None
        # Inode of our socket file, captured after bind. If the file disappears
        # or is replaced (e.g. Hub deletes metadata), we self-terminate instead
        # of lingering as an unreachable orphan.
        self.socket_inode: int | None = None
        self.last_socket_check = 0.0
        # Aggregated overflow accounting. A no-reader PTY producing data faster
        # than the ring buffer can drain triggers _read_pty thousands of times
        # per second; logging each overflow individually flooded the file with
        # ~1.13 billion identical WARNING lines (2026-05-06 incident). We now
        # accumulate and emit one rolled-up line per OVERFLOW_FLUSH_INTERVAL.
        self._overflow_bytes = 0
        self._overflow_events = 0
        self._overflow_last_flush = 0.0

    def start(self):
        """Fork into background daemon and start the session."""
        # First fork
        pid = os.fork()
        if pid > 0:
            # Parent waits briefly for daemon to initialize
            time.sleep(0.3)
            return

        # Child — become session leader
        os.setsid()

        # Second fork (prevent acquiring controlling terminal)
        pid = os.fork()
        if pid > 0:
            os._exit(0)

        # Daemon process
        self._write_meta()
        self._run_daemon()

    def _write_meta(self):
        """Save session metadata."""
        meta = {
            "name": self.name,
            "command": self.command,
            "cwd": self.cwd,
            "pid": os.getpid(),
            "created": time.time(),
            "suspend_after": self.suspend_after,
            "ai_sessions": [],
        }
        self.meta_path.write_text(json.dumps(meta))
        self.pid_path.write_text(str(os.getpid()))

    def _run_daemon(self):
        """Main daemon loop: PTY + socket server."""
        self.log = setup_logger(self.name)
        self.log.info("Daemon starting: name=%s cmd=%s cwd=%s pid=%d",
                      self.name, self.command, self.cwd, os.getpid())

        # Create PTY and fork child process
        self.child_pid, self.master_fd = pty.fork()

        if self.child_pid == 0:
            # Child — chdir and exec the command
            os.chdir(self.cwd)
            env = os.environ.copy()
            env["TERM"] = "xterm-256color"
            env["DEVOLUTION_BROKER_META"] = str(self.meta_path)
            env["DEVOLUTION_BROKER_SESSION"] = self.name
            final_cmd = apply_cli_overrides(self.command, env)
            os.execvpe(final_cmd[0], final_cmd, env)

        self.log.info("Child forked: child_pid=%d master_fd=%d", self.child_pid, self.master_fd)

        # Daemon — set up socket
        signal.signal(signal.SIGCHLD, self._on_child_exit)
        signal.signal(signal.SIGTERM, self._on_terminate)

        # Non-blocking master fd
        flags = fcntl.fcntl(self.master_fd, fcntl.F_GETFL)
        fcntl.fcntl(self.master_fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)

        # Create Unix socket
        self.socket_path.unlink(missing_ok=True)
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(str(self.socket_path))
        server.listen(2)
        server.setblocking(False)

        try:
            self.socket_inode = os.stat(self.socket_path).st_ino
        except OSError:
            self.socket_inode = None

        self.log.info("Socket ready at %s", self.socket_path)
        try:
            self._event_loop(server)
        except Exception as e:
            self.log.error("Event loop crashed: %s", e, exc_info=True)
        finally:
            self.log.info("Event loop ended, running=%s", self.running)
            self._cleanup(server)

    def _event_loop(self, server: socket.socket):
        """Main select() loop."""
        self.log.info("Event loop started")
        while self.running:
            read_fds = [server, self.master_fd] + self.clients
            try:
                readable, _, _ = select.select(read_fds, [], [], 1.0)
            except (select.error, ValueError) as e:
                self.log.warning("select() error: %s", e)
                continue

            for fd in readable:
                if fd is server:
                    self._accept_client(server)
                elif fd is self.master_fd:
                    self._read_pty()
                elif fd in self.clients:
                    self._read_client(fd)

            # Periodically scan for AI sessions
            if self.is_ai_command:
                now = time.time()
                if now - self.last_ai_scan > AI_SCAN_INTERVAL:
                    self.last_ai_scan = now
                    self._scan_ai_sessions()

            # Auto-suspend: freeze child's process group if idle past grace period.
            # Only engages after at least one client attached (preserves unattended background jobs).
            # suspend_after == 0 means the mechanism is disabled for this session.
            if (self.suspend_after > 0 and self.was_attached and not self.suspended
                    and not self.clients and self.last_disconnect_time is not None
                    and time.time() - self.last_disconnect_time >= self.suspend_after):
                self._suspend_child()

            # Self-shutdown if our socket file was unlinked or replaced externally
            # (e.g. Hub "delete connection" wiping /tmp/devolution-broker). Without
            # this, the daemon keeps running but is unreachable — an orphan.
            if time.time() - self.last_socket_check >= SOCKET_CHECK_INTERVAL:
                self.last_socket_check = time.time()
                if not self._own_socket_present():
                    self.log.info("Socket file missing or replaced, self-terminating")
                    self.running = False

    def _accept_client(self, server: socket.socket):
        """Accept new client connection and send ring buffer."""
        try:
            conn, _ = server.accept()
        except OSError as e:
            self.log.warning("Accept failed: %s", e)
            return
        self.log.info("Client connected (total: %d)", len(self.clients) + 1)

        # Resume the child's process group if it was frozen waiting for a reader
        if self.suspended:
            self._resume_child()
        self.last_disconnect_time = None

        try:
            # Read terminal size from client: 'S' + rows(2B) + cols(2B)
            header = self._recv_exact(conn, 5, timeout=2.0)
            if header and header[0:1] == b'S':
                rows, cols = struct.unpack('!HH', header[1:5])
                self.log.debug("Client sent resize on connect: %dx%d", cols, rows)
                self._resize_pty(rows, cols)
            else:
                self.log.warning("Client handshake failed: header=%r", header)

            # Send replay payload (includes terminal reset + alt-screen
            # restoration if needed — see RingBuffer.read_for_replay).
            payload = self.buffer.read_for_replay()
            length = struct.pack('!I', len(payload))
            conn.sendall(length + payload)
            self.log.debug("Sent replay: %d bytes (mode=%s, normal=%d alt=%d)",
                           len(payload), self.buffer.mode,
                           len(self.buffer.normal_buf), len(self.buffer.alt_buf))

            conn.setblocking(False)
            self.clients.append(conn)
            self.was_attached = True
        except (OSError, BrokenPipeError) as e:
            self.log.warning("Client dropped during handshake: %s", e)
            conn.close()

    def _recv_exact(self, sock: socket.socket, n: int, timeout: float = 2.0) -> bytes | None:
        """Receive exactly n bytes with timeout."""
        sock.settimeout(timeout)
        try:
            data = b''
            while len(data) < n:
                chunk = sock.recv(n - len(data))
                if not chunk:
                    return None
                data += chunk
            return data
        except socket.timeout:
            return None

    def _read_pty(self):
        """Read from PTY, buffer, and forward to all clients."""
        try:
            data = os.read(self.master_fd, 4096)
        except BlockingIOError:
            # Non-blocking fd had no data (spurious select wakeup) — not an error
            return
        except OSError as e:
            self.log.error("PTY read error (stopping): %s", e)
            self.running = False
            return

        if not data:
            self.log.info("PTY returned empty data (child exited)")
            self.running = False
            return

        overflow = self.buffer.write(data)
        if overflow:
            self._overflow_bytes += overflow
            self._overflow_events += 1
        self._maybe_flush_overflow()

        dead = []
        for client in self.clients:
            try:
                client.sendall(data)
            except (OSError, BrokenPipeError):
                dead.append(client)

        for client in dead:
            self.clients.remove(client)
            client.close()
            self.log.info("Client disconnected (broken pipe, remaining: %d)", len(self.clients))
        if dead and self.was_attached and not self.clients:
            self.last_disconnect_time = time.time()

    def _maybe_flush_overflow(self, force: bool = False):
        """Emit one aggregated overflow WARNING per flush interval (or on shutdown)."""
        if self._overflow_events == 0:
            return
        now = time.time()
        if not force and (now - self._overflow_last_flush) < OVERFLOW_FLUSH_INTERVAL:
            return
        # First-ever flush after process start: window is meaningless; report the
        # interval rather than seconds-since-epoch.
        window = (now - self._overflow_last_flush) if self._overflow_last_flush else OVERFLOW_FLUSH_INTERVAL
        self.log.warning(
            "Ring buffer overflow: dropped %d bytes in last %.1fs (%d events)",
            self._overflow_bytes, window, self._overflow_events,
        )
        self._overflow_bytes = 0
        self._overflow_events = 0
        self._overflow_last_flush = now

    def _read_client(self, client: socket.socket):
        """Read input from client and forward to PTY."""
        try:
            data = client.recv(4096)
        except OSError as e:
            self.log.warning("Client recv error: %s", e)
            data = b''

        if not data:
            self.clients.remove(client)
            client.close()
            self.log.info("Client disconnected (EOF, remaining: %d)", len(self.clients))
            if self.was_attached and not self.clients:
                self.last_disconnect_time = time.time()
            return

        # Process control sequences (0x00 prefix)
        i = 0
        output = bytearray()
        while i < len(data):
            if data[i:i+1] == CTRL_PREFIX and i + 1 < len(data):
                ctrl = data[i+1:i+2]
                if ctrl == b'R' and i + 5 < len(data):
                    # Resize: 0x00 + 'R' + rows(2B) + cols(2B)
                    rows, cols = struct.unpack('!HH', data[i+2:i+6])
                    self.log.debug("Client resize request: %dx%d", cols, rows)
                    self._resize_pty(rows, cols)
                    i += 6
                elif ctrl == CTRL_PREFIX:
                    # Escaped 0x00
                    output.append(0)
                    i += 2
                else:
                    output.append(data[i])
                    i += 1
            else:
                output.append(data[i])
                i += 1

        if output:
            try:
                os.write(self.master_fd, bytes(output))
            except OSError as e:
                self.log.error("PTY write error: %s", e)

    def _resize_pty(self, rows: int, cols: int):
        """Resize the PTY."""
        try:
            winsize = struct.pack('HHHH', rows, cols, 0, 0)
            fcntl.ioctl(self.master_fd, termios.TIOCSWINSZ, winsize)
            os.kill(self.child_pid, signal.SIGWINCH)
            self.log.debug("PTY resized to %dx%d", cols, rows)
        except (OSError, ProcessLookupError) as e:
            self.log.warning("PTY resize failed (%dx%d): %s", cols, rows, e)

    def _scan_ai_sessions(self):
        """Scan ~/.claude/projects/ for AI session IDs matching this session's CWD."""
        try:
            # Convert CWD path to Claude's folder naming: /root/projects/foo → -root-projects-foo
            claude_dir_name = self.cwd.replace("/", "-")
            if claude_dir_name.startswith("-"):
                pass  # already starts with dash
            else:
                claude_dir_name = "-" + claude_dir_name
            claude_project_dir = CLAUDE_PROJECTS_DIR / claude_dir_name

            if not claude_project_dir.exists():
                return

            # Collect session IDs from .jsonl files
            session_ids = []
            for f in sorted(claude_project_dir.glob("*.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True):
                sid = f.stem
                mtime = f.stat().st_mtime
                session_ids.append({"id": sid, "last_active": mtime})

            if not session_ids:
                return

            # Update meta file
            try:
                meta = json.loads(self.meta_path.read_text())
            except (json.JSONDecodeError, FileNotFoundError):
                return

            meta["ai_sessions"] = session_ids
            self.meta_path.write_text(json.dumps(meta))
            self.log.debug("AI scan: found %d sessions in %s", len(session_ids), claude_dir_name)
        except OSError as e:
            self.log.warning("AI scan error: %s", e)

    def _suspend_child(self):
        """SIGSTOP the child's process group to stop runaway writes while detached."""
        if self.suspended or self.child_pid <= 0:
            return
        try:
            os.killpg(self.child_pid, signal.SIGSTOP)
            self.suspended = True
            self.log.info("Child suspended (SIGSTOP) after %ds idle: pgid=%d",
                          self.suspend_after, self.child_pid)
        except (OSError, ProcessLookupError) as e:
            self.log.warning("SIGSTOP failed: %s", e)

    def _resume_child(self):
        """SIGCONT the child's process group when a reader reconnects."""
        if not self.suspended or self.child_pid <= 0:
            return
        try:
            os.killpg(self.child_pid, signal.SIGCONT)
            self.suspended = False
            self.log.info("Child resumed (SIGCONT): pgid=%d", self.child_pid)
        except (OSError, ProcessLookupError) as e:
            self.log.warning("SIGCONT failed: %s", e)

    def _on_child_exit(self, signum, frame):
        """Handle child process exit. Ignores SIGCHLD from stop/continue state changes."""
        try:
            pid, status = os.waitpid(self.child_pid, os.WNOHANG)
        except ChildProcessError:
            if self.log:
                self.log.info("Child already reaped")
            self.running = False
            return
        # pid == 0 means the child is still alive (SIGCHLD fired for stop/continue,
        # not for exit). Do NOT shut the daemon down in that case.
        if pid == 0:
            return
        if self.log:
            self.log.info("Child exited: pid=%d status=%d", pid, status)
        self.running = False

    def _on_terminate(self, signum, frame):
        """Handle SIGTERM."""
        if self.log:
            self.log.info("Received SIGTERM, shutting down")
        self.running = False

    def _own_socket_present(self) -> bool:
        """True if self.socket_path still points at the inode we bound to."""
        if self.socket_inode is None:
            return True  # unknown baseline — don't self-kill based on it
        try:
            return os.stat(self.socket_path).st_ino == self.socket_inode
        except OSError:
            return False

    def _cleanup(self, server: socket.socket):
        """Clean up resources."""
        if self.log:
            self._maybe_flush_overflow(force=True)
            self.log.info("Cleanup: closing %d clients, removing socket files", len(self.clients))
        for client in self.clients:
            try:
                client.close()
            except OSError:
                pass

        try:
            server.close()
        except OSError:
            pass

        try:
            os.close(self.master_fd)
        except OSError:
            pass

        try:
            # Unfreeze first so SIGTERM can be delivered & handled, not queued behind SIGSTOP
            if self.suspended:
                os.killpg(self.child_pid, signal.SIGCONT)
            os.kill(self.child_pid, signal.SIGTERM)
        except (OSError, ProcessLookupError):
            pass

        self.socket_path.unlink(missing_ok=True)
        self.pid_path.unlink(missing_ok=True)
        self.meta_path.unlink(missing_ok=True)


def is_session_alive(name: str) -> bool:
    """Check if a session exists and its daemon process is alive."""
    pid_path = SOCKET_DIR / f"{name}.pid"
    socket_path = SOCKET_DIR / f"{name}.sock"

    if not pid_path.exists() or not socket_path.exists():
        return False

    try:
        pid = int(pid_path.read_text().strip())
        os.kill(pid, 0)  # signal 0 = check if alive
        return True
    except (ProcessLookupError, ValueError, OSError):
        return False


def cleanup_dead_session(name: str):
    """Remove stale files from a dead session.

    If a daemon process for this name is still alive but unreachable (its socket
    was deleted externally, e.g. by the Hub), send it SIGTERM so the upcoming
    new daemon has a clean slate and the ghost does not keep holding its child.
    """
    # Try pidfile first, then fall back to /proc scan — the file may already be gone.
    candidate_pids: set[int] = set()
    pid_path = SOCKET_DIR / f"{name}.pid"
    if pid_path.exists():
        try:
            candidate_pids.add(int(pid_path.read_text().strip()))
        except (OSError, ValueError):
            pass

    try:
        proc_root = Path("/proc")
        if proc_root.exists():
            for proc_entry in proc_root.iterdir():
                if not proc_entry.name.isdigit():
                    continue
                pid = int(proc_entry.name)
                argv = _read_proc_cmdline(pid)
                if not _is_broker_session_argv(argv) or argv[3] != name:
                    continue
                if _read_proc_ppid(pid) != 1:
                    continue
                candidate_pids.add(pid)
    except OSError:
        pass

    # Only signal the process if its socket file is absent — otherwise a healthy
    # daemon is running and this call is a race (e.g. concurrent attach path).
    sock_exists = (SOCKET_DIR / f"{name}.sock").exists()
    if not sock_exists:
        for pid in candidate_pids:
            try:
                os.kill(pid, signal.SIGTERM)
            except (ProcessLookupError, PermissionError, OSError):
                pass

    for suffix in [".sock", ".pid", ".json"]:
        (SOCKET_DIR / f"{name}{suffix}").unlink(missing_ok=True)


def _read_proc_cmdline(pid: int) -> list[str]:
    """Read process argv from /proc/<pid>/cmdline."""
    try:
        raw = Path(f"/proc/{pid}/cmdline").read_bytes()
    except OSError:
        return []
    if not raw:
        return []
    return [chunk.decode(errors="replace") for chunk in raw.split(b"\x00") if chunk]


_BOOT_TIME: float | None = None
_CLK_TCK: int | None = None


def _proc_start_time(pid: int) -> float | None:
    """Return real process start time (unix seconds) from /proc/<pid>/stat.

    On /procfs inode times (st_ctime/st_mtime) reflect access time, not process
    start, so they cannot be trusted. Compute from stat field 22 (starttime in
    jiffies since boot) plus /proc/stat btime.
    """
    global _BOOT_TIME, _CLK_TCK
    try:
        if _BOOT_TIME is None:
            for line in Path("/proc/stat").read_text().splitlines():
                if line.startswith("btime "):
                    _BOOT_TIME = float(line.split()[1])
                    break
        if _CLK_TCK is None:
            _CLK_TCK = os.sysconf("SC_CLK_TCK") or 100
        if _BOOT_TIME is None:
            return None

        raw = Path(f"/proc/{pid}/stat").read_text()
        # Field 2 (comm) is parenthesized and can contain spaces, so split on
        # the last ')' to find the post-comm tail.
        tail = raw.rsplit(")", 1)[1].split()
        # tail[0] = state, tail[1] = ppid, ..., starttime is field 22 overall,
        # which is tail index 19 (22 - 2 for pid/comm, minus 1 zero-based).
        starttime_jiffies = int(tail[19])
        return _BOOT_TIME + starttime_jiffies / _CLK_TCK
    except (OSError, ValueError, IndexError):
        return None


def _read_proc_ppid(pid: int) -> int | None:
    """Read PPid from /proc/<pid>/status."""
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("PPid:"):
                return int(line.split()[1])
    except (OSError, ValueError, IndexError):
        return None
    return None


def _is_broker_session_argv(argv: list[str]) -> bool:
    """Check whether argv belongs to `python ... broker session ...`."""
    if len(argv) < 4:
        return False
    exe = os.path.basename(argv[0])
    script = os.path.basename(argv[1])
    return exe.startswith("python") and script in {"broker", "broker.py"} and argv[2] == "session"


def _discover_live_daemon_sessions() -> dict[str, dict]:
    """Discover broker daemon sessions from live processes.

    This is a recovery path for cases when metadata files in /tmp/devolution-broker
    were removed but daemon processes are still alive.
    """
    recovered: dict[str, dict] = {}
    proc_root = Path("/proc")
    if not proc_root.exists():
        return recovered

    for proc_entry in proc_root.iterdir():
        if not proc_entry.name.isdigit():
            continue
        pid = int(proc_entry.name)
        argv = _read_proc_cmdline(pid)
        if not _is_broker_session_argv(argv):
            continue
        if _read_proc_ppid(pid) != 1:
            # Active foreground attach clients can have the same argv;
            # daemonized sessions are re-parented to PID 1.
            continue
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            continue
        except OSError:
            continue

        name = argv[3]
        command = argv[4:] if len(argv) > 4 else ["/bin/bash"]

        # Skip orphan daemons that lost their socket file: they are unreachable
        # (nobody can attach via broker.py) and leaving them in `list` causes
        # the "empty session on attach" bug — clients connect, fail, and a new
        # empty daemon is spawned while the ghost keeps consuming memory.
        socket_present = (SOCKET_DIR / f"{name}.sock").exists()
        if not socket_present:
            continue

        created = _proc_start_time(pid)
        if created is None:
            # Fall back to "now" rather than current time of /proc inode,
            # so uptime at worst resets to 0 but never reports a wrong past.
            created = time.time()
        try:
            cwd = os.readlink(f"/proc/{pid}/cwd")
        except OSError:
            cwd = ""

        # Keep the oldest daemon if duplicates are seen for the same name.
        existing = recovered.get(name)
        if existing and existing["created"] <= created:
            continue

        recovered[name] = {
            "name": name,
            "command": command,
            "pid": pid,
            "created": created,
            "uptime_seconds": max(0, int(time.time() - created)),
            "cwd": cwd,
            "suspend_after": SUSPEND_GRACE_SEC,
            "ai_sessions": [],
            "recovered": True,
            "socket_present": True,
        }
    return recovered


def _restore_session_metadata(entry: dict):
    """Recreate .json/.pid files for a recovered session if missing."""
    name = entry["name"]
    meta_path = SOCKET_DIR / f"{name}.json"
    pid_path = SOCKET_DIR / f"{name}.pid"

    if not meta_path.exists():
        meta = {
            "name": name,
            "command": entry["command"],
            "cwd": entry.get("cwd", ""),
            "pid": entry["pid"],
            "created": entry["created"],
            "suspend_after": entry.get("suspend_after", SUSPEND_GRACE_SEC),
            "ai_sessions": entry.get("ai_sessions", []),
        }
        try:
            meta_path.write_text(json.dumps(meta))
        except OSError:
            pass

    if not pid_path.exists():
        try:
            pid_path.write_text(str(entry["pid"]))
        except OSError:
            pass


ATTACH_CONNECT_TIMEOUT = 5.0  # seconds to wait for the daemon to accept the socket


def attach_session(name: str):
    """Attach to an existing session."""
    socket_path = SOCKET_DIR / f"{name}.sock"

    conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    # Without a timeout a stale socket file with no listener makes connect()
    # hang indefinitely, spawning zombie clients that pile up over time.
    conn.settimeout(ATTACH_CONNECT_TIMEOUT)
    try:
        conn.connect(str(socket_path))
    except (socket.timeout, OSError) as e:
        print(f"\r\n[attach failed: {e}]", file=sys.stderr)
        conn.close()
        raise ConnectionRefusedError(str(e))
    conn.settimeout(None)

    # Send current terminal size
    try:
        winsize = fcntl.ioctl(sys.stdout.fileno(), termios.TIOCGWINSZ, b'\x00' * 8)
        rows, cols = struct.unpack('HHHH', winsize)[:2]
    except OSError:
        rows, cols = 24, 80

    conn.sendall(b'S' + struct.pack('!HH', rows, cols))

    # Receive ring buffer
    length_data = b''
    while len(length_data) < 4:
        chunk = conn.recv(4 - len(length_data))
        if not chunk:
            print("Connection lost.", file=sys.stderr)
            sys.exit(1)
        length_data += chunk

    buf_len = struct.unpack('!I', length_data)[0]
    if buf_len > 0:
        buf_data = b''
        while len(buf_data) < buf_len:
            chunk = conn.recv(min(4096, buf_len - len(buf_data)))
            if not chunk:
                break
            buf_data += chunk
        sys.stdout.buffer.write(buf_data)
        sys.stdout.buffer.flush()

    # Switch terminal to raw mode
    old_settings = termios.tcgetattr(sys.stdin.fileno())
    try:
        tty.setraw(sys.stdin.fileno())
        conn.setblocking(False)

        def on_resize(signum, frame):
            try:
                winsize = fcntl.ioctl(sys.stdout.fileno(), termios.TIOCGWINSZ, b'\x00' * 8)
                rows, cols = struct.unpack('HHHH', winsize)[:2]
                conn.sendall(CTRL_PREFIX + b'R' + struct.pack('!HH', rows, cols))
            except OSError:
                pass

        signal.signal(signal.SIGWINCH, on_resize)

        while True:
            try:
                readable, _, _ = select.select([sys.stdin, conn], [], [], 1.0)
            except (select.error, ValueError):
                continue

            for fd in readable:
                if fd is sys.stdin:
                    try:
                        data = os.read(sys.stdin.fileno(), 4096)
                    except OSError:
                        data = b''

                    if not data:
                        return

                    if DETACH_KEY in data:
                        sys.stdout.buffer.write(b'\r\n[detached]\r\n')
                        sys.stdout.buffer.flush()
                        return

                    escaped = data.replace(CTRL_PREFIX, CTRL_PREFIX + CTRL_PREFIX)
                    try:
                        conn.sendall(escaped)
                    except (OSError, BrokenPipeError):
                        return

                elif fd is conn:
                    try:
                        data = conn.recv(4096)
                    except BlockingIOError:
                        continue
                    except OSError:
                        data = b''

                    if not data:
                        sys.stdout.buffer.write(b'\r\n[session ended]\r\n')
                        sys.stdout.buffer.flush()
                        return

                    sys.stdout.buffer.write(data)
                    sys.stdout.buffer.flush()

    finally:
        termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, old_settings)
        conn.close()


def session_command(name: str, command: list[str], cwd: str | None = None,
                    suspend_after: int = SUSPEND_GRACE_SEC,
                    suspend_after_given: bool = False):
    """Smart session management: create if needed, always attach."""
    if is_session_alive(name):
        if suspend_after_given:
            print(f"Note: --suspend-after ignored, session '{name}' already running.", file=sys.stderr)
        print(f"Attaching to session '{name}'...")
    else:
        # Clean up stale files if any
        cleanup_dead_session(name)
        print(f"Creating session '{name}': {' '.join(command)}")
        daemon = SessionDaemon(name, command, cwd=cwd, suspend_after=suspend_after)
        daemon.start()

    # Wait for socket to appear (daemon needs time to set up)
    socket_path = SOCKET_DIR / f"{name}.sock"
    for _ in range(20):  # up to 2 seconds
        if socket_path.exists():
            break
        time.sleep(0.1)

    if not socket_path.exists():
        print(f"Session '{name}' exited immediately.", file=sys.stderr)
        cleanup_dead_session(name)
        sys.exit(1)

    try:
        attach_session(name)
    except (FileNotFoundError, ConnectionRefusedError):
        print(f"\r\n[session ended]", file=sys.stderr)
        cleanup_dead_session(name)


def list_sessions(as_json: bool = False):
    """List all active sessions."""
    if not SOCKET_DIR.exists():
        if as_json:
            print(json.dumps({"sessions": []}))
        else:
            print("No sessions.")
        return

    sessions = list(SOCKET_DIR.glob("*.json"))

    result = []
    for meta_path in sorted(sessions):
        try:
            meta = json.loads(meta_path.read_text())
            pid = meta["pid"]

            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                cleanup_dead_session(meta["name"])
                continue

            uptime = time.time() - meta["created"]
            entry = {
                "name": meta["name"],
                "command": meta["command"],
                "pid": pid,
                "created": meta["created"],
                "uptime_seconds": int(uptime),
                "cwd": meta.get("cwd", ""),
                "suspend_after": meta.get("suspend_after", SUSPEND_GRACE_SEC),
                "ai_sessions": meta.get("ai_sessions", []),
                "recovered": False,
                "socket_present": (SOCKET_DIR / f"{meta['name']}.sock").exists(),
            }
            result.append(entry)
        except (json.JSONDecodeError, KeyError, OSError):
            continue

    known_names = {s["name"] for s in result}
    recovered = _discover_live_daemon_sessions()
    for name, entry in recovered.items():
        if name in known_names:
            continue
        result.append(entry)
        _restore_session_metadata(entry)

    result.sort(key=lambda s: s["name"])

    if as_json:
        print(json.dumps({"sessions": result}))
    else:
        if not result:
            print("No sessions.")
            return
        print(f"{'NAME':<20} {'COMMAND':<30} {'PID':<10} {'UPTIME':<10} {'SUSPEND'}")
        print("-" * 85)
        for s in result:
            hours = s["uptime_seconds"] // 3600
            minutes = (s["uptime_seconds"] % 3600) // 60
            cmd = " ".join(s["command"])
            suspend = "never" if s["suspend_after"] == 0 else f"{s['suspend_after']}s"
            print(f"{s['name']:<20} {cmd:<30} {s['pid']:<10} {f'{hours}h {minutes}m':<10} {suspend}")


def kill_session(name: str):
    """Kill a session by name."""
    pid_path = SOCKET_DIR / f"{name}.pid"
    pid: int | None = None

    if pid_path.exists():
        try:
            pid = int(pid_path.read_text().strip())
        except ValueError:
            pid = None

    if pid is None:
        # Fallback for sessions whose metadata files were deleted.
        entry = _discover_live_daemon_sessions().get(name)
        if entry:
            pid = entry["pid"]

    if pid is None:
        print(f"Session '{name}' not found.", file=sys.stderr)
        sys.exit(1)

    try:
        os.kill(pid, signal.SIGTERM)
        print(f"Session '{name}' (pid {pid}) terminated.")
    except ProcessLookupError:
        print(f"Session '{name}' already dead, cleaning up.")

    # Wait briefly for the daemon to actually exit so the log files we delete
    # next aren't being written to in parallel by a still-shutting-down process.
    for _ in range(20):  # up to ~2 seconds
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            break
        time.sleep(0.1)

    cleanup_dead_session(name)

    # Remove session log files (incl. rotated backups). Without this, an aborted
    # session leaves its logs (potentially gigabytes after a runaway WARNING
    # cascade — see 2026-05-06 incident) on disk indefinitely.
    if LOG_DIR.exists():
        for log_file in LOG_DIR.glob(f"{name}.log*"):
            try:
                log_file.unlink()
            except OSError:
                pass


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    cmd = sys.argv[1]
    SOCKET_DIR.mkdir(parents=True, exist_ok=True)

    if cmd == "session":
        if len(sys.argv) < 3:
            print("Usage: broker.py session <name> [command...] [--cwd PATH]", file=sys.stderr)
            sys.exit(1)

        name = sys.argv[2]
        args = sys.argv[3:]

        # Extract --cwd from args
        cwd = None
        if "--cwd" in args:
            idx = args.index("--cwd")
            if idx + 1 < len(args):
                cwd = args[idx + 1]
                args = args[:idx] + args[idx + 2:]
            else:
                print("--cwd requires a path argument", file=sys.stderr)
                sys.exit(1)

        # Extract --suspend-after from args (seconds; 0 disables auto-suspend)
        suspend_after = SUSPEND_GRACE_SEC
        suspend_after_given = False
        if "--suspend-after" in args:
            idx = args.index("--suspend-after")
            if idx + 1 < len(args):
                try:
                    suspend_after = int(args[idx + 1])
                    if suspend_after < 0:
                        raise ValueError("negative")
                except ValueError:
                    print("--suspend-after requires a non-negative integer (seconds; 0 = never)",
                          file=sys.stderr)
                    sys.exit(1)
                args = args[:idx] + args[idx + 2:]
                suspend_after_given = True
            else:
                print("--suspend-after requires a numeric argument (seconds; 0 = never)",
                      file=sys.stderr)
                sys.exit(1)

        command = args if args else ["/bin/bash"]
        session_command(name, command, cwd=cwd, suspend_after=suspend_after,
                        suspend_after_given=suspend_after_given)

    elif cmd == "list" or cmd == "ls":
        list_sessions(as_json="--json" in sys.argv)

    elif cmd == "kill":
        if len(sys.argv) < 3:
            print("Usage: broker.py kill <name>", file=sys.stderr)
            sys.exit(1)
        kill_session(sys.argv[2])

    else:
        print(f"Unknown command: {cmd}", file=sys.stderr)
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
