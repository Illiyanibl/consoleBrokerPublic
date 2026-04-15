#!/usr/bin/env python3
"""
Devolution PTY Broker — persistent terminal sessions.

Runs CLI programs (Claude Code, Codex, bash, etc.) in a PTY that survives
SSH disconnects. On reconnect, replays missed output from a ring buffer.

Usage:
    broker.py session <name> [command...] [--cwd PATH]  — connect to session (create if needed)
    broker.py list [--json]                             — list active sessions
    broker.py kill <name>                               — terminate a session

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


def setup_logger(name: str) -> logging.Logger:
    """Create a file logger for a session daemon."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(f"broker.{name}")
    logger.setLevel(logging.DEBUG)
    if not logger.handlers:
        handler = logging.FileHandler(LOG_DIR / f"{name}.log")
        handler.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        ))
        logger.addHandler(handler)
    return logger


class RingBuffer:
    """Fixed-size ring buffer for terminal output."""

    def __init__(self, capacity=BUFFER_SIZE):
        self.capacity = capacity
        self.buf = bytearray()

    def write(self, data: bytes):
        self.buf.extend(data)
        if len(self.buf) > self.capacity:
            overflow = len(self.buf) - self.capacity
            self.buf = self.buf[-self.capacity:]
            return overflow
        return 0

    def read_all(self) -> bytes:
        return bytes(self.buf)

    def size(self) -> int:
        return len(self.buf)


class SessionDaemon:
    """Daemon process that holds a PTY and serves clients via Unix socket."""

    def __init__(self, name: str, command: list[str], cwd: str | None = None):
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
            os.execvpe(self.command[0], self.command, env)

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

    def _accept_client(self, server: socket.socket):
        """Accept new client connection and send ring buffer."""
        try:
            conn, _ = server.accept()
        except OSError as e:
            self.log.warning("Accept failed: %s", e)
            return
        self.log.info("Client connected (total: %d)", len(self.clients) + 1)

        try:
            # Read terminal size from client: 'S' + rows(2B) + cols(2B)
            header = self._recv_exact(conn, 5, timeout=2.0)
            if header and header[0:1] == b'S':
                rows, cols = struct.unpack('!HH', header[1:5])
                self.log.debug("Client sent resize on connect: %dx%d", cols, rows)
                self._resize_pty(rows, cols)
            else:
                self.log.warning("Client handshake failed: header=%r", header)

            # Send ring buffer
            data = self.buffer.read_all()
            length = struct.pack('!I', len(data))
            conn.sendall(length + data)
            self.log.debug("Sent ring buffer: %d bytes", len(data))

            conn.setblocking(False)
            self.clients.append(conn)
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
            self.log.warning("Ring buffer overflow: %d bytes dropped", overflow)

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

    def _on_child_exit(self, signum, frame):
        """Handle child process exit."""
        try:
            pid, status = os.waitpid(self.child_pid, os.WNOHANG)
            if self.log:
                self.log.info("Child exited: pid=%d status=%d", pid, status)
        except ChildProcessError:
            if self.log:
                self.log.info("Child already reaped")
        self.running = False

    def _on_terminate(self, signum, frame):
        """Handle SIGTERM."""
        if self.log:
            self.log.info("Received SIGTERM, shutting down")
        self.running = False

    def _cleanup(self, server: socket.socket):
        """Clean up resources."""
        if self.log:
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
    """Remove stale files from a dead session."""
    for suffix in [".sock", ".pid", ".json"]:
        (SOCKET_DIR / f"{name}{suffix}").unlink(missing_ok=True)


def attach_session(name: str):
    """Attach to an existing session."""
    socket_path = SOCKET_DIR / f"{name}.sock"

    conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    conn.connect(str(socket_path))

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


def session_command(name: str, command: list[str], cwd: str | None = None):
    """Smart session management: create if needed, always attach."""
    if is_session_alive(name):
        print(f"Attaching to session '{name}'...")
    else:
        # Clean up stale files if any
        cleanup_dead_session(name)
        print(f"Creating session '{name}': {' '.join(command)}")
        daemon = SessionDaemon(name, command, cwd=cwd)
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
    if not sessions:
        if as_json:
            print(json.dumps({"sessions": []}))
        else:
            print("No sessions.")
        return

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
                "ai_sessions": meta.get("ai_sessions", []),
            }
            result.append(entry)
        except (json.JSONDecodeError, KeyError, OSError):
            continue

    if as_json:
        print(json.dumps({"sessions": result}))
    else:
        if not result:
            print("No sessions.")
            return
        print(f"{'NAME':<20} {'COMMAND':<30} {'PID':<10} {'UPTIME'}")
        print("-" * 75)
        for s in result:
            hours = s["uptime_seconds"] // 3600
            minutes = (s["uptime_seconds"] % 3600) // 60
            cmd = " ".join(s["command"])
            print(f"{s['name']:<20} {cmd:<30} {s['pid']:<10} {hours}h {minutes}m")


def kill_session(name: str):
    """Kill a session by name."""
    pid_path = SOCKET_DIR / f"{name}.pid"
    if not pid_path.exists():
        print(f"Session '{name}' not found.", file=sys.stderr)
        sys.exit(1)

    try:
        pid = int(pid_path.read_text().strip())
        os.kill(pid, signal.SIGTERM)
        print(f"Session '{name}' (pid {pid}) terminated.")
    except (ProcessLookupError, ValueError):
        print(f"Session '{name}' already dead, cleaning up.")

    cleanup_dead_session(name)


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

        command = args if args else ["/bin/bash"]
        session_command(name, command, cwd=cwd)

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
