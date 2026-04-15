# Devolution Broker: AI Usage Guide (English)

This file is a compact reference for small-context AI models (for example, Qwen 2.5 Coder).

## 1) What this tool does

`broker.py` runs persistent terminal sessions in the background.
You can disconnect and reconnect without losing the running process.

## 2) CLI Handles (Commands)

Use this format:

```bash
python3 broker.py <handle> [params...]
```

Available handles:

### `session`
Create or attach to a session.

Minimal:

```bash
python3 broker.py session <name>
```

With custom command:

```bash
python3 broker.py session <name> <program> [arg1 arg2 ...]
```

With working directory:

```bash
python3 broker.py session <name> <program> --cwd /path/to/project
```

Notes:
- If session does not exist: creates it, then attaches.
- If session is alive: attaches (extra command params are ignored).
- If session is dead/stale: cleans old files, recreates, then attaches.
- If `<program>` is omitted, default command is `/bin/bash`.
- `--cwd` sets the working directory for the session (default: current dir).
- Environment variables set inside session: `DEVOLUTION_BROKER_META` (path to meta JSON), `DEVOLUTION_BROKER_SESSION` (session name).

### `list` (alias: `ls`)
Show active sessions.

```bash
python3 broker.py list
python3 broker.py list --json
python3 broker.py ls
```

JSON output includes `cwd` and `ai_sessions` fields per session.

### `kill`
Terminate a session by name.

```bash
python3 broker.py kill <name>
```

## 3) Interactive Controls During Attach

- Detach without stopping session: `Ctrl+\\`
- Resize terminal window: automatically forwarded to the session PTY

## 4) Low-Level Socket Handles (for custom clients)

Socket path:

```text
/tmp/devolution-broker/<name>.sock
```

Protocol:

1. Client connects to Unix socket.
2. Client sends terminal size packet:
   - `b'S' + rows(2 bytes, big-endian) + cols(2 bytes, big-endian)`
3. Server replies with ring buffer replay:
   - `length(4 bytes, big-endian) + data`
4. Then raw bidirectional stream starts.

In-band control prefix byte is `0x00`:
- Resize handle: `0x00 + b'R' + rows(2B) + cols(2B)`
- Literal zero byte: `0x00 + 0x00`

## 5) Runtime Files Created Per Session

In `/tmp/devolution-broker/`:

- `<name>.sock` — Unix socket
- `<name>.pid` — daemon PID
- `<name>.json` — metadata (name, command, cwd, pid, created, ai_sessions)

## 6) Minimal Examples

Start/attach default shell:

```bash
python3 broker.py session dev
```

Start/attach a tool:

```bash
python3 broker.py session codex codex
```

List:

```bash
python3 broker.py list
```

Kill:

```bash
python3 broker.py kill dev
```
