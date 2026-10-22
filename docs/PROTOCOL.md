# et/1: the protocol between E.T. and etd

The source of truth is `crates/et-proto/src/lib.rs`. When this document and the code disagree, the code is right: correct this document.

## Transport

- iroh QUIC, ALPN `et/1`. Both ends are identified by their iroh keys (TLS 1.3).
- One connection per app session. Each operation opens one bidirectional stream.
- Framing: a big-endian `u32` length, then a [postcard](https://docs.rs/postcard) body. Frames over 16 MiB are refused.
- The first frame on a stream is a `Request`. etd answers with one `Response` frame. Some requests then carry more data on the same stream (see below).

## Changes to the protocol

postcard is positional: an enum is a bare index, and a struct is its fields in order.

- A new request or response goes **at the end** of its enum. An older peer still decodes everything it knows, and it never sends what it does not know.
- Any other change (reorder, remove, a new field in a struct) is a new ALPN.
- There are no users yet, so there is no compatibility code.

## Access

etd reads the peer's `EndpointId` from the connection.

- Not in `clients.json`: the first stream must be `Pair` with a valid secret. Anything else closes the connection with the reason `not paired`. 5 pairing tries per minute per device.
- Removed with `etd revoke`: open connections close with the reason `revoked` within 2 seconds.

## Pairing ticket

`et://pair?t=<BASE32>`: upper-case base32 (no padding) of the postcard `Ticket`:

| Field | Meaning |
|---|---|
| `addr` | etd's `EndpointAddr`: id and relay URL only (direct addresses make the QR code too large) |
| `secret` | 32 random bytes; etd stores only its SHA-256, and it works one time |
| `expires` | Unix seconds; 10 minutes after `etd pair` |
| `host_name` | For the app's confirm sheet |

## Requests

| # | Request | Response | Then on the stream |
|---|---|---|---|
| 0 | `Hello { app }` | `HostInfo { host_name, os, etd_version, tmux }` | — |
| 1 | `ListSessions` | `Sessions([SessionInfo])` (tmux) | — |
| 2 | `Control { target }` | `ControlStarted` | `ControlFrame`s down, raw bytes up (tmux stdin) |
| 3 | `Ping` | `Pong` | — |
| 4 | `Pair { secret, device_name }` | `Paired { host_name }` or `Rejected(reason)` | — |
| 5 | `Exec { script }` | `ExecResult { output, exit_code, truncated }` | — |
| 6 | `Upload { path, len }` | `Uploaded { path }` | `len` raw bytes up, before the response |
| 7 | `Agent { host }` | `AgentStarted` | `ControlFrame`s down, raw bytes up (`mm agent` stdin) |
| 8 | `ExecOn { host, script }` | `ExecResult { … }` | — |
| 9 | `Machines` | `Machines([String])` | — |

Any request can get `Error(message)` instead.

### tmux (`Control`)

`etd` runs `tmux -C attach-session -t =<name>` (or `new-session -A -s <name>`) with `TERM=et-ios` and no PTY. When the app finishes its side of the stream, tmux's stdin closes, tmux detaches, and the session keeps running. The app reads tmux control mode itself; etd only moves bytes.

`ControlFrame`: `Stdout(bytes)`, `Stderr(line)`, `Exit(code)` (the last frame; `None` = killed by a signal).

### Shell (`Exec`, `ExecOn`)

`/bin/sh -c <script>` in the home directory, with the user's login environment, stderr joined to stdout. Limits: 120 s and 8 MiB of output (`truncated`). `ExecOn` runs it on another machine through `ssh <host> /bin/sh -c '<script>'`.

### Files (`Upload`)

`path` is absolute or starts with `~/`, and must be inside the home directory (also after symlinks). Parent directories are made. At most 32 MiB.

### manymux (`Agent`, `Machines`)

`Agent { host: None }` runs `mm agent` on etd's machine; `Agent { host: Some(name) }` runs `ssh <name> mm agent`. The app then speaks [manymux](https://github.com/rayfish/manymux)'s own protocol through the pipe; etd does not read it. A manymux node answers one request per stream, except `Attach` and `Events`, so the app opens many short `Agent` streams. etd keeps one ssh connection per machine (`ControlMaster`, 2 minutes), so each is a round trip, not a login.

`host` (for `Agent` and `ExecOn`) must be in `Machines`: the output of `mm hosts` on etd's machine. Other names are refused before any ssh, so a paired device cannot make etd's machine connect to an address of its choice.

## Notifications

Not part of the protocol. If `ETD_NOTIFY_COMMAND` is set, etd follows the local manymux node's event feed (which also carries the machines it watches). For a bell, a notification request (OSC 9/777), or a non-zero exit in a session with nobody attached, it runs the command through `/bin/sh -c` with `ETD_KIND` (`bell`, `notify`, `exit`), `ETD_MACHINE`, `ETD_SESSION`, `ETD_TITLE` and `ETD_BODY`. The command does the rest (for example, an APNs push).
