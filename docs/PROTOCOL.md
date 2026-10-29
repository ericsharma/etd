# et/1: the protocol between E.T. and etd, and between etds

The source of truth is `crates/et-proto/src/lib.rs` (requests) and `crates/et-proto/src/network.rs` (the network document). When this document and the code disagree, the code is right: correct this document.

## Transport

- iroh QUIC, ALPN `et/1`. Both ends are identified by their iroh keys (TLS 1.3).
- One connection per app session; machines also connect to each other. Each operation opens one bidirectional stream.
- Framing: a big-endian `u32` length, then a [postcard](https://docs.rs/postcard) body. Frames over 16 MiB are refused.
- The first frame on a stream is a `Request`. etd answers with one `Response` frame. Some requests then carry more data on the same stream (see below).

## Changes to the protocol

postcard is positional: an enum is a bare index, and a struct is its fields in order.

- A new request or response goes **at the end** of its enum. An older peer still decodes everything it knows, and it never sends what it does not know.
- Any other change (reorder, remove, a new field in a struct) is a new ALPN.
- There are no users yet, so there is no compatibility code.

## The network

Each etd keeps one network: `network.json` (the signed document) in its config directory, and on machines `network.key` (the network key's secret half).

```
Network { key: PublicKey, name, records: { EndpointId → Record } }
Record  { state: Member { name, kind: Machine | Device } | Removed, stamp: Stamp }
Stamp   { counter: u64, author: EndpointId }        // compared counter first, then author
Signed  { network: Network, signature }              // signed by the network key
```

- **Signature.** A document counts only when the network key signed its postcard bytes. A machine applies only signed documents; the app checks the signature too.
- **Who changes it.** A machine gets the network key when it joins; a device does not. So only machines change the document: add a member (an invite redeemed there), or remove one (`etd revoke`).
- **Merge without clocks.** A change gives the record a stamp one higher than any in the document. Merging two documents keeps, for each id, the record with the higher stamp. Two machines that change the document at the same time both keep their changes, and every machine ends with the same result. A removal is a record (`Removed`), so it travels like any other change; a removed member comes back only with a newer record (a new invite).
- **Sync.** When a machine's document changes, it sends it to every other machine (`NetworkUpdate`). The receiver checks the signature, merges, signs the result with its copy of the key, and sends it on if it changed. A failed send is retried after 10 s, then with doubling waits; every machine also sends its document every 5 minutes.
- **The first start** makes a network with this machine as the only member (and moves devices paired before networks existed, from `clients.json`, into it).
- **New key after a machine is revoked.** A revoked machine still has the network key, so the machine that revokes it makes a new one. Each key change is a pair: `old` (the document with the machine removed, signed by the old key) and `new` (the same document with the new key, signed by it). Every machine keeps all key changes, oldest first, in `rotations.json`. Before each `NetworkUpdate`, a machine sends `NetworkRotate { chain, key }`: all key changes, and the secret half of the last new key. The receiver checks each step and that the steps follow each other. It moves to the last key if its own key is in the chain and it is still a member; a chain that ends at a key it already left changes nothing; a chain without its key is refused (a fork: two machines revoked at once with different results; fix by hand). Records changed only on the receiver under the old key stay, except records about or written by a removed member (a machine about to be revoked could have added a helper, or itself again with a high counter). A document signed by an old key is then refused as "another network".
- **Catching up.** At its start, a machine asks every other machine for the document (`Network`). A machine that is asked by a machine answers and also sends it its document and the last key change, so a machine that was off during a key change gets it. There is no pkarr record: a pkarr record is last-writer-wins, and a revoked machine has the old key, so it could replace the record.

## Access

etd reads the peer's `EndpointId` from the connection and looks it up in the network.

- Not a member: the first stream must be `Join` or `Pair` with a valid invite secret. Anything else closes the connection with the reason `not paired`. 5 tries per minute per peer.
- Removed (`etd revoke` on any machine): every machine closes its open connections, with the reason `revoked`, within seconds of getting the document.

## Invite ticket

`et://pair?t=<BASE32>`: upper-case base32 (no padding) of the postcard `Ticket`:

| Field | Meaning |
|---|---|
| `addr` | The inviting etd's `EndpointAddr`: id and relay URL only (direct addresses make the QR code too large) |
| `secret` | 32 random bytes; the inviting machine stores only its SHA-256 and the kind it admits (machine or device); it works one time |
| `expires` | Unix seconds; 10 minutes after the invite |
| `host_name` | For the app's confirm sheet |

## Requests

| # | Request | Who may send it | Response | Then on the stream |
|---|---|---|---|---|
| 0 | `Hello { app }` | member | `HostInfo { host_name, os, etd_version, tmux }` | — |
| 1 | `ListSessions` | member | `Sessions([SessionInfo])` (tmux) | — |
| 2 | `Control { target }` | member | `ControlStarted` | `ControlFrame`s down, raw bytes up (tmux stdin) |
| 3 | `Ping` | member | `Pong` | — |
| 4 | `Pair { secret, device_name }` | anyone | `Paired { host_name }` or `Rejected(reason)` | — |
| 5 | `Exec { script }` | member | `ExecResult { output, exit_code, truncated }` | — |
| 6 | `Upload { path, len }` | member | `Uploaded { path }` | `len` raw bytes up, before the response |
| 7 | `Agent { host }` | member | `AgentStarted` | `ControlFrame`s down, raw bytes up (`mm agent` stdin) |
| 8 | `ExecOn { host, script }` | member | `ExecResult { … }` | — |
| 9 | `Machines` | member | `Machines([String])` | — |
| 10 | `Join { secret, name, kind }` | anyone | `Joined { network, network_key }` or `Rejected(reason)` | — |
| 11 | `Network` | member | `NetworkDoc(signed)` | — |
| 12 | `NetworkUpdate { network }` | machine | `Pong` | — |
| 13 | `Invite { kind }` | member | `Invitation { url }` | — |
| 14 | `Notify { note }` | machine | `Pong` | — |
| 15 | `Foreground { until }` | member | `Pong` | — |
| 16 | `NetworkRotate { chain, key }` | machine | `Pong` | — |
| 17 | `Deliver { hash, session }` | member | `Progress { done, total }` frames, then `Delivered { paths, typed }` | etd fetches the files over this connection first (see below) |
| 18 | `Send { paths, to, session }` | local socket only (`etd send`) | `Progress` frames, then the target's `Delivered` | — |
| 19 | `Shared` | member (local socket: every machine's) | `SharedList([SharedItem])` | — |
| 20 | `Offer { path }` | member | `Offered { hash, total }` | held open by the asker; the offer ends when it closes |
| 21 | `Blobs` | member | — | the rest of the stream is an iroh-blobs request (offered hashes only) |
| 22 | `Share { path, to, session }` | member | `Progress` frames, then `Delivered` | — |
| 23 | `Unshare { path }` | member | `Pong` | — |
| 24 | `LogInfo` | member | `LogInfo { archive, paused }` | — |
| 25 | `LogAppend { session_id, meta, offset, bytes }` | machine | `LogAck { offset }` | — |
| 26 | `LogArchives` | member | `Machines([name])` | — |
| 27 | `LogSessions { since }` | member (archive) | `LogList([LogEntry])` | — |
| 28 | `LogRead { machine, session_id, from }` | member (archive) | `LogChunk { offset, bytes, live }` frames | until the asker finishes its side |
| 29 | `LogPause { machine, mm_session, paused }` | member (archive) | `Pong` | — |
| 30 | `Report { refresh }` | member (archive) | `ReportProgress` frames, then `Report { json }` | — |
| 31 | `Grouping { change_json }` | member (archive) | `Pong` | — |
| 32 | `LogSend { selection, to, session }` | member (archive) | `Progress` frames, then `Delivered` | — |
| 33 | `Agents` | member | `Agents { json }` | — |

Any request can get `Error(message)` instead.

### Joining (`Join`, `Pair`)

`Join` redeems an invite secret for a member of `kind`. The invite's kind must match: a device invite cannot make a machine. A machine name must be unique in the network (manymux and `etd-ssh` address machines by name); a device with a taken name gets " 2", " 3"… `Joined` carries the signed document and, for a machine only, the network key. `Pair` is the app's form of `Join` with `kind: Device`; it answers `Paired`. The connection continues as a member.

### tmux (`Control`)

`etd` runs `tmux -C attach-session -t =<name>` (or `new-session -A -s <name>`) with `TERM=et-ios` and no PTY. When the app finishes its side of the stream, tmux's stdin closes, tmux detaches, and the session keeps running. The app reads tmux control mode itself; etd only moves bytes.

`ControlFrame`: `Stdout(bytes)`, `Stderr(line)`, `Exit(code)` (the last frame; `None` = killed by a signal).

### Shell (`Exec`, `ExecOn`)

`/bin/sh -c <script>` in the home directory, with the user's login environment, stderr joined to stdout. Limits: 120 s and 8 MiB of output (`truncated`). `ExecOn` runs it on a machine reached by ssh (see `Agent`).

### Files (`Upload`)

`path` is absolute or starts with `~/`, and must be inside the home directory (also after symlinks). Parent directories are made. At most 32 MiB.

### Sending files (`Deliver`, `Send`)

The bytes move with [iroh-blobs](https://github.com/n0-computer/iroh-blobs) 0.103, inside the et/1 connection, so only members can send.

1. The sender imports its files and folders into its blob store as one iroh-blobs `Collection` (names relative to each path's parent: `notes.md`, `out/logs/run.txt`).
2. On a connection **it opened** to the target, the sender runs the iroh-blobs provider (`provider::handle_connection`), which serves that collection's root hash only (other gets and observes are refused). A phone can send although it accepts no connections.
3. The sender sends `Deliver { hash }`. The target etd asks for the sizes, checks that its disk keeps 2 GB free, then fetches over the same connection only the chunks its store does not have: a `Deliver` sent again after a cut resumes.
4. The target writes each top-level item to `~/etd-downloads/` (`$ETD_DOWNLOADS`); a taken name gets ` (2)`, ` (3)`… It answers `Delivered` with the top-level paths. With `session`, etd then types the paths (shell-quoted when needed, a space after each, no Enter) into that manymux session on its machine, through `mm agent`: it attaches at the session's current size (a session takes the smallest size of its clients, so nothing resizes), types, and detaches. `typed` says whether that worked.

Blobs live only while a send or delivery runs; the store's garbage collection (every 10 minutes) removes the rest. The sender imports by reference, and the store never deletes a file it references, so the user's files are never removed.

`etd send <path>… <machine>[:<session>]` sends `Send` on the local socket; the local etd does steps 1–3 to that machine and prints the paths.

### Shared files (`Shared`, `Offer`, `Blobs`, `Share`, `Unshare`)

Each etd writes a line to `deliveries.jsonl` in its state directory for each delivery (top-level path, sender's member name, time, session). `Shared` lists those still on disk, newest first, a path once. On the local socket (`etd files`) the local etd asks every machine and merges.

- **Fetch to a device** (preview): the device sends `Offer { path }` (inside `~/etd-downloads/`) and keeps that stream open; etd imports it and answers `Offered`. On a second stream the device sends `Blobs` and then an iroh-blobs get for the hash; etd serves it from the offered collections only. The device closes the `Offer` stream when done.
- **`Share`**: `to` is this machine: type `path` into `session` here. Otherwise this etd sends `path` to the machine `to` as `etd send` does (typed into `session` there).
- **`Unshare`**: deletes the file or folder and its log lines.

### manymux (`Agent`, `Machines`)

`Agent { host: None }` runs `mm agent` on etd's machine. The app then speaks [manymux](https://github.com/rayfish/manymux)'s own protocol through the pipe; etd does not read it. A manymux node answers one request per stream, except `Attach` and `Events`, so the app opens many short `Agent` streams.

In a network, the app connects to each machine's etd and uses `Agent { host: None }` there. `Agent { host: Some(name) }` and `ExecOn` reach the machine of the network called `name` through this etd (as `etd-ssh` does); other names are refused. etd never runs ssh. `Machines` is the output of `mm hosts`.

### Pushes (`Notify`, `Foreground`)

Each etd follows its own manymux node. For a session that nobody is attached to, a bell, a notification request (OSC 9/777) or a non-zero exit becomes a `Note { kind, machine, session, title, body }` (`machine`: the network's name for the machine). etd runs its push command (`ETD_NOTIFY_COMMAND`) if it has one, and sends `Notify` to every other machine; a machine with a push command runs it. A machine that etd's node reaches by ssh but that is not in the network is pushed for locally.

The push command runs through `/bin/sh -c` with `ETD_KIND` (`bell`, `notify`, `exit`), `ETD_MACHINE`, `ETD_SESSION`, `ETD_TITLE` and `ETD_BODY`.

`Foreground { until }` (Unix seconds, at most 2 minutes ahead): the app is open; this machine runs no push command until then. The app sends it every 20 s to every machine while it is in the foreground.

### Logbook (`LogInfo`, `LogAppend`, and the apps' requests 26–32)

Each etd sends the transcripts of the AI agents (Claude Code) that run in its manymux sessions to the archive machines of the network. A machine is an archive after `etd logbook archive on` (`logbook.json`). The other machines ask each machine with `LogInfo`, at start and every minute. With no archive, nothing is read or sent.

- An agent belongs to the manymux session whose leader process is its parent (or the parent of its parent, and so on). An agent in no live session is not sent.
- `LogAppend` carries whole JSONL lines of the transcript `session_id`, from `offset` in the file on the source machine. The archive keys it by the sender's member name and answers how far into that file it is (`LogAck`). Empty `bytes` asks only for that offset (and updates `meta`). An `offset` above the archive's skips bytes: the archive writes `{"type":"etd-gap","skipped":N}` in their place. An `offset` below writes nothing.
- `meta` (`LogMeta`): `agent`, `mm_session`, `cwd`, `repo` (the `origin` URL, else the repository root), `branch`, `status` (`busy`, `idle`, or `gone`), `previous` (the transcript before it in the same manymux session). The archive keeps a known `previous` when a later `meta` has none.
- A resumed agent (`claude --resume <id>`, from its command line) writes a new transcript without the old one: the source also sends transcript `<id>` whole, as `gone`, and the new one gets `previous = <id>`.
- `LogInfo.paused`: the asker's manymux sessions that the archive does not record. The source skips their new lines (a gap); the lines never leave the machine.
- An archive keeps `logbook/<machine>/<session id>.jsonl` (mode 0600) and `index.json` in etd's data directory.
- `LogArchives`: the archives this machine found (its own name first if it is one), from its last look (at most 90 s old), else asked now.
- `LogSessions { since }`: the transcripts that changed at or after `since` (lines, state, pause). `LogEntry`: `machine`, `session_id`, `meta`, `bytes` (the size of the archive's copy), `first`, `last`, `paused`, `title` (Claude's last `ai-title`).
- `LogRead`: offsets are in the archive's copy. A `from` inside a line starts at the next line. Whole lines, about 1 MB per chunk; `offset` is the byte after the chunk. `live` is true on the chunk that reaches the end of the bytes stored when the request came (or on an empty chunk, when they ended at a chunk's end), and on all later chunks. New lines come as they arrive. The asker ends the follow by finishing its side.
- `LogPause`: the archive drops new lines of that session at once (one gap line per batch); its source stops sending them within a minute (`LogInfo.paused`).
- `Report { refresh: false }`: the last report as JSON (`id`, `taken`, `projects`, `unassigned`, `proposals`), or `{"id":null,…}` before the first. `refresh: true` makes a new one (the watcher, below): `ReportProgress { stage, done, total }` frames (`Digests`, then `Report`; at least one every 10 s), then `Report { json }`. A second refresh while one runs joins it. The report goes on if the asker leaves.
- The watcher: `claude -p` with no tools, in `logbook/watcher/`, outside manymux (`ETD_WATCHER_COMMAND` replaces the `claude` program). First a digest of each transcript that grew (transcripts with lines in the last 7 days; at most 4 at a time; kept in `digests.json`), then one call that groups the sessions by goal. Reports are kept as `watcher/reports/<id>.json`; `id` is the time in Unix seconds.
- `Grouping { change_json }`: the owner's choice, kept in `watcher/projects.json` for each next report and applied to the last report at once. `{"decision":"accept"|"reject","kind","detail","why"}` for a proposal; `{"decision":"edit","kind":"rename","project","name"}`; `{"decision":"edit","kind":"move","key":"machine/session id","to":"project id, or empty for none"}`.
- `LogSend`: `Turns { machine, session_id, turn_ids }` (the `uuid` of each prompt) becomes a Markdown file (each prompt and Claude's text replies; no tool output) in `~/etd-downloads/` of the machine `to`, typed into `session` there (empty: not typed). `ReportSection { report_id, project_id }`: that project of the report (goal, status, progress, sessions with their roles, next steps) as Markdown, sent the same way.

### Agents (`Agents`)

- `Agents`: the AI agents of this machine (et-ios `docs/PLAN-multi-agent.md`), as JSON (`et_proto::agents::AgentsInfo`), so new fields do not break older peers. `agents`: one entry per built-in profile (`claude`, `codex`, `gemini`, `grok`, `antigravity`): `id`, `version` (null: not installed), `path`, `transcript` (the Logbook reads it), `state` (`own`, `hooks`, `transcript` or `none`: where its live state comes from), `alerts` ("needs you" reaches etd), `setup_hint`. The machine finds each launch command in its login `PATH` and runs it with `--version` (at most 5 s); the answer is kept for 60 s.

## The local socket (`etd-ssh`)

Not over iroh: a Unix socket in etd's state directory (`etd.sock`, mode 0600, or `$ETD_SOCKET`), with the same frames. `etd-ssh` is an `ssh` stand-in for manymux (`MM_SSH=etd-ssh`):

- `etd-ssh [options] <machine> -- mm agent` sends `Agent { host: Some(machine) }`; the local etd connects to that machine's etd (it keeps the connection) and opens `Agent { host: None }` there. Then the stream is the pipe.
- Any other command sends `ExecOn { host: machine, script }`; the local etd runs it there as `Exec`. Output goes to stdout; the exit code becomes etd-ssh's.
- `<machine>` is a machine member's name. etd-ssh exits with 255 when it fails itself, as ssh does.
