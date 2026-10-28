# How etd works

```
iPhone ──iroh──▶ etd on each machine ──▶ tmux, or the manymux node there
mm (any machine) ──MM_SSH=etd-ssh──▶ local etd ──iroh──▶ another machine's etd ──▶ mm agent
```

- **iroh.** Each machine and device is known by its key, not by an IP address. Connections are direct when possible, with an encrypted relay when not.
- **The network** is one list of members (machines and devices), signed by a network key. Machines hold that key. Phones and tablets do not, so only a machine can change the list. Each machine keeps a copy and sends changes to the others.
- **tmux.** etd serves tmux on its machine in control mode: one native page per window in the app.
- **manymux.** Sessions live on their machine and survive a dropped connection. manymux reaches other machines with `ssh <host> mm agent`. etd sets `MM_SSH=etd-ssh`, which does the same through the local etd and iroh. etd keeps manymux's machine list (`hosts.toml`) equal to the network's machines, so there is no `mm add`. `mm ls` and `mm attach` on any machine reach the sessions of every machine.
- **Pushes.** Each etd follows its own manymux node. A bell, a notification request or a failed exit in a session that nobody is attached to goes to the machines that have `ETD_NOTIFY_COMMAND` set. They run it. Set it on one machine. While the app is open, it tells the machines so, and they send no push.

## What `etd install` changes

- Installs `mm` with manymux's own installer when it is missing (pinned to the version etd is tested with).
- Adds two user services (systemd or launchd): etd, and the manymux node.
- Adds one marked `MM_SSH` line to your shell's startup file (`~/.zshenv` or `~/.bashrc`).

It never restarts a running manymux node, and `etd uninstall` never stops one. A manymux node that etd did not start stays as it is. `etd install --no-service` installs only manymux: E.T. for Mac runs etd itself.

Keep `etd` and `etd-ssh` in the same directory.

## Files and environment

| What | Where |
|---|---|
| Keys and the network | `~/.config/etd` (Linux), `~/Library/Application Support/etd` (macOS), or `$ETD_HOME` |
| Received files | `~/etd-downloads/` |
| Logbook archive (`etd logbook archive on`) | `~/.local/share/etd/logbook` (Linux), `~/Library/Application Support/etd/logbook` (macOS), or `$ETD_HOME/logbook` |
| Log to a file | `ETD_LOG_FILE` (a leading `~/` is the home directory) |
| Who manages etd | `etd status --json` → `manager`: `nix`, `cli`, `app` or `manual` |

One etd runs per key. A second etd with the same files stops at start.
