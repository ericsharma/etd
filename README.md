# etd

`etd` lets the E.T. iPhone app reach your terminal sessions on a machine, with no VPN and no open port. It runs an [iroh](https://iroh.computer) endpoint. The phone finds the machine by its key, not by an IP address. Connections are direct when possible, with an encrypted relay when not.

It serves two kinds of sessions:

- **tmux** on the machine that runs etd (tmux control mode, one native page per window).
- **[manymux](https://github.com/rayfish/manymux)** sessions on that machine *and* on every machine its manymux node watches (`mm add <host>`). etd reaches those with `ssh <host> mm agent`, so only one machine needs etd.

**Status: experimental.** The protocol can change. The iOS app with iroh support is not released yet.

## Use

1. Build or install `etd` (see below).
2. Run `etd install`. This starts etd as a user service (systemd on Linux, launchd on macOS).
3. Run `etd pair` and scan the QR code with the iPhone camera.

To try it without a service, run `etd` in a terminal. When no device is paired, it shows the QR code.

| Command | What it does |
|---|---|
| `etd` | Run the daemon in the foreground |
| `etd install` / `etd uninstall` | Add or remove the user service |
| `etd pair [--url]` | Show a QR code (or a link) that adds one device. Valid for 10 minutes. |
| `etd status` | Show the endpoint id, relay, addresses, tmux, and connected devices |
| `etd clients` | List paired devices |
| `etd revoke <name\|id>` | Remove a device and close its connections |

### manymux

Install manymux on every machine (`mm` must be on the `PATH` of a non-interactive ssh), and run its node as a service on each. On the machine with etd, add the others: `mm add <host>`. Its node must find `ssh` on its `PATH` to watch them, or bells from those machines never arrive.

### Notifications

Set `ETD_NOTIFY_COMMAND` in etd's environment. For a bell, a notification request, or a failed exit in a manymux session that nobody is attached to, etd runs the command through `/bin/sh -c` with `ETD_KIND`, `ETD_MACHINE`, `ETD_SESSION`, `ETD_TITLE` and `ETD_BODY` set. See [docs/PROTOCOL.md](docs/PROTOCOL.md#notifications).

## Install

With Nix (Home Manager, Linux or macOS):

```nix
# flake inputs
etd.url = "github:ericsharma/etd";

# home-manager configuration
imports = [ inputs.etd.homeManagerModules.default ];
services.etd.enable = true;
```

From source:

```sh
cargo install --path crates/etd
etd install
```

On Linux without a desktop login, run `loginctl enable-linger $USER`. Otherwise the service stops when you log out.

## Security

A paired device can do what you can do in a shell on this machine: it can run commands and write files in your home directory. Through manymux, it can also run commands on every machine in `mm hosts`, with this machine's ssh keys. Pair only your own devices. Use `etd revoke` for a lost device.

- Each device must scan a QR code from this machine. A pairing code works one time, for 10 minutes.
- iroh checks the keys of both ends (QUIC, TLS 1.3). Relays see only encrypted traffic.
- etd refuses any machine name that is not in `mm hosts`, before it runs ssh.
- Keys and paired devices: `~/.config/etd` (Linux) or `~/Library/Application Support/etd` (macOS), or `$ETD_HOME`.

## Parts

| Crate | What |
|---|---|
| `crates/et-proto` | The wire types and framing: [docs/PROTOCOL.md](docs/PROTOCOL.md) |
| `crates/etd` | The daemon, its CLI, and `etd-probe` (a test client) |
| `crates/et-link` | The app's side, for Swift through UniFFI; `scripts/build-ios.sh` makes the xcframework, `apple/EtLink` is the Swift package |

## Development

```sh
nix develop
env -u TMUX cargo test
```

The end-to-end tests start their own tmux server and their own manymux node (`mm --socket <temp>`), with private config and log directories. They never touch your running sessions. Remove `$TMUX` anyway: inside a tmux pane, a tmux command without `-S` goes to that pane's server.

`etd-probe` is a test client: `etd-probe pair <link>`, `etd-probe hello <id>`, `etd-probe machines <id>`, `etd-probe exec-on <id> <host> <script>`, `etd-probe bench <id> <session>`. `cargo run --example agent_ls -- <id> [host]` lists a machine's manymux sessions through etd.

## License

MIT or Apache-2.0, at your choice.
