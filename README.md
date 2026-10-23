# etd

`etd` lets the E.T. iPhone app reach your terminal sessions on all your machines, with no VPN, no open port and no ssh keys. Install it on each machine, join them into one network, and scan one QR code with the phone.

- It runs an [iroh](https://iroh.computer) endpoint. Each machine and phone is known by its key, not by an IP address. Connections are direct when possible, with an encrypted relay when not.
- It installs a [manymux](https://github.com/rayfish/manymux) node next to itself: terminal sessions that live on their machine and survive a dropped connection. `mm ls` and `mm attach` on any machine reach the sessions of every machine, through etd.
- It also serves tmux on its machine (tmux control mode, one native page per window).

**Status: experimental.** The protocol can change. The iOS app with iroh support is not released yet.

## Use

On the first machine:

1. Install etd (see below), then run `etd install`. It starts two user services: etd, and the manymux node.
2. Run `etd invite` and scan the QR code with the iPhone camera.

On every other machine:

1. Install etd, then run `etd install`.
2. On a machine that is already in the network, run `etd invite --machine`. It prints a command.
3. Run that command on the new machine: `etd join <link>`.

A new phone or tablet scans a QR code from `etd invite` on any machine, or from the app. Every machine learns about every new member in seconds.

To try etd without a service, run `etd` in a terminal. When the network has no device yet, it shows the QR code.

| Command | What it does |
|---|---|
| `etd` | Run the daemon in the foreground |
| `etd install` / `etd uninstall` | Add or remove the user services and the `MM_SSH` line in your shell's startup file |
| `etd invite [--machine] [--url]` | Invite a phone or tablet (QR code), or another machine. Valid for 10 minutes, for one member. |
| `etd join <link> [--name N]` | Join the network of the machine that made the link |
| `etd members` | List the network's machines and devices |
| `etd revoke <name\|id>` | Remove a member from every machine, and close its connections |
| `etd status` | The daemon's id, addresses, network, and connected members |

### How the machines work together

```
iPhone ──iroh──▶ etd on each machine ──▶ tmux, or the manymux node there
mm (any machine) ──MM_SSH=etd-ssh──▶ local etd ──iroh──▶ another machine's etd ──▶ mm agent
```

- **The network** is one list of members (machines and devices), signed by a network key. Machines hold that key; phones and tablets do not, so only a machine can change the list. Each machine keeps a copy and sends changes to the others.
- **manymux** reaches other machines with `ssh <host> mm agent`. etd sets `MM_SSH=etd-ssh`, which does the same through the local etd and iroh. etd also keeps manymux's machine list (`hosts.toml`) equal to the network's machines, so there is no `mm add`.
- **Pushes**: each etd follows its own manymux node. A bell (or a notification request, or a failed exit) in a session that nobody is attached to goes to the machines that have `ETD_NOTIFY_COMMAND` set; they run it. Set it on one machine. While the app is open, it tells the machines so, and they send no push.

## Install

With Nix (Home Manager, Linux or macOS):

```nix
# flake inputs
etd.url = "github:ericsharma/etd";

# home-manager configuration
imports = [ inputs.etd.homeManagerModules.default ];
services.etd.enable = true;
# Optional: send pushes from this machine.
# services.etd.notifyCommand = "my-push-command";
```

The module runs etd and the manymux node (`services.etd.manymux.enable`, on by default), sets `MM_SSH` for your shells, and keeps manymux's machine list equal to the network. A deploy never restarts the manymux node: a restart ends every session on that machine.

From source:

```sh
cargo install --path crates/etd
etd install
```

`etd install` installs `mm` with manymux's own installer when it is missing (pinned to the version etd is tested with), sets up both services, and adds one marked `MM_SSH` line to your shell's startup file. It never restarts a running manymux node, and `etd uninstall` never stops one. If a manymux node already runs that etd did not start, it stays as it is.

On Linux without a desktop login, run `loginctl enable-linger $USER`. Otherwise the services stop when you log out.

## Security

A member of the network can do what you can do in a shell on **every machine** of the network: run commands, and write files in your home directory. Invite only your own devices and machines. Use `etd revoke` for a lost one.

- A new member needs an invite from a machine of the network: a one-time secret, valid for 10 minutes. etd keeps only its hash.
- iroh checks the keys of both ends (QUIC, TLS 1.3). Relays see only encrypted traffic.
- A member list counts only when the network key signed it. The phone checks the signature too.
- Only machines may change the member list (add, revoke). A phone or tablet may invite, but not revoke.
- `etd-ssh` talks to the local etd over a socket that only your user can open.
- Revoking a *machine* also moves the network to a new network key, because the revoked machine knows the old one. The other machines take the new key only together with the change signed by the old key, so the revoked machine cannot follow or make its own. A machine that was off gets the change when it starts again, from any machine that is on.
- A machine that is lost while every other machine is off can still sign changes until one of them revokes it: revoke a lost machine soon.
- Keys and the network: `~/.config/etd` (Linux) or `~/Library/Application Support/etd` (macOS), or `$ETD_HOME`.

## Parts

| Crate | What |
|---|---|
| `crates/et-proto` | The wire types, framing and the network document: [docs/PROTOCOL.md](docs/PROTOCOL.md) |
| `crates/etd` | The daemon and its CLI, `etd-ssh` (manymux's `MM_SSH`), and `etd-probe` (a test client) |
| `crates/et-link` | The app's side, for Swift through UniFFI; `scripts/build-ios.sh` makes the xcframework, `apple/EtLink` is the Swift package |

## Development

```sh
nix develop
env -u TMUX cargo test
```

The tests start their own etds, tmux servers and manymux nodes (`mm --socket <temp>`, with private config and log directories). They never touch your running sessions. Remove `$TMUX` anyway: inside a tmux pane, a tmux command without `-S` goes to that pane's server.

`etd-probe` is a test client: `etd-probe pair <link>`, `etd-probe hello <id>`, `etd-probe machines <id>`, `etd-probe exec-on <id> <host> <script>`, `etd-probe bench <id> <session>`. `cargo run --example agent_ls -- <id> [host]` lists a machine's manymux sessions through etd.

## License

MIT or Apache-2.0, at your choice.
