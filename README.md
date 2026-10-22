# etd

`etd` lets the E.T. iPhone app reach tmux on a machine, with no VPN and no open port. It runs an [iroh](https://iroh.computer) endpoint. The phone finds the machine by its key, not by an IP address. Connections are direct when possible, with an encrypted relay when not.

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

A paired device can do what you can do in a shell on this machine: it can run commands and write files in your home directory. Pair only your own devices. Use `etd revoke` for a lost device.

- Each device must scan a QR code from this machine. A pairing code works one time, for 10 minutes.
- iroh checks the keys of both ends (QUIC, TLS 1.3). Relays see only encrypted traffic.
- Keys and paired devices: `~/.config/etd` (Linux) or `~/Library/Application Support/etd` (macOS), or `$ETD_HOME`.

## Development

```sh
nix develop
env -u TMUX cargo test   # the end-to-end test starts its own tmux server
```

`etd-probe` is a test client: `etd-probe pair <link>`, `etd-probe hello <id>`, `etd-probe bench <id> <session>`.

## License

MIT or Apache-2.0, at your choice.
