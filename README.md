# etd

Your terminal sessions, on your iPhone, from every machine you own. No VPN, no open port, no ssh keys.

`etd` is the daemon for [E.T.](https://github.com/ericsharma/et-ios). Install it on each machine, scan one QR code with your phone, and every tmux and [manymux](https://github.com/rayfish/manymux) session is one tap away.

> **Experimental.** The protocol can change between releases.

## Why it is cool

- **It works from anywhere.** Each machine is known by its key, not by its IP address ([iroh](https://iroh.computer)). Connections are direct when possible, and go through an encrypted relay when not. You do not configure a router or a firewall.
- **One setup for each device.** Scan a QR code on the phone. Paste one link on a new machine. All the machines learn about each new member.
- **Sessions stay alive.** If you lose Wi-Fi, the session continues on its machine. You go back to it when you connect again.
- **Every machine reaches every machine.** `mm ls` and `mm attach` on any machine show the sessions of all machines, with no ssh config.
- **Files go where you need them.** `etd send report.pdf laptop` puts the file in `~/etd-downloads/` on the laptop.

## Get started

1. Install etd (Linux or macOS):
   ```sh
   curl -fsSL https://raw.githubusercontent.com/ericsharma/etd/main/install.sh | sh
   ```
2. Show a QR code: `etd invite` (the install script prints the full path if `~/.local/bin` is not in your `PATH`)
3. Install **E.T.** on your iPhone or iPad from [TestFlight](https://testflight.apple.com/join/TODO) <!-- TODO: public TestFlight link -->, then scan the QR code.
4. Start a session: `mm new -n hello`. It shows in the app.

On a Mac, you can use [E.T. for Mac](https://github.com/ericsharma/et-ios/releases/latest) instead. It runs etd for you.

On Linux without a desktop login, run `loginctl enable-linger $USER`. If you do not, the services stop when you log out.

## Add a machine

1. On a machine that is already in the network, run `etd invite --machine`. It shows an `etd join <link>` command.
2. On the new machine, run the install script, then run that `etd join <link>` command.

## Commands

| Command | What it does |
|---|---|
| `etd invite [--machine]` | Invite a phone or tablet (QR code), or a machine. One use, 10 minutes. |
| `etd join <link>` | Join the network that made the link. |
| `etd members` | List the machines and devices. |
| `etd revoke <name>` | Remove a member from all machines. |
| `etd send <path>… <machine>[:<session>]` | Send files to another machine. With a session, etd types the file paths into it. |
| `etd status` | Show this machine's id, addresses, and connections. |
| `etd uninstall` | Stop and remove the services. |

`etd help` shows all commands, including the Logbook (an archive of your AI agent transcripts).

## Security

A member of the network can do on **every** machine what you can do in a shell. Invite only your own devices. If you lose a device, use `etd revoke` immediately. See [docs/SECURITY.md](docs/SECURITY.md).

## Other installs

<details>
<summary>Nix (Home Manager)</summary>

```nix
# flake inputs
etd.url = "github:ericsharma/etd";

# home-manager configuration
imports = [ inputs.etd.homeManagerModules.default ];
services.etd.enable = true;
```
</details>

<details>
<summary>From source</summary>

```sh
cargo install --path crates/etd
etd install
```
</details>

<details>
<summary>macOS: the browser download is blocked</summary>

The binaries are not signed, so Gatekeeper blocks a browser download. Use the install script, or run `xattr -d com.apple.quarantine ~/.local/bin/etd ~/.local/bin/etd-ssh`.
</details>

## More

- [How it works](docs/ARCHITECTURE.md)
- [Security](docs/SECURITY.md)
- [Protocol](docs/PROTOCOL.md)
- [Development](docs/DEVELOPMENT.md)

## License

MIT or Apache-2.0, at your choice.
