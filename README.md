# etd

Your terminal sessions, on your iPhone, from every machine you own. No VPN, no open port, no ssh keys.

`etd` is the daemon for [E.T.](https://github.com/ericsharma/et-ios). Install it on each machine, scan one QR code with the phone, and every tmux and [manymux](https://github.com/rayfish/manymux) session on those machines is one tap away.

> **Experimental.** The protocol can change between releases.

## Quick start

Linux or macOS, aarch64 or x86_64:

```sh
curl -fsSL https://raw.githubusercontent.com/ericsharma/etd/main/install.sh | sh
~/.local/bin/etd invite    # shows a QR code
```

The script puts `etd` and `etd-ssh` in `~/.local/bin` (checksum verified) and runs `etd install`, which starts etd and a manymux node as user services. `ETD_VERSION=v0.0.6` pins a release. `ETD_BIN=<dir>` installs to another directory.

1. Install **E.T.** on the iPhone or iPad from [TestFlight](https://testflight.apple.com/join/TODO). <!-- TODO: public TestFlight link -->
2. Scan the QR code.
3. Run `mm new -n hello` on the machine. The session shows in the app.

On a Mac, you can use [E.T. for Mac](https://github.com/ericsharma/et-ios/releases/latest) instead of these commands. It runs etd for you and is also a client.

On Linux without a desktop login, run `loginctl enable-linger $USER`. Otherwise the services stop when you log out.

## Add more machines

On a machine that is in the network:

```sh
etd invite --machine   # prints: etd join <link>
```

Run the install script on the new machine, then run the printed `etd join <link>` there. All machines learn about the new member in seconds.

## Commands

| Command | What it does |
|---|---|
| `etd install` / `etd uninstall` | Add or remove the user services and the `MM_SSH` line in your shell's startup file |
| `etd invite [--machine]` | Invite a phone or tablet (QR code), or another machine. One use, 10 minutes |
| `etd join <link> [--name N]` | Join the network of the machine that made the link |
| `etd members` | List the machines and devices |
| `etd revoke <name\|id>` | Remove a member from every machine |
| `etd send <path>… <machine>[:<session>]` | Send files to `~/etd-downloads/` on another machine, and type their paths into a session |
| `etd status [--json]` | Show the daemon's id, addresses and connected members |
| `etd` | Run the daemon in the foreground, with no service |

## Security

A member of the network can do what you can do in a shell, on **every** machine of the network. Invite only your own devices. Use `etd revoke` for a lost one. Details: [docs/SECURITY.md](docs/SECURITY.md).

## Other installs

<details>
<summary>Nix (Home Manager, Linux or macOS)</summary>

```nix
# flake inputs
etd.url = "github:ericsharma/etd";

# home-manager configuration
imports = [ inputs.etd.homeManagerModules.default ];
services.etd.enable = true;
# Optional: send pushes from this machine.
# services.etd.notifyCommand = "my-push-command";
```

The module runs etd and the manymux node, and sets `MM_SSH` for your shells. A deploy never restarts the manymux node, because a restart ends every session on that machine.
</details>

<details>
<summary>From source</summary>

```sh
cargo install --path crates/etd
etd install
```
</details>

<details>
<summary>macOS: browser downloads</summary>

The binaries are not signed. A browser download gets a quarantine flag, and Gatekeeper blocks it. Use the install script (it uses `curl`), or run `xattr -d com.apple.quarantine ~/.local/bin/etd ~/.local/bin/etd-ssh`.
</details>

## More

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md): how the machines work together, and what `etd install` changes
- [docs/SECURITY.md](docs/SECURITY.md): the trust model
- [docs/PROTOCOL.md](docs/PROTOCOL.md): the wire format
- [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md): build, test, crates

## License

MIT or Apache-2.0, at your choice.
