# Development

```sh
nix develop
env -u TMUX cargo test
```

The tests start their own etds, tmux servers and manymux nodes (`mm --socket <temp>`, with private config and log directories). They never touch your running sessions. Remove `$TMUX` anyway: inside a tmux pane, a tmux command without `-S` goes to that pane's server.

## Crates

| Crate | What |
|---|---|
| `crates/et-proto` | The wire types, framing and the network document: [PROTOCOL.md](PROTOCOL.md) |
| `crates/etd` | The daemon and its CLI, `etd-ssh` (manymux's `MM_SSH`), and `etd-probe` (a test client) |
| `crates/et-link` | The app's side, for Swift through UniFFI. `scripts/build-ios.sh` makes the xcframework, `apple/EtLink` is the Swift package |

## Test client

`etd-probe pair <link>`, `etd-probe hello <id>`, `etd-probe machines <id>`, `etd-probe exec-on <id> <host> <script>`, `etd-probe bench <id> <session>`.

`cargo run --example agent_ls -- <id> [host]` lists a machine's manymux sessions through etd.

## Releases

Push a `v*` tag. CI publishes `etd-<version>-<target>.tar.gz` (with `.sha256`) for aarch64 and x86_64, macOS and Linux. `install.sh` finds the newest release by itself.
