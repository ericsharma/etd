# Vendored crates

## netdev 0.46.3 (MIT, https://github.com/shellrow/netdev)

Pulled in by iroh 1.4 (via netwatch and portmapper). Upstream compiles its
`os/darwin` IPv6 address-flag lookup for every Apple OS, but `libc` declares
`in6_ifreq` only for macOS, so iroh does not build for iOS (S5 Mac run).

The one change is in `src/os/darwin/ipv6_addr_flags.rs`: on Apple OSes other
than macOS the function returns empty flags. Remove this vendored copy once
an upstream netdev release fixes it and netwatch depends on that release.
