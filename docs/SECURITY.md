# Security

A member of the network can do what you can do in a shell on **every machine** of the network: run commands, and write files in your home directory. Invite only your own devices and machines. Use `etd revoke` for a lost one.

- A new member needs an invite from a machine of the network: a one-time secret, valid for 10 minutes. etd keeps only its hash.
- iroh checks the keys of both ends (QUIC, TLS 1.3). Relays see only encrypted traffic.
- A member list counts only when the network key signed it. The phone checks the signature too.
- Only machines may change the member list (add, revoke). A phone or tablet may invite, but not revoke.
- `etd-ssh` talks to the local etd over a socket that only your user can open.
- Revoking a *machine* also moves the network to a new network key, because the revoked machine knows the old one. The other machines take the new key only together with the change signed by the old key, so the revoked machine cannot follow or make its own. A machine that was off gets the change when it starts again, from any machine that is on.
- A machine that is lost while every other machine is off can still sign changes until one of them revokes it. Revoke a lost machine soon.
