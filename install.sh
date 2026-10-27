#!/bin/sh
# Installs the newest etd release and runs `etd install`.
#   curl -fsSL https://raw.githubusercontent.com/ericsharma/etd/main/install.sh | sh
# ETD_VERSION=v0.0.6  pins a release (default: the newest, prereleases too).
# ETD_BIN=~/bin       installs to another directory (default: ~/.local/bin).
set -eu

repo=ericsharma/etd
bin=${ETD_BIN:-$HOME/.local/bin}

fail() { echo "etd: $*" >&2; exit 1; }

case $(uname -s) in
  Linux) os=unknown-linux-gnu ;;
  Darwin) os=apple-darwin ;;
  *) fail "no prebuilt etd for $(uname -s). Build from source: https://github.com/$repo#other-installs" ;;
esac
case $(uname -m) in
  x86_64 | amd64) arch=x86_64 ;;
  aarch64 | arm64) arch=aarch64 ;;
  *) fail "no prebuilt etd for $(uname -m)" ;;
esac

# /releases/latest skips prereleases, so ask for the newest release of any kind.
version=${ETD_VERSION:-$(curl -fsSL "https://api.github.com/repos/$repo/releases?per_page=1" |
  sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p' | head -n 1)}
[ -n "$version" ] || fail "cannot find the newest release (GitHub API rate limit?). Set ETD_VERSION."

name=etd-$version-$arch-$os.tar.gz
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

echo "etd: downloading $name"
url=https://github.com/$repo/releases/download/$version/$name
{ curl -fsSL -o "$tmp/$name" "$url" && curl -fsSL -o "$tmp/$name.sha256" "$url.sha256"; } ||
  fail "cannot download $url"
if command -v sha256sum >/dev/null; then
  (cd "$tmp" && sha256sum -c "$name.sha256") >/dev/null
else
  (cd "$tmp" && shasum -a 256 -c "$name.sha256") >/dev/null
fi || fail "checksum mismatch for $name"

mkdir -p "$bin"
tar xzf "$tmp/$name" -C "$bin" etd etd-ssh
echo "etd: installed $version to $bin"

"$bin/etd" install

case ":$PATH:" in
  *":$bin:"*) etd=etd ;;
  *) etd=$bin/etd ;;
esac
echo
echo "Next: run \`$etd invite\` and scan the QR code with E.T. on your iPhone."
echo "Or join a machine that is already in a network: \`$etd join <link>\`."
