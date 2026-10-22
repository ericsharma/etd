#!/usr/bin/env bash
# Builds et-link for Apple platforms. Run on a Mac with Xcode and rustup.
# Output (not in git):
#   apple/EtLink/EtLinkFFI.xcframework      iOS device, iOS simulator, macOS
#   apple/EtLink/Sources/EtLink/et_link.swift  the generated Swift API
# Then: cd apple/EtLink && swift test
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$PWD"
PKG="$ROOT/apple/EtLink"
GEN="$ROOT/target/uniffi-swift"
# On some Macs the default toolchain is x86_64 (Rosetta); build natively.
TC="${RUST_TOOLCHAIN:-stable-aarch64-apple-darwin}"
CARGO="cargo +$TC"
TARGETS=(aarch64-apple-ios aarch64-apple-ios-sim aarch64-apple-darwin)
# Match the app (iOS 18) and the package (macOS 15), or the C parts (ring)
# are built for the SDK version and the linker warns.
export MACOSX_DEPLOYMENT_TARGET=15.0 IPHONEOS_DEPLOYMENT_TARGET=18.0
rustup target add --toolchain "$TC" "${TARGETS[@]}" >/dev/null

for t in "${TARGETS[@]}"; do
  $CARGO rustc -p et-link --release --lib --crate-type staticlib --target "$t"
done

rm -rf "$GEN" && mkdir -p "$GEN/headers"
$CARGO run -p et-link --release --bin uniffi-bindgen -- generate \
  --library target/aarch64-apple-darwin/release/libet_link.a --language swift --out-dir "$GEN"
# The xcframework wants one header directory with a module.modulemap.
cp "$GEN/et_linkFFI.h" "$GEN/headers/"
cp "$GEN/et_linkFFI.modulemap" "$GEN/headers/module.modulemap"

args=()
for t in "${TARGETS[@]}"; do
  args+=(-library "target/$t/release/libet_link.a" -headers "$GEN/headers")
done
rm -rf "$PKG/EtLinkFFI.xcframework"
xcodebuild -create-xcframework "${args[@]}" -output "$PKG/EtLinkFFI.xcframework"
mkdir -p "$PKG/Sources/EtLink"
cp "$GEN/et_link.swift" "$PKG/Sources/EtLink/et_link.swift"
echo "OK: $PKG/EtLinkFFI.xcframework + Sources/EtLink/et_link.swift"
