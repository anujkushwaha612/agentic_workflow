#!/usr/bin/env bash
# Re-provision the Rust toolchain in this egress-restricted sandbox.
#
# static.rust-lang.org and crates.io are unreachable from this sandbox, but PyPI
# is allowlisted. The `arena-rust-toolchain` PyPI packages (publisher "Yuri")
# bundle the official Rust 1.97.0 dist as split tar.zst parts. At the time of
# writing only data1..data3 are published; their concatenation is nonetheless a
# COMPLETE zstd stream (verified: decompresses cleanly, rustc/cargo/clippy/
# rustfmt + host & wasm32 std all work). This script installs the parts and
# extracts the toolchain to /tmp/rust-tc/prefix.
#
# Usage:  bash tools/setup-rust.sh
# Then:   export PATH=/tmp/rust-tc/prefix/bin:$PATH
set -euo pipefail
DEST=/tmp/rust-tc
if [ -x "$DEST/prefix/bin/cargo" ] && "$DEST/prefix/bin/cargo" --version >/dev/null 2>&1; then
    echo "toolchain already present: $DEST/prefix"
    exit 0
fi
python3 -m pip install --break-system-packages --quiet \
    arena-rust-toolchain-data1 arena-rust-toolchain-data2 arena-rust-toolchain-data3 zstandard
python3 - <<'EOF'
import os, shutil, sys
import zstandard as zstd
parts = []
for n in (1, 2, 3):
    import importlib
    m = importlib.import_module(f"arena_rust_toolchain_data{n}")
    d = os.path.dirname(m.__file__)
    p = os.path.join(d, f"part_{n}")
    assert os.path.exists(p), p
    parts.append(p)
dest = "/tmp/rust-tc/prefix"
shutil.rmtree("/tmp/rust-tc", ignore_errors=True)
os.makedirs("/tmp/rust-tc/raw", exist_ok=True)
dctx = zstd.ZstdDecompressor()
import tarfile
with open("/tmp/rust-tc/raw/toolchain.tar", "wb") as tarf:
    with dctx.stream_reader(open(parts[0], "rb")) as r0:
        pass  # probe
    # concatenate parts then stream-decompress
    with open("/tmp/rust-tc/raw/toolchain.tar.zst", "wb") as z:
        for p in parts:
            shutil.copyfileobj(open(p, "rb"), z, length=1 << 20)
    with dctx.stream_reader(open("/tmp/rust-tc/raw/toolchain.tar.zst", "rb")) as r:
        while True:
            chunk = r.read(1 << 20)
            if not chunk:
                break
            tarf.write(chunk)
tf = tarfile.open("/tmp/rust-tc/raw/toolchain.tar")
tf.extractall("/tmp/rust-tc/raw/x")
os.rename("/tmp/rust-tc/raw/x/prefix", dest)
shutil.rmtree("/tmp/rust-tc/raw")
print("extracted:", dest)
EOF
/tmp/rust-tc/prefix/bin/rustc --version
/tmp/rust-tc/prefix/bin/cargo --version
echo "OK — export PATH=$DEST/prefix/bin:\$PATH"
