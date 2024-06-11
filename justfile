set shell := ["bash", "-uc"]

check:
	cargo check -p evented
	cargo check -p rusty-accounts

fmt toolchain="+nightly":
	cargo {{toolchain}} fmt

fmt-check toolchain="+nightly":
	cargo {{toolchain}} fmt --check

lint:
	cargo clippy --no-deps -- -D warnings

test:
	cargo test -p evented
	cargo test -p rusty-accounts

fix:
	cargo fix --allow-dirty --allow-staged

doc toolchain="+nightly":
	RUSTDOCFLAGS="-D warnings --cfg docsrs" cargo {{toolchain}} doc -p evented --no-deps

all: check fmt lint test doc
