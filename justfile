default:
    @just --list

fmt:
    rustfmt --all

run *ARGS:
    cargo run {{ARGS}}

watch *ARGS:
	bacon --job run -- -- {{ ARGS }}
