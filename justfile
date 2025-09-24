default:
    @just --list

fmt:
    cargo clippy --fix --allow-dirty && rustfmt --edition=2021 **/*.rs

run *ARGS:
    cargo run {{ARGS}}

watch *ARGS:
	bacon --job run -- -- {{ ARGS }}

etcd:
    etcd