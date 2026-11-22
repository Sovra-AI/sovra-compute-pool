# Install dist-alloc on this computer

The pool program is one file, `dist-alloc`, published on
<https://github.com/Sovra-AI/sovra-compute-pool/releases>. These steps install the newest
release for the current user only (no administrator password). Run each numbered step as
its own `run_terminal_command` call, and stop and explain if one fails.

1. **Find the right download.** Run `uname -m`.
   - `x86_64` → target `x86_64-unknown-linux-gnu`
   - `aarch64` or `arm64` → target `aarch64-unknown-linux-gnu`
   - anything else: there is no ready-made download; go to "Build it instead" below.
2. **Find the newest version.** Run
   `curl -fsSL https://api.github.com/repos/Sovra-AI/sovra-compute-pool/releases/latest`
   and read `tag_name` (for example `v0.1.0`). The version is the tag without the `v`.
3. **Download it and its fingerprint** into a fresh folder (substitute `VERSION` and
   `TARGET`):

   ```sh
   mkdir -p ~/.cache/dist-alloc-install && cd ~/.cache/dist-alloc-install && curl -fsSLO https://github.com/Sovra-AI/sovra-compute-pool/releases/download/vVERSION/dist-alloc-VERSION-TARGET.tar.gz && curl -fsSLO https://github.com/Sovra-AI/sovra-compute-pool/releases/download/vVERSION/dist-alloc-VERSION-TARGET.tar.gz.sha256
   ```

4. **Check the fingerprint.** In that folder run
   `sha256sum -c dist-alloc-VERSION-TARGET.tar.gz.sha256`. It must print `OK`. If it does
   not, delete the folder, tell the person the download was damaged or tampered with, and
   do not continue.
5. **Install it.**

   ```sh
   cd ~/.cache/dist-alloc-install && tar -xzf dist-alloc-VERSION-TARGET.tar.gz && mkdir -p ~/.local/bin && install -m 0755 dist-alloc-VERSION-TARGET/dist-alloc ~/.local/bin/dist-alloc
   ```

6. **Confirm.** Run `~/.local/bin/dist-alloc --version`. If `command -v dist-alloc` finds
   nothing, `~/.local/bin` is not on the person's `PATH`; keep using the full path
   `~/.local/bin/dist-alloc` in every command.

The download folder can be removed afterwards with `rm -r ~/.cache/dist-alloc-install`.

To **update** later, repeat these steps with the newer version, then restart the services
that are installed: `systemctl --user restart dist-alloc-server dist-alloc-agent` (leave out
any that are not installed on this computer).

## Build it instead

Only when there is no ready-made download, or the person prefers to build from source.
This needs Rust (`cargo --version`; if missing, explain that Rust from <https://rustup.rs>
is needed and ask whether to install it). Then run:

```sh
cargo install --locked --git https://github.com/Sovra-AI/sovra-compute-pool --root ~/.local
```

It takes a few minutes and puts `dist-alloc` in `~/.local/bin`.
