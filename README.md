# apt-mirror

A pull-through cache for apt repositories (Ubuntu, Debian, Docker, NodeSource) that keeps
`apt-get update` and `apt-get install` working when the internet is gone.

It is a caching proxy, not a full mirror: files are fetched the first time a client asks for
them, and every refresh also prefetches the new versions of packages that clients have used
before. Repository metadata is served from consistent, signed snapshots, so clients never see
a `Hash Sum mismatch`, and a new snapshot only goes live once the packages it makes clients
want are already in the cache.

## Run it

Node 22 or newer, no dependencies:

```sh
git clone <this repo> apt-mirror && cd apt-mirror
node src/main.js serve
```

On first run `config.json` is created from `config.example.json`. Edit it: remove the
upstreams, suites and architectures you don't need (each one costs disk space and refresh
time) and restart. All state lives under `./data/`.

```sh
node src/main.js serve                                   # listen on :3142, refresh every refreshMinutes
node src/main.js refresh [<upstream>[/<suite>]] [--recheck]
node src/main.js warm <export-file>... [--no-closure]
node src/main.js warm --os ubuntu:noble --arch amd64 nginx postgresql-16
node src/main.js gc --older-than-days 90 [--dry-run]
node src/main.js status
```

Every command accepts `--config <path>`. `LOG_LEVEL=debug` shows more detail. Logs are JSON
lines on stdout.

If your network intercepts TLS (a corporate proxy re-signing HTTPS with its own root CA),
the HTTPS upstreams (Docker, NodeSource) fail with "unable to verify the first certificate".
Run Node with the system CA store: `node --use-system-ca src/main.js serve`.

Debian's `-security` and `-updates` suites only publish `Packages.xz`, which Node cannot
decompress. Clients use them normally, but refreshes cannot prefetch new versions from them
(a warning is logged) and their downloads are checked against `Content-Length` only, so run
`warm` regularly for Debian machines.

## Client setup

Clients use `http://<mirror>:3142/<upstream>` as `URIs`. Remove `/etc/apt/sources.list` entries
and the distribution's default `/etc/apt/sources.list.d/ubuntu.sources` or `debian.sources`
(or replace their contents), otherwise apt keeps talking to the internet directly.
Below, replace `mirror` with your mirror's host name.

Ubuntu 24.04, amd64 — `/etc/apt/sources.list.d/ubuntu.sources`:

```
Types: deb
URIs: http://mirror:3142/ubuntu
Suites: noble noble-updates noble-backports
Components: main restricted universe multiverse
Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg

Types: deb
URIs: http://mirror:3142/ubuntu-security
Suites: noble-security
Components: main restricted universe multiverse
Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg
```

Ubuntu 24.04, arm64 — `/etc/apt/sources.list.d/ubuntu.sources`:

```
Types: deb
URIs: http://mirror:3142/ubuntu-ports
Suites: noble noble-updates noble-backports noble-security
Components: main restricted universe multiverse
Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg
```

Debian 12 — `/etc/apt/sources.list.d/debian.sources` (use `trixie` for Debian 13):

```
Types: deb
URIs: http://mirror:3142/debian
Suites: bookworm bookworm-updates
Components: main contrib non-free non-free-firmware
Signed-By: /usr/share/keyrings/debian-archive-keyring.gpg

Types: deb
URIs: http://mirror:3142/debian-security
Suites: bookworm-security
Components: main contrib non-free non-free-firmware
Signed-By: /usr/share/keyrings/debian-archive-keyring.gpg
```

Docker on Ubuntu (use `docker-debian` and `bookworm`/`trixie` on Debian):

```sh
install -m 0755 -d /etc/apt/keyrings
curl -fsSL http://mirror:3142/keys/docker -o /etc/apt/keyrings/docker.asc
cat > /etc/apt/sources.list.d/docker.sources <<'EOF'
Types: deb
URIs: http://mirror:3142/docker-ubuntu
Suites: noble
Components: stable
Signed-By: /etc/apt/keyrings/docker.asc
EOF
```

NodeSource (Node 22):

```sh
install -m 0755 -d /etc/apt/keyrings
curl -fsSL http://mirror:3142/keys/nodesource -o /etc/apt/keyrings/nodesource.asc
cat > /etc/apt/sources.list.d/nodesource.sources <<'EOF'
Types: deb
URIs: http://mirror:3142/nodesource
Suites: nodistro
Components: main
Signed-By: /etc/apt/keyrings/nodesource.asc
EOF
```

The keys are cached by the mirror and refreshed daily. Release files are served byte for byte,
so apt verifies them against the distribution's own keys.

## Warm the cache from real machines

The mirror only prefetches packages it has seen. Packages that came with a base image never
passed through it, yet a minimal machine needs them as dependencies when it reinstalls or
upgrades during an outage. Export the complete installed package list on every important
machine and image:

```sh
{ . /etc/os-release; echo "#os $ID:$VERSION_CODENAME $(dpkg --print-architecture)"; \
  dpkg-query -W -f='${Package} ${Version} ${Architecture}\n'; } > $(hostname).pkgs
```

Then on the mirror:

```sh
node src/main.js warm *.pkgs
```

For each package this fetches the exact installed version (if an index still lists it), the
current candidate, and their dependency closure (Depends, Pre-Depends and Recommends). Every
name is remembered in `data/cache/known.json`, so later refreshes keep it current. Run the
export periodically (for example from cron) and copy the files to the mirror; it is cheap
when nothing changed.

## During an outage

- `apt-get update` keeps working: it is served from the last promoted snapshot. Responses
  carry `X-Cache: STALE` once a refresh attempt has failed.
- `apt-get install` works for every package in the cache. A package that was never cached
  gets `503` (temporary), never `404`.
- Some repositories (Debian security, for example) sign a `Valid-Until` date into their
  Release file, and the mirror cannot extend it without breaking the signature. `status`
  shows the date and the server logs a warning 48 hours before. After it passes, clients
  need:

  ```sh
  apt-get -o Acquire::Check-Valid-Until=false update
  ```

## Back up `data/cache/`

Unlike a normal cache, `data/cache/` cannot be rebuilt while offline: it holds the only copy
of the packages your machines depend on when upstream is unreachable. Back it up.
`gc --older-than-days N` removes packages that no current index references and that nobody
has downloaded for N days; it never touches `known.json`, snapshots or keys.
