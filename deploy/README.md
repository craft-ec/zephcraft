# ZephCraft headless deployment (Linux server)

First deployed 2026-07-02 on Hetzner (Ubuntu 24.04, x86_64) — M1.3b gate:
Mac behind NAT ↔ server heartbeats over the public internet.

## Steps (as root)

```bash
# 1. Toolchain
apt-get install -y build-essential pkg-config
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal

# 2. Source + build (rsync from dev machine; repo is private)
#    from dev machine:  rsync -az --exclude target Cargo.toml Cargo.lock zephcraft root@SERVER:/opt/zeph-src/
source $HOME/.cargo/env
cd /opt/zeph-src && cargo build --release -p zeph-noded

# 3. Install
install -m 755 target/release/zeph /usr/local/bin/zeph
useradd --system --home /var/lib/zeph --shell /usr/sbin/nologin zeph
mkdir -p /var/lib/zeph && chown zeph:zeph /var/lib/zeph

# 4. Service + firewall (fixed UDP port so ufw can allow it)
cp /opt/zeph-src/zephcraft/deploy/zeph.service /etc/systemd/system/
ufw allow 9944/udp
systemctl daemon-reload && systemctl enable --now zeph

# 5. Get the node's dialable address
journalctl -u zeph --no-pager | grep ZEPH_ADDR | tail -1
```

Connect from anywhere:

```bash
zeph --reach relayed --peer "<node_id_hex>@<server_ip>:9944"
```

Notes:
- `--reach relayed` uses iroh's default relay + discovery; the server also
  publishes direct addrs, so most connections go direct (RTT ~300ms MY→FSN).
- The web dashboard (M-UI) binds 127.0.0.1 only — reach it via
  `ssh -L 8080:127.0.0.1:8080 root@SERVER` when it lands.

## Relay (M1.8): relay1.zeph.craft.ec

The relay runs as plain HTTP behind Coolify's Traefik, which terminates TLS
on 443 with its Let's Encrypt resolver — no new public ports, no cert cron.

```bash
# 1. Install (server feature required for the binary)
cargo install iroh-relay --locked --features server
install -m 755 ~/.cargo/bin/iroh-relay /usr/local/bin/

# 2. Config: /etc/zeph-relay/config.toml
#    enable_relay = true
#    http_bind_addr = "0.0.0.0:3340"     # plain HTTP; TLS at the proxy
#    (no [tls] section)

# 3. systemd: zeph-relay.service (User=zeph, Restart=on-failure, enabled)
# 4. Firewall: allow container networks to reach the host service
ufw allow from 10.0.0.0/8 to any port 3340 proto tcp
ufw allow from 172.16.0.0/12 to any port 3340 proto tcp

# 5. Traefik route: /data/coolify/proxy/dynamic/zeph-relay.yaml
#    Host(`relay1.zeph.craft.ec`) → http://host.docker.internal:3340
#    tls.certResolver: letsencrypt   (camelCase!)
#    NOTE: after an ACME failure Traefik won't retry until restart:
#    docker restart coolify-proxy    (~5s blip for other services)
```

zeph config: `relay_urls = ["https://relay1.zeph.craft.ec"]`;
`fallback_relays = false` for our-mesh-only (the server runs exclusive —
it's public-IP anyway), true to keep n0 as bootstrap fallback (client default).

Known limitation: no QUIC address discovery (QAD) behind Traefik — in mixed
relay maps iroh's net_report prefers QAD-capable (n0) relays as home. Exclusive
maps home on ours fine. Revisit: relay-terminated TLS + UDP for QAD.

## Tracker (M2.3b): systemd service

```bash
install -m 755 target/release/tracker /usr/local/bin/zeph-tracker
mkdir -p /var/lib/zeph-tracker && chown zeph:zeph /var/lib/zeph-tracker
# /etc/systemd/system/zeph-tracker.service:
#   ExecStart=/usr/local/bin/zeph-tracker --data-dir /var/lib/zeph-tracker --reach relayed --listen-port 9955
ufw allow 9955/udp
systemctl enable --now zeph-tracker
journalctl -u zeph-tracker | grep TRACKER_ADDR   # → <node_id>@<ip>:9955
```

Point nodes at it (config.toml): `trackers = ["<node_id>@46.224.172.252:9955"]`,
or `--tracker <addr>`. Nodes then announce into the node registry and publish/
resolve providers through it. Live tracker id: `69f7b4b2…@46.224.172.252:9955`.
