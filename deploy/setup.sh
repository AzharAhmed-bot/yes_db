#!/usr/bin/env bash
# YesDB Cloud — self-hosted provisioning script
# Run on any systemd-capable Linux machine (bare metal, VM, or WSL2 with
# systemd enabled) that is already joined to your Tailscale network.
# Public HTTPS access is provided by Tailscale Funnel — no domain, no
# router port forwarding, and no certbot needed.
#
# Usage:
#   chmod +x setup.sh
#   sudo ./setup.sh
#
# Prerequisites:
#   - Tailscale installed and logged in on this machine (`tailscale status`)
#   - HTTPS certificates enabled for your tailnet:
#       https://login.tailscale.com/admin/dns -> "Enable HTTPS Certificates"
#   - Funnel enabled for this node (on by default for most personal plans;
#     check the tailnet admin console -> Access Controls if it's refused)
#   - WSL2 only: systemd enabled in /etc/wsl.conf ([boot] systemd=true),
#     then `wsl --shutdown` from Windows and reopen the terminal

set -euo pipefail

REPO_URL="https://github.com/AzharAhmed-bot/yes_db.git"
PORT=8420  # chosen to avoid colliding with common dev-server defaults (3000/5000/8000/8080)

echo "=== YesDB Cloud Setup ==="

# ── 1. System packages ───────────────────────────────────────────
echo "[1/6] Installing system packages..."
apt-get update -qq
apt-get install -y -qq python3 python3-pip python3-venv git

if ! command -v tailscale &>/dev/null; then
    echo "  ERROR: tailscale not found. Install and log in first: https://tailscale.com/download"
    exit 1
fi

# ── 2. Create yesdb system user ──────────────────────────────────
echo "[2/6] Creating yesdb user..."
if ! id -u yesdb &>/dev/null; then
    useradd --system --shell /bin/false --home /opt/yesdb yesdb
fi

# ── 3. Clone repo and set up venv ────────────────────────────────
echo "[3/6] Setting up application..."
mkdir -p /opt/yesdb
cd /opt/yesdb

if [ -d "yes_db" ]; then
    cd yes_db && git pull && cd ..
else
    git clone "$REPO_URL" yes_db
fi

python3 -m venv venv
./venv/bin/pip install --upgrade pip -q
./venv/bin/pip install -e yes_db -q
./venv/bin/pip install -r yes_db/server/requirements.txt -q

# ── 4. Create data directories ───────────────────────────────────
echo "[4/6] Creating data directories..."
mkdir -p /var/lib/yesdb/data
chown -R yesdb:yesdb /var/lib/yesdb
chown -R yesdb:yesdb /opt/yesdb

# ── 5. Install systemd service ───────────────────────────────────
echo "[5/6] Installing systemd service..."
cp yes_db/deploy/yesdb.service /etc/systemd/system/yesdb.service
systemctl daemon-reload
systemctl enable yesdb
systemctl start yesdb

# Wait for server to start
sleep 2
if systemctl is-active --quiet yesdb; then
    echo "  Server is running."
else
    echo "  WARNING: Server failed to start. Check: journalctl -u yesdb"
fi

# ── 6. Expose via Tailscale Funnel ────────────────────────────────
echo "[6/6] Enabling Tailscale Funnel on port $PORT..."
tailscale funnel --bg "$PORT"

PUBLIC_URL=$(tailscale funnel status 2>/dev/null | grep -o 'https://[^ ]*' | head -1)

echo ""
echo "=== Setup complete ==="
echo "Server: ${PUBLIC_URL:-<run: tailscale funnel status>}"
echo "Health: ${PUBLIC_URL:-<see above>}/api/v1/health"
echo ""
echo "Useful commands:"
echo "  sudo systemctl status yesdb      # Check server status"
echo "  sudo journalctl -u yesdb -f      # View server logs"
echo "  sudo systemctl restart yesdb     # Restart server"
echo "  tailscale funnel status          # Show public Funnel URL"
echo "  tailscale funnel off             # Stop public exposure"
