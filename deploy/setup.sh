#!/usr/bin/env bash
# YesDB Cloud — Azure VM provisioning script
# Run on a fresh Ubuntu 22.04 Azure VM (B1s or B1ms)
#
# Usage:
#   chmod +x setup.sh
#   sudo ./setup.sh
#
# Prerequisites:
#   - Set a DNS label on your Azure VM (e.g. "yesdb") in the portal
#     → Networking → Public IP → Configuration → DNS name label
#   - Open port 443 (HTTPS) in the Azure Network Security Group
#   - Open port 80 (HTTP, for certbot) in the Azure NSG

set -euo pipefail

DOMAIN="yesdb.centralindia.cloudapp.azure.com"  # Change to your actual DNS
REPO_URL="https://github.com/AzharAhmed-bot/yesdb.git"

echo "=== YesDB Cloud Setup ==="

# ── 1. System packages ───────────────────────────────────────────
echo "[1/7] Installing system packages..."
apt-get update -qq
apt-get install -y -qq python3 python3-pip python3-venv nginx certbot python3-certbot-nginx git

# ── 2. Create yesdb system user ──────────────────────────────────
echo "[2/7] Creating yesdb user..."
if ! id -u yesdb &>/dev/null; then
    useradd --system --shell /bin/false --home /opt/yesdb yesdb
fi

# ── 3. Clone repo and set up venv ────────────────────────────────
echo "[3/7] Setting up application..."
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
echo "[4/7] Creating data directories..."
mkdir -p /var/lib/yesdb/data
chown -R yesdb:yesdb /var/lib/yesdb
chown -R yesdb:yesdb /opt/yesdb

# ── 5. Install systemd service ───────────────────────────────────
echo "[5/7] Installing systemd service..."
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

# ── 6. Configure nginx ───────────────────────────────────────────
echo "[6/7] Configuring nginx..."
cp yes_db/deploy/nginx.conf /etc/nginx/sites-available/yesdb
ln -sf /etc/nginx/sites-available/yesdb /etc/nginx/sites-enabled/yesdb
rm -f /etc/nginx/sites-enabled/default

# Test nginx config before reloading
nginx -t
systemctl reload nginx

# ── 7. SSL certificate ───────────────────────────────────────────
echo "[7/7] Obtaining SSL certificate..."
certbot --nginx -d "$DOMAIN" --non-interactive --agree-tos --email admin@example.com

echo ""
echo "=== Setup complete ==="
echo "Server: https://$DOMAIN"
echo "Health: https://$DOMAIN/api/v1/health"
echo ""
echo "Useful commands:"
echo "  sudo systemctl status yesdb      # Check server status"
echo "  sudo journalctl -u yesdb -f      # View server logs"
echo "  sudo systemctl restart yesdb     # Restart server"
