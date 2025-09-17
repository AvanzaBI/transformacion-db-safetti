#!/usr/bin/env bash
set -euo pipefail

export HOME=/home/azureuser
cd /home/azureuser/transformacion_db

# activa el venv
source venv/bin/activate

# carga variables de entorno
set -a
[ -f .env ] && source .env
set +a

# ejecuta el runner y loguea con timestamp
ts="$(date +'%Y-%m-%d_%H-%M-%S')"
mkdir -p logs
python run_jobs_load.py >> logs/run_$ts.log 2>&1
echo "[OK] ETL completado: $(date '+%F %T')" >> logs/run_$ts.log

