#!/bin/bash

# Ruta absoluta a tu proyecto
PROYECTO_DIR="/home/ubuntu/"
VENV_PATH="$PROYECTO_DIR/venv"
SCRIPT="$PROYECTO_DIR/nba_api_stats.py"
LOG="$PROYECTO_DIR/log_nba_stats.log"

# Línea del cron que ejecutará el script a las 3:00 AM cada día
CRON_LINE="0 3 * * * cd $PROYECTO_DIR && source $VENV_PATH/bin/activate && python $SCRIPT >> $LOG 2>&1"

# Registrar cron si no existe ya
(crontab -l 2>/dev/null | grep -v "$SCRIPT"; echo "$CRON_LINE") | crontab -

echo "✅ Cron job agregado:"
echo "$CRON_LINE"
 