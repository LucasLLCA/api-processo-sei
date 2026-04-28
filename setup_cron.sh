#!/bin/bash
# Script para configurar cron job - atualiza tipos_documento todo dia às 02:00

CRON_DIR="/Users/brunoibiapina/Desktop/NTGD/Sistema Sei/visualizador-de-processos-api-main"
VENV_PYTHON="$CRON_DIR/venv/bin/python"
SCRIPT="$CRON_DIR/scripts/bootstrap/populate_tipos_documento.py"
LOG_FILE="$CRON_DIR/cron_tipos_documento.log"

# Adicionar ao crontab (executa todo dia às 02:00)
CRON_JOB="0 2 * * * cd $CRON_DIR && $VENV_PYTHON $SCRIPT >> $LOG_FILE 2>&1"

# Verificar se já existe
if crontab -l 2>/dev/null | grep -q "populate_tipos_documento"; then
    echo "Cron job já existe!"
else
    (crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -
    echo "✅ Cron job adicionado com sucesso!"
    echo "Horário: 02:00 todo dia"
    echo "Log: $LOG_FILE"
fi
