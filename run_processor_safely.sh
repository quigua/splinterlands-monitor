#!/bin/bash
# Script orquestador para sincronizar main.py y process_raw_battles.py

LOG_FILE="/mnt/ssd/Splinterlands/orchestrator.log"

echo "$(date) --- Iniciando ciclo de procesamiento seguro ---" >> $LOG_FILE

# Paso 1: Detener el servicio principal (main.py)
echo "$(date) Deteniendo splinterlands-monitor.service..." >> $LOG_FILE
/usr/bin/systemctl stop splinterlands-monitor.service >> $LOG_FILE 2>&1

# Paso 2: Ejecutar el script de procesamiento de batallas
echo "$(date) Ejecutando process_raw_battles.py..." >> $LOG_FILE
/usr/bin/python3 /mnt/ssd/Splinterlands/process_raw_battles.py >> $LOG_FILE 2>&1

# Paso 3: Reiniciar el servicio principal (main.py)
echo "$(date) Reiniciando splinterlands-monitor.service..." >> $LOG_FILE
/usr/bin/systemctl start splinterlands-monitor.service >> $LOG_FILE 2>&1

echo "$(date) --- Ciclo de procesamiento seguro completado ---" >> $LOG_FILE
