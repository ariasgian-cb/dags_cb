#!/bin/bash
# Abortar si hay un error
set -e

# Instalar xmltodict usando el binario de python del sistema
echo "Instalando dependencias de Python..."
python3 -m pip install xmltodict