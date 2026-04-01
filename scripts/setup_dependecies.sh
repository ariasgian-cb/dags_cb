#!/bin/bash
# Forzar que el script se detenga si hay algún error
set -e

echo "=== Iniciando instalación OFFLINE de xmltodict ==="

# 1. Definir variables para evitar errores de escritura
WHEEL_NAME="xmltodict-0.13.0-py2.py3-none-any.whl"
BUCKET_PATH="gs://bucket-via-gcb-ia-tributario-hlg-composer/dags/dependencies"

# 2. Copiar el archivo manteniendo su NOMBRE ORIGINAL
echo "Descargando $WHEEL_NAME desde GCS..."
gsutil cp "${BUCKET_PATH}/${WHEEL_NAME}" /tmp/

# 3. Instalar usando el nombre completo del archivo
echo "Instalando paquete..."
python3 -m pip install "/tmp/${WHEEL_NAME}" --no-index --break-system-packages

echo "=== Instalación finalizada con éxito ==="