#!/bin/bash
# Forzar que el script se detenga si hay algún error
set -e

echo "============================================="
echo "=== INICIANDO INSTALAÇÃO DE DEPENDÊNCIAS ==="
echo "============================================="

# ============================================
# 1. XMLTODICT (OFFLINE - já existe)
# ============================================
echo ""
echo "1️⃣ Instalando xmltodict (OFFLINE)..."
WHEEL_NAME="xmltodict-1.0.4-py3-none-any.whl"
BUCKET_PATH="gs://bucket-via-gcb-ia-tributario-hlg-composer/dags/dependencies"

echo "   Baixando $WHEEL_NAME do GCS..."
gsutil cp "${BUCKET_PATH}/${WHEEL_NAME}" /tmp/

echo "   Instalando pacote..."
python3 -m pip install "/tmp/${WHEEL_NAME}" --no-index --break-system-packages
echo "   ✅ xmltodict instalado!"

# ============================================
# 2. DEPENDÊNCIAS DE IA (OFFLINE - subpasta ia/)
# ============================================
echo ""
echo "2️⃣ Instalando dependências de IA (OFFLINE)..."

# Baixar TODOS os .whl da subpasta ia/
echo "   Baixando todos os .whl da subpasta ia/..."
gsutil -m cp "${BUCKET_PATH}/ia/*.whl" /tmp/

# Instalar TODOS os .whl baixados (google-genai, tqdm e todas as dependências)
echo "   Instalando todos os pacotes..."
python3 -m pip install /tmp/*.whl --no-index --break-system-packages --quiet

echo "   ✅ Todas as dependências de IA instaladas!"

# Verificar instalação
echo ""
echo "3️⃣ Verificando instalações..."
python3 -c "import xmltodict; print('   ✅ xmltodict OK')"
python3 -c "import google.genai; print('   ✅ google.genai OK')"
python3 -c "import tqdm; print('   ✅ tqdm OK')"

echo ""
echo "============================================="
echo "✅ TODAS AS DEPENDÊNCIAS INSTALADAS!"
echo "============================================="
