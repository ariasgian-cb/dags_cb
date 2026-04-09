#!/bin/bash
# Script para baixar todos os .whl necessários para google-genai e tqdm
set -e

echo "=========================================="
echo "=== BAIXANDO DEPENDÊNCIAS .WHL ==="
echo "=========================================="

# Usar pasta arq do projeto
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
ARQ_DIR="$PROJECT_DIR/arq"

cd "$ARQ_DIR"
echo "📂 Salvando em: $ARQ_DIR"

echo ""
echo "📦 Baixando google-genai e todas suas dependências..."
python3 -m pip download google-genai --dest . --only-binary=:all: --python-version 3.11 --platform manylinux2014_x86_64

echo ""
echo "📦 Baixando tqdm..."
python3 -m pip download tqdm --dest . --only-binary=:all: --python-version 3.11 --platform manylinux2014_x86_64

echo ""
echo "✅ Download concluído!"
echo "📂 Arquivos salvos em: $TEMP_DIR"
echo ""
echo "📋 Arquivos baixados:"
ls -lh *.whl

echo ""
echo "=========================================="
echo "Total de arquivos: $(ls -1 *.whl | wc -l)"
echo "=========================================="
