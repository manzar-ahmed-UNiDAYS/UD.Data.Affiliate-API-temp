#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$HOME/UD.Data.Affiliate-API-temp"
PYTHON_BIN="$HOME/dbt_venv/bin/python"
PIP_BIN="$HOME/dbt_venv/bin/pip"

echo "ℹ️ Building wheel for: $PROJECT_DIR"

if [ ! -x "$PYTHON_BIN" ]; then
  echo "🚨 Python executable not found: $PYTHON_BIN" >&2
  exit 1
fi

if [ ! -x "$PIP_BIN" ]; then
  echo "🚨 pip executable not found: $PIP_BIN" >&2
  exit 1
fi

cd "$PROJECT_DIR"

if ! "$PIP_BIN" show build >/dev/null 2>&1 || ! "$PIP_BIN" show wheel >/dev/null 2>&1; then
  echo "ℹ️ Installing build dependencies"
  "$PIP_BIN" install build wheel
else
  echo "ℹ️ Build dependencies already installed"
fi

echo "ℹ️ Removing previous build artifacts"
rm -rf build dist dbt_affiliate_api.egg-info

echo "ℹ️ Creating wheel"
"$PYTHON_BIN" -m build --wheel --no-isolation

WHEEL_PATH="$(ls -1 dist/*.whl)"

echo "ℹ️ Verifying wheel contents"
"$PYTHON_BIN" -m zipfile -l "$WHEEL_PATH" | grep -q "dbt_affiliate_api_bundle/dbt_project.yml"
"$PYTHON_BIN" -m zipfile -l "$WHEEL_PATH" | grep -q "dbt_affiliate_api_bundle/models/step4_copy_to_redshift.sql"
"$PYTHON_BIN" -m zipfile -l "$WHEEL_PATH" | grep -q "dbt_affiliate_api_bundle/macros/affiliate_helper.sql"

echo "🔔 Wheel build completed"
echo "$WHEEL_PATH"
