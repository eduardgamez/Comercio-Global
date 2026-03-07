# Script de configuración automática para el proyecto Comercio-Global

Write-Host "Iniciando configuración del entorno virtual..." -ForegroundColor Cyan

# 1. Crear el entorno virtual si no existe
if (-not (Test-Path ".venv")) {
    Write-Host "Creando entorno virtual (.venv)..."
    python -m venv .venv
}
else {
    Write-Host "El entorno virtual ya existe."
}

# 2. Activar el entorno e instalar dependencias
Write-Host "Instalando dependencias desde requirements.txt..." -ForegroundColor Cyan
& ".\.venv\Scripts\python.exe" -m pip install -r requirements.txt

Write-Host "`n¡Configuración completada con éxito!" -ForegroundColor Green
Write-Host "Para activar el entorno manualmente, usa: .\.venv\Scripts\Activate.ps1"
Write-Host "Para ejecutar el programa, usa: .\.venv\Scripts\python.exe main.py`n"

# 3. Preguntar si se desea ejecutar el programa ahora
$choice = Read-Host "¿Deseas ejecutar main.py ahora? (s/n)"
if ($choice -eq 's') {
    & ".\.venv\Scripts\python.exe" main.py
}
