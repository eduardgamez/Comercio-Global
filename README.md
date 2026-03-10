# Análisis de Comercio Global (BACI Dataset 1995-2024) 🌍

Este proyecto es una plataforma de análisis interactivo diseñada para procesar el dataset **BACI (International Trade Database)**, con más de 270 millones de registros de transacciones comerciales internacionales.

## 🚀 Características
- **Procesamiento Masivo**: Utiliza **Parquet** para cargar 30 años de datos en segundos después de la primera indexación.
- **Visualización Interactiva**: Gráficos dinámicos con **Plotly** exportables a HTML.
- **Optimizado**: Configuración específica para hardware moderno (aprovecha múltiples hilos y memoria RAM).

## 🛠️ Instalación y Configuración (Windows)
1. **Clona** el repositorio.
2. **Prepara los datos**: Copia tus archivos CSV de BACI en la carpeta `data/`.
3. **Configura el entorno**: Ejecuta el script de automatización en PowerShell:
   ```powershell
   .\setup.ps1
   ```
   *(Este script creará el entorno virtual e instalará las librerías necesarias).*
4. **¡Listo!**: El script te preguntará si quieres ejecutar el análisis al finalizar.

## 📈 Uso
Ejecuta el script principal:
```powershell
.\.venv\Scripts\python.exe main.py
```
Esto generará un archivo `vista_comercio.html` con las visualizaciones actualizadas.

## 🐍 Snake (mini-juego)
Este repo también incluye una implementación mínima de **Snake** (sin dependencias extra) para ejecutar en terminal.

### Ejecutar Snake
Desde la raíz del proyecto:
```bash
python3 -m src.snake.cli
```

Controles: flechas o WASD, `Space`/`P` para pausar, `R` reinicia, `Q` sale.

### Tests (lógica de Snake)
```bash
python3 -m unittest discover -s tests
```

---
*Desarrollado para análisis avanzado de economía internacional.*
