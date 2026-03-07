# Análisis de Comercio Global (BACI Dataset 1995-2024) 🌍

Este proyecto es una plataforma de análisis interactivo diseñada para procesar el dataset **BACI (International Trade Database)**, con más de 270 millones de registros de transacciones comerciales internacionales.

## 🚀 Características
- **Procesamiento Masivo**: Utiliza **Parquet** para cargar 30 años de datos en segundos después de la primera indexación.
- **Visualización Interactiva**: Gráficos dinámicos con **Plotly** exportables a HTML.
- **Optimizado**: Configuración específica para hardware moderno (aprovecha múltiples hilos y memoria RAM).

## 🛠️ Instalación y Configuración (Windows)
1. Clona el repositorio.
2. Asegúrate de tener Python 3.11+ instalado.
3. Ejecuta el script de configuración automática:
   ```powershell
   .\setup.ps1
   ```

## 📁 Estructura de Datos
Para que el script funcione, debes colocar los archivos CSV de BACI en la carpeta `data/`. 
*(Nota: La carpeta `data/` y el entorno `.venv/` están excluidos del repositorio para mantenerlo ligero).*

## 📈 Uso
Ejecuta el script principal:
```powershell
.\.venv\Scripts\python.exe main.py
```
Esto generará un archivo `vista_comercio.html` con las visualizaciones actualizadas.

---
*Desarrollado para análisis avanzado de economía internacional.*