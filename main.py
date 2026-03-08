import pandas as pd  # type: ignore
import pyarrow  # type: ignore
import glob
import os
import re
import plotly.express as px  # type: ignore

def load_all_years(data_dir='data', cache_file='data/cache_comercio.parquet'):
    """
    Carga todos los años. Usa caché en Parquet si existe para ser 100x más rápido.
    """
    if os.path.exists(cache_file):
        print(f"Cargando dataset desde caché: {cache_file}...")
        return pd.read_parquet(cache_file)

    pattern = os.path.join(data_dir, 'BACI_HS92_Y*_V202601.csv')
    files = sorted(glob.glob(pattern))
    
    if not files:
        print("No se encontraron archivos BACI en la carpeta data.")
        return None

    all_data = []
    print("Iniciando carga inicial de CSVs...")
    for file_path in files:
        file_name = os.path.basename(file_path)
        match = re.search(r'_Y(\d{4})', file_name)
        if match:
            year = match.group(1)
            print(f"-> Procesando año {year}...")
        else:
            print(f"-> Saltando archivo con formato inesperado: {file_name}")
            continue
        df = pd.read_csv(file_path, dtype={
            't': 'int16', 'i': 'int16', 'j': 'int16', 
            'k': 'int32', 'v': 'float32', 'q': 'float32'
        })
        all_data.append(df)
    
    full_df = pd.concat(all_data, ignore_index=True)
    full_df.to_parquet(cache_file, compression='snappy')
    return full_df

def main():
    df = load_all_years()
    
    if df is not None:
        print("\nGenerando vista previa interactiva...")
        
        # Agrupamos por año para el gráfico
        stats = df.groupby('t')['v'].sum().reset_index()
        stats.columns = ['Año', 'Valor Total']
        
        # Creamos gráfico con Plotly
        fig = px.line(stats, x='Año', y='Valor Total', 
                      title='Evolución del Comercio Global (USD)',
                      markers=True,
                      template='plotly_dark')
        
        # Guardamos como HTML
        output_file = 'vista_comercio.html'
        fig.write_html(output_file)
        
        print(f"¡Éxito! Abre el archivo '{output_file}' en tu navegador para ver los datos.")

if __name__ == "__main__":
    main()



