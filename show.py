import pandas as pd

# Lee el archivo parquet descargado
df = pd.read_parquet('part-00000-xxxx.snappy.parquet')

# Muestra las primeras filas
print(df.head())

# O muestra todo si es pequeño
print(df)