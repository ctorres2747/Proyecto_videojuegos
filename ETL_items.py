import pandas as pd
import numpy as np
import ast
import json
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq

rows = []
with open ('australian_users_items.json', encoding= 'MacRoman') as f:
    for line in f.readlines():
        rows.append(ast.literal_eval(line))        
df_items = pd.DataFrame(rows)

# Crear una función lambda para desanidar las filas y agregar las columnas originales
expand_items = lambda row: pd.DataFrame(row['items']).assign(user_id=row['user_id'], items_count=row['items_count'], steam_id=row['steam_id'], user_url=row['user_url'])
# Aplicamos la función lambda a la columna 'items' y concatenamos los resultados en un nuevo DataFrame
expanded_items_df = pd.concat(df_items.apply(expand_items, axis=1).tolist(), ignore_index=True)
# Eliminamos columnas redundantes dado que steam_id es igual a user_id
expanded_items_df.drop(columns=['steam_id', 'user_url'], inplace= True)
# Eliminamos la columna items_count dado que la podemos calcular y la columna item_name la tenemos en el dataframe games
expanded_items_df.drop(columns=['items_count', 'item_name'], inplace=True)
# Eliminamos otra columna dado que no la necesitamos para el análisis
expanded_items_df.drop(columns=['playtime_2weeks'], inplace=True)
# Transformamos en entero la columna playtime_forever
expanded_items_df['playtime_forever'] = expanded_items_df['playtime_forever'].astype(int)
# Creamos una tabla
table = pa.Table.from_pandas(expanded_items_df)
# Transformamos en archivo parquet
pq.write_table(table, 'items.parquet')