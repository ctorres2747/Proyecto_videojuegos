import pandas as pd
import numpy as np
import ast
import json
from datetime import datetime
from collections import Counter
import re


# Script para ETL games
# Leemos los datos de JSON 
rows = []
with open('output_steam_games.json', encoding='utf-8') as f:
    for line in f.readlines():
        rows.append(json.loads(line))

df_output = pd.DataFrame(rows)

# Eliminar las filas en las que todos los valores son nulos pasamos de 120.445 filas a 32135 filas
df_output.dropna(axis=0, how='all', inplace=True)
# Eliminamos la columna 'app_name' dado que es la misma que title
# Eliminamos la columna reviews_url dado que ya tenemos información en el df de reviews
df_output.drop(columns=['app_name', 'reviews_url'], inplace= True)
# Reemplazamos el nombre de la columna 'id' por 'item_id'
df_output.rename(columns={'id': 'item_id'}, inplace=True)
# Eliminamos la columna publisher dado que no aporta información relevante para el análisis
df_output.drop(columns='publisher', inplace= True)
"""
Analizando la columna genres queremos ver las diferentes categorias que definene el género de los videojuegos

"""
# Crear una lista para almacenar todas las palabras (categorías)
all_categories = []
# Recorrer cada celda de la columna 'genres'
for genres_list in df_output['genres']:
    if isinstance(genres_list, list):  # Verificar si es una lista antes de iterar
        all_categories.extend(genres_list)

# Calcular la frecuencia de cada palabra (categoría)
word_frequencies = Counter(all_categories)

"""
Las siguientes lineas de código se realizan para revisar la información contenida en la columna genre y tags
"""

# Analizando la columna genres queremos ver las diferentes categorias que definene el género de los videojuegos
# Crear una lista para almacenar todas las palabras (categorías)
all_categories = []
# Recorrer cada celda de la columna 'genres'
for genres_list in df_output['genres']:
    if isinstance(genres_list, list):  # Verificar si es una lista antes de iterar
        all_categories.extend(genres_list)

# Calcular la frecuencia de cada palabra (categoría)
word_frequencies = Counter(all_categories)
# Mostrar las palabras y sus frecuencias
for word, frequency in word_frequencies.items():
    print(f'{word}: {frequency}')
    
# Analizando la columna tags queremos ver si hay similitud con la columna genre
# Crear una lista para almacenar todas las palabras (tags)
all_tags = []
# Recorrer cada celda de la columna 'tags'
for tags_list in df_output['tags']:
    if isinstance(tags_list, list):  # Verificar si es una lista antes de iterar
        all_tags.extend(tags_list)
# Calcular la frecuencia de cada palabra (tag)
tag_frequencies = Counter(all_tags)
# Mostrar las palabras y sus frecuencias
for tag, frequency in tag_frequencies.items():
    print(f'{tag}: {frequency}')
    
"""
Eliminamos la columa tags dado que ofrece información muy específica para cada juego y orientada al género 
lo cual es redundante para el estudio.
"""
df_output.drop(columns='tags', inplace= True)
# Crear un conjunto de todas las categorías únicas
all_categories = set()
for genres_list in df_output['genres']:
    if isinstance(genres_list, list):
        all_categories.update(genres_list)
# Generar las columnas codificadas para cada categoría
for category in all_categories:
    df_output[category] = df_output['genres'].apply(lambda x: 1 if isinstance(x, list) and category in x else 0)
    
"""
Las siguientes lineas de código se realizan para revisar la información contenida en la columna specs
"""

# Crear una lista para almacenar todas las palabras (specs)
all_specs = []
# Recorrer cada celda de la columna 'tags'
for specs_list in df_output['specs']:
    if isinstance(specs_list, list):  # Verificar si es una lista antes de iterar
        all_specs.extend(specs_list)
# Calcular la frecuencia de cada palabra (specs)
specs_frequencies = Counter(all_specs)
# Mostrar las palabras y sus frecuencias
for specs, frequency in specs_frequencies.items():
    print(f'{specs}: {frequency}')
    
# Revisamos la cantidad de categorías que tenemos en specs
# Crear un conjunto de todas las especificaciones únicas
unique_specs = set()
for specs_list in df_output['specs']:
    if isinstance(specs_list, list):
        unique_specs.update(specs_list)

# Obtener la cantidad de especificaciones únicas
num_unique_specs = len(unique_specs)

print(f'Cantidad de especificaciones únicas: {num_unique_specs}')

# Eliminamos la columa specs dado que tiene 40 categorías que pueden ser relacionadas también al genero del videojuego
df_output.drop(columns=['genres', 'specs'], inplace= True)

# Filtrar y mostrar los valores únicos en la columna 'price' que contengan letras
unique_prices_with_letters = df_output['price'].loc[df_output['price'].apply(lambda x: bool(re.search('[a-zA-Z]', str(x))))].unique()
print(unique_prices_with_letters)
# Reemplazamos 'Free To Play' por 0 en la columna 'price'
df_output['price'] = df_output['price'].replace('Starting at $499.00', 499)
df_output['price'] = df_output['price'].replace('Starting at $449.00', 449)
# Valores a reemplazar por ceros
values_to_replace = ['Free HITMAN™ Holiday Pack', 'Play Now', 'Third-party','Free To Play', 'Free Mod', 'Install Theme', 
                     'Free to Play', 'Free', 'Free Demo', 'Play for Free!', 'Install Now', 'Play WARMACHINE: Tactics Demo', 
                     'Play the Demo', 'Free to Try', 'Free Movie', 'Free to Use']
# Reemplazar los valores por ceros en la columna 'price'
df_output['price'] = df_output['price'].replace(values_to_replace, 0)
# Convertir la columna 'price' a tipo de dato float
df_output['price'] = df_output['price'].astype(float)
# Transformamos los valores booleanos de la columna early_access en binarios
df_output['early_access'] = df_output['early_access'].astype(int)
# Filtrar y mostrar los valores únicos en 'release_date' que contienen letras (excluyendo nulos)
df_output['release_date'].loc[df_output['release_date'].apply(lambda x: isinstance(x, str) and any(c.isalpha() for c in x))].unique()
""" 
Al revisar la data vemos que tenemos muchos valores en diferentes formatos por lo que se asume que vamos a tomar solo 
el año de lanzamiento del videojuego dado que la consulta developer solo pide el año.
"""
# Filtrar y extraer solo los años numéricos de la columna 'release_date'
df_output['release_year'] = df_output['release_date'].apply(lambda x: re.findall(r'\d{4}', str(x))[0] if isinstance(x, str) and re.match(r'\d{4}', str(x)) else None)
# Eliminamos la columna release_date y nos quedamos con release_year
df_output.drop(columns='release_date', inplace=True)
# Eliminamos las dos finas que tenemos vacías en item_id
df_output = df_output.dropna(subset=['item_id'])
# Exportamos a csv
df_output.to_csv('games.csv', index=False)

"""
El siguiente archivo csv se crea para facilitar la consulta en la API
"""
# Importamos el archivo items recientemente creado
expanded_items_df = pd.read_parquet('items.parquet')
expanded_items_df['item_id'] = expanded_items_df['item_id'].astype(str)
# Filtramos los df para hacer mejor la busqueda 
df_intems_filtered = expanded_items_df[['item_id', 'playtime_forever']]
# Eliminamos columnas que no necesitamos
columas_para_excluir = ['title', 'url', 'price', 'early_access', 'developer', 'release_year']
new_df_games = df_output.drop(columns=columas_para_excluir)
# Hacemos un merge del df games con el df filtrado
merge_df = pd.merge(df_intems_filtered, new_df_games, on= 'item_id', how= 'left')
# Crear un nuevo dataframe para guardar los resultados
result_df = pd.DataFrame(columns=['Total_Playtime'])
# Realizar el ciclo for para calcular sumatoria por columna
for column in merge_df.columns:
    if column != 'playtime_forever':
        total_playtime = merge_df[merge_df[column] == 1]['playtime_forever'].sum()
        result_df.at[column, 'Total_Playtime'] = total_playtime
# Ordenar el dataframe de mayor a menor
result_df_sorted = result_df.sort_values(by='Total_Playtime', ascending=False)
# Agregar una columna de posición numérica
result_df_sorted.insert(0, 'Position', range(1, len(result_df_sorted) + 1))
result_df_sorted.to_csv('genre.csv')