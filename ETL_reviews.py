import pandas as pd
import numpy as np
import ast
import json
from datetime import datetime


# Leemos los datos de JSON 
rows = []
with open('australian_user_reviews.json', encoding='utf-8') as f:
    for line in f.readlines():
        rows.append(ast.literal_eval(line))
        
df_reviews = pd.DataFrame(rows)

# Crear una nueva lista para almacenar las filas expandidas
expanded_rows = []
# Iterar sobre cada fila del DataFrame
for index, row in df_reviews.iterrows():
    # Iterar sobre cada diccionario en la lista 'reviews' de la fila actual
    for review in row['reviews']:
        # Crear una nueva fila con valores de 'user_id' y 'user_url' repetidos y valores del diccionario
        new_row = {
            'user_id': row['user_id'],
            'user_url': row['user_url'],
            'item_id': review['item_id'],
            'funny': review['funny'],
            'posted': review['posted'],
            'last_edited': review['last_edited'],
            'helpful': review['helpful'],
            'recommend': review['recommend'],
            'review': review['review']
        }
        expanded_rows.append(new_row)
# Crear un nuevo DataFrame con las filas expandidas
expanded_df = pd.DataFrame(expanded_rows)
"""
Se hace la siguiente consulta: expanded_df[~expanded_df['posted'].str.contains(',')].shape

Tenemos 10119 valores sin el año por lo que no se tomará en cuenta las fechas que no tengan el año

"""
# Se reemplaza el nombre de la columna 'posted' por 'posted_date'
expanded_df.rename(columns={'posted': 'posted_date'}, inplace=True)
# Crear una función para convertir el formato de fecha
def transform_date_format(date_str):
    parts = date_str.split(' ')
    month_mapping = {
        'January': '01', 'February': '02', 'March': '03', 'April': '04',
        'May': '05', 'June': '06', 'July': '07', 'August': '08',
        'September': '09', 'October': '10', 'November': '11', 'December': '12'
    }
    if len(parts) == 4 and parts[0] == 'Posted' and parts[2][-1] == ',':
        year = parts[3][:-1]
        month = month_mapping.get(parts[1])
        day = parts[2][:-1]
        if month and day.isdigit() and year.isdigit():
            return f'{year}-{month}-{day.zfill(2)}'
    return None

# Aplicar la función para transformar el formato de fecha
expanded_df['posted_date'] = expanded_df['posted_date'].apply(transform_date_format)
"""
Los datos sin el año se convierten en datos nulos.
Revisamos que hay en la columa review tenemos otros idiomas en los textos por lo que para este análisis solo se
Tomará en cuenta el idioma Inglés

"""
# Importamos las librerías necesarias para aplicar el análisis de sentimiento
from textblob import TextBlob
from textblob import Word
import nltk
from nltk.corpus import stopwords
nltk.download('wordnet')
# Descargar las stopwords
nltk.download('stopwords')
# Definir una función para normalizar el texto y eliminar stopwords
def preprocess_text(text, language='english'):
    words = TextBlob(text).words
    words = [Word(word).lemmatize() for word in words if word.lower() not in stopwords.words(language)]
    return ' '.join(words)
# Aplicar la función de preprocesamiento al DataFrame
expanded_df['normalized_review'] = expanded_df['review'].apply(preprocess_text)

# Creamos la función de analisis de sentimiento
def sentiment_analysis(text):
    if isinstance(text, str) and len(text) > 0:
        analysis = TextBlob(text)
        sentiment_score = analysis.sentiment.polarity
        if sentiment_score < -0.2:
            return 0  # Malo
        elif sentiment_score > 0.2:
            return 2  # Positivo
        else:
            return 1  # Neutral
    else:
        return 1  # Valor por defecto si no hay reseña

# Aplicar la función de análisis de sentimiento y crear la nueva columna
expanded_df['sentiment_analysis'] = expanded_df['normalized_review'].apply(sentiment_analysis)
# Reemplazar valores vacíos en la columna 'funny' y 'last_edited' por NaN
expanded_df['funny'] = expanded_df['funny'].replace('', pd.NA)
expanded_df['last_edited'] = expanded_df['last_edited'].replace('', pd.NA)
"""
Debemos eliminar las columnas funnny y last_edited dado que tienen mas del 75% como nulos, e igualmente eliminamos las 
columnas review y normalized_review para quedarnos solo con sentiment_analysis.
"""
expanded_df.drop(columns=['funny', 'last_edited', 'review', 'normalized_review'], inplace=True)
# Transformamos los valores booleanos de la columna recommend en binarios
expanded_df['recommend'] = expanded_df['recommend'].astype(int)
# Eliminamos la columna helpful dado que contiene 50% de registros sin evaluación
expanded_df.drop(columns=['helpful'], inplace=True)
# Transformar la columna 'posted_date' a tipo de dato fecha
expanded_df['posted_date'] = pd.to_datetime(expanded_df['posted_date'])
# Exportamo el dataframe tratado a un csv
expanded_df.to_csv('reviews.csv', index=False)
