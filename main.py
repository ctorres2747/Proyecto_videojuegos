import fastapi
import pandas as pd
import numpy as np
import pyarrow as pa
from datetime import datetime
import json
from fastapi.responses import JSONResponse
import orjson

class ORJSONResponse(JSONResponse):
    media_type = "application/json"
    def render(self, content):
        return orjson.dumps(content)

df_items = pd.read_parquet('items.parquet')
df_items['item_id'] = df_items['item_id'].astype(str)
df_review = pd.read_csv('reviews.csv')
df_review['item_id'] = df_review['item_id'].astype(str)
df_games = pd.read_csv('games.csv')
df_games['item_id'] = df_games['item_id'].astype(str)
df_genre = pd.read_csv('genre.csv')
df_genre.columns = ['Genero', 'Posición', 'Playtimeforever']
merge_df_items_games = pd.read_parquet('userforgenre.parquet')
merge_df_items_games['item_id'] = merge_df_items_games['item_id'].astype(str)

# Asignar un valor a la variable app
app = fastapi.FastAPI(default_response_class=ORJSONResponse)

# Definir los endpoints de la API
@app.get("/")
def index():
    return {"message": "Bienvenido a la API de Cristhian!"}

@app.get("/userdata/{user_id}")
def user_data_endpoint(user_id: str):
    user_data_r = userdata(user_id)
    return user_data_r

@app.get("/countreviews/{start_date}y{end_date}")
def countreviews_endpoint(start_date: str, end_date: str):
    countreviews_r = countreviews(start_date, end_date)
    return countreviews_r

@app.get("/genre/{genero}")
def genre_endpoint(genero: str):
    genero_r = genre(genero)
    if genero_r:
        return JSONResponse(content=genero_r)
    else:
        return JSONResponse(content={})

@app.get("/userforgenre/{genre}")
def userforgenre_endpoint(genre: str):
    genre_r = userforgenre(genre)
    return genre_r

@app.get("/developer/{nombre}")
def developer_endpoint(nombre: str):
    developer_r = developer(nombre)
    return developer_r

@app.get("/sentiment_analysis/{year_analysys}")
def sentiment_analysis_endpoint(year_analysys: int):
    sentiment_analysis_r = sentiment_analysis(year_analysys)
    return sentiment_analysis_r

def userdata(user):
    # Filtrar las columnas relevantes en df_games
    df_games_filtered = df_games[['item_id', 'price']]
    # Filtramos por el user_id en el dataframe items
    df_money = df_items[df_items.user_id == user]
    # Eliminamos los duplicados
    df_money = df_money['item_id'].drop_duplicates()
    # Filtrar las columnas relevantes en df_games y df_review
    df_money = pd.merge(df_money, df_games_filtered, on='item_id', how='left')
    money_spent = df_money['price'].sum() # Calcular la cantidad de dinero gastado por el usuario

    # Calcular el porcentaje de recomendaciones positivas
    total_recommendations = df_review[df_review.user_id == user]['recommend'].count()
    positive_recommendations = df_review[(df_review.user_id == user) & (df_review.recommend == 1)]['recommend'].sum()
    if total_recommendations > 0:
        positive_recommendation_percentage = (positive_recommendations / total_recommendations) * 100
    else:
        positive_recommendation_percentage = 0.0
    
    # Calcular la cantidad de items únicos
    unique_items_count = df_items[df_items.user_id == user]['item_id'].nunique()
    # Creando el diccionario resultado
    result_dict = {'Dinero gastado por el usuario': float(money_spent),
                    'Porcentaje de recomendaciones positivas del usuario': float(positive_recommendation_percentage),
                    'Número de items del usuario': int(unique_items_count)}
    
    return result_dict

def countreviews(start_date, end_date):
    # Convertimos la cadena en un objeto datetime
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    # Convertimos la columna posted_date a formato datetime
    df_review['posted_date'] = pd.to_datetime(df_review['posted_date'])
    # Filtrar por el rango de fechas
    filtered_reviews = df_review[(df_review['posted_date'] >= start) & (df_review['posted_date'] <= end)]
    
    # Calcular la cantidad de usuarios únicos que realizaron reviews
    unique_users_count = filtered_reviews['user_id'].nunique()
    
    # Calcular el porcentaje de recomendaciones positivas por usuario
    positive_recommendations = filtered_reviews[filtered_reviews['recommend'] == 1].groupby('user_id')['recommend'].count()
    total_recommendations = filtered_reviews.groupby('user_id')['recommend'].count()
    positive_recommendation_percentages = (positive_recommendations / total_recommendations) * 100
    
    # Redondear los valores a dos decimales y convertirlos a cadenas
    positive_recommendation_percentages = (positive_recommendations / total_recommendations) * 100
    positive_recommendation_percentages = positive_recommendation_percentages.round(2).astype(str)
    
    # Crear el diccionario resultado
    result_dict = {'La cantidad de usuario que hicieron recomendaciones en las fechas dadas fueron': unique_users_count,
                    'El porcentaje de recomendación positiva por usuario': positive_recommendation_percentages}
    
    return result_dict

def genre(column_name):
    # Obtener la posición de la columna especificada
    position = df_genre.loc[df_genre.Genero == column_name, 'Posición'].values[0]
    message = f"El género '{column_name}' está en la posición {position} en el ranking de playtime_forever."

    return {"message": message}

def userforgenre(column_name):
    filtered_data = merge_df_items_games[merge_df_items_games[column_name] == 1][['user_id', 'playtime_forever']]
    pivot_table = filtered_data.pivot_table(index='user_id', values='playtime_forever', aggfunc=np.sum)
    # Ordenar el resultado por 'playtime_forever' de mayor a menor
    pivot_df_sorted = pivot_table.sort_values(by='playtime_forever', ascending=False)
    # Obtener el top 5 del ranking
    top_users = pivot_df_sorted.head(5)
    # Convertir el resultado en un nuevo dataframe
    pivot_df_as_dataframe = pd.DataFrame(top_users).reset_index()
    # Filtramos df_reviews
    df_review_filtered = df_review[['user_id', 'user_url']]
    # Hacemos un merge
    pivot_df_as_dataframe = pd.merge(pivot_df_as_dataframe, df_review_filtered, on= 'user_id', how= 'left')
    pivot_df_as_dataframe = pivot_df_as_dataframe.drop_duplicates()
    pivot_df_as_dataframe.reset_index(drop=True, inplace=True)
    # Agregar una columna enumerada
    pivot_df_as_dataframe['Rank'] = range(1, len(pivot_df_as_dataframe['user_url']) + 1)
    # Crear un diccionario con los resultados
    result_dict = {}
    for index, row in pivot_df_as_dataframe.iterrows():
        result_dict[row['user_id']] = {'Rank': row['Rank'], 'user_id': row['user_id'], 'user_url': row['user_url']}
    try:
        # Intenta serializar el diccionario a JSON
        result_json = json.dumps(result_dict)
    except Exception as e:
        # Captura cualquier excepción que ocurra al intentar serializar
        print(f"Error al serializar a JSON: {str(e)}")

    return ORJSONResponse(content=result_dict)

def developer(developer_name):
    df_developer = df_games[['developer', 'item_id', 'price', 'release_year']]
    # Filtrar el dataframe por el desarrollador especificado
    filtered_df = df_developer[df_developer['developer'] == developer_name]
    # Crear un diccionario para almacenar los resultados
    results = {}
    # Iterar sobre cada año único en release_year
    unique_years = filtered_df['release_year'].unique()
    for year in unique_years:
        year_data = filtered_df[filtered_df['release_year'] == year]
        
        # Cantidad de valores únicos de item_id
        unique_items_count = year_data['item_id'].nunique()
        
        # Porcentaje de contenido gratis
        total_games_in_year = len(year_data)
        free_games_count = year_data[year_data['price'] == 0]['item_id'].count()
        free_games_percentage = (free_games_count / total_games_in_year) * 100
        
        # Convertir los valores de numpy.float64 a valores de punto flotante de Python
        unique_items_count = float(unique_items_count)
        free_games_percentage = float(free_games_percentage)
        
        # Agregar los resultados al diccionario como cadenas
        results[str(int(year))] = {'Cantidad de items': unique_items_count,
                                   'Porcentaje de contenido Free por año': free_games_percentage}
    
    return results

def sentiment_analysis(year):
    df_sentiment_analysis = df_review[['user_id', 'posted_date', 'sentiment_analysis']]
    # Crear una copia del DataFrame para evitar problemas de asignación
    df_sentiment_analysis_copy = df_sentiment_analysis.copy()
    # Convertir la columna 'posted_date' a tipo datetime
    df_sentiment_analysis_copy['posted_date'] = pd.to_datetime(df_sentiment_analysis_copy['posted_date'])
    # Filtrar el DataFrame por el año dado
    filtered_df = df_sentiment_analysis_copy[df_sentiment_analysis_copy['posted_date'].dt.year == year]
    # Contar la cantidad de registros de cada categoría de sentiment_analysis
    sentiment_counts = filtered_df['sentiment_analysis'].value_counts()
    # Crear un diccionario con las etiquetas de sentimiento correspondientes
    sentiment_labels = {0: 'Negative', 1: 'Neutral', 2: 'Positive'}
    formatted_counts = {sentiment_labels[key]: value for key, value in sentiment_counts.items()}
    
    return formatted_counts

if __name__ == "__main__":
    app.run(host="localhost", port=8000)