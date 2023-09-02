import pandas as pd
import numpy as np

df_items = pd.read_parquet('items.parquet')
df_review = pd.read_csv('reviews.csv')
df_games = pd.read_csv('games.csv')

def user_data(user_id):
    # Filtrar las columnas relevantes en df_games y df_review
    df_games_filtered = df_games[['item_id', 'price']]
    df_review_filtered = df_review[['user_id', 'item_id', 'recommend']]
    # Combinar df_items, df_games_filtered y df_review_filtered
    merged_df = pd.merge(df_items, df_games_filtered, on='item_id', how='left')
    # Combinamos con review filtrado
    merged_df = pd.merge(merged_df, df_review_filtered, on=['user_id', 'item_id'], how='left')
    # Filtrar por user_id
    user_df = merged_df[merged_df['user_id'] == user_id]
    money_spent = user_df['price'].sum() # Calcular la cantidad de dinero gastado por el usuario
    # Calcular el porcentaje de recomendaciones positivas
    total_recommendations = user_df[user_df['recommend'] == 1]['recommend'].count()
    positive_recommendations = user_df[user_df['recommend'] == 1]['recommend'].sum()
    if total_recommendations > 0:
        positive_recommendation_percentage = (positive_recommendations / total_recommendations) * 100
    else:
        positive_recommendation_percentage = 0.0
    # Calcular la cantidad de items únicos
    unique_items_count = user_df['item_id'].nunique()
    # Creando el diccionario resultado
    result_dict = {
        'dinero gastado por el usuario': money_spent,
        'porcentaje de recomendaciones positivas del usuario': positive_recommendation_percentage,
        'Número de items del usuario': unique_items_count
    }
    return result_dict

def count_reviews(start_date, end_date):
    # Filtrar por el rango de fechas
    filtered_reviews = df_review[(df_review['posted_date'] >= start_date) & (df_review['posted_date'] <= end_date)]
    
    # Calcular la cantidad de usuarios únicos que realizaron reviews
    unique_users_count = filtered_reviews['user_id'].nunique()
    
    # Calcular el porcentaje de recomendaciones positivas por usuario
    positive_recommendations = filtered_reviews[filtered_reviews['recommend'] == 1].groupby('user_id')['recommend'].count()
    total_recommendations = filtered_reviews.groupby('user_id')['recommend'].count()
    positive_recommendation_percentages = (positive_recommendations / total_recommendations) * 100
    
    # Crear el diccionario resultado
    result_dict = {'La cantidad de usuario que hicieron recomendaciones en las fechas dadas fueron': unique_users_count,
                    'El porcentaje de recomendación positiva por usuario': positive_recommendation_percentages}
    
    return result_dict

def genre(column_name):
    # Filtramos los df para hacer mejor la busqueda 
    df_intems_filtered = df_items[['item_id', 'playtime_forever']]
    # Eliminamos columnas que no necesitamos
    columas_para_excluir = ['title', 'url', 'price', 'early_access', 'developer', 'release_year']
    new_df_games = df_games.drop(columns=columas_para_excluir)
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
    # Obtener la posición de la columna especificada
    position = result_df_sorted[result_df_sorted.index == column_name]['Position'].values[0]

    return print(f"El genero '{column_name}' está en la posición {position} en el ranking de playtime_forever.")

def userforgenre(column_name):
    # Filtramos los df para hacer mejor la búsqueda
    df_intems_filtered = df_items[['item_id', 'user_id', 'playtime_forever']]
    # Filtramos el df game para manejarlo mejor
    columns_to_exclude = ['title', 'url', 'price', 'early_access', 'developer', 'release_year']
    new_df_games = df_games.drop(columns=columns_to_exclude)
    # Unimos items y reviews con games
    merge_df_items_games = pd.merge(df_intems_filtered, new_df_games, on='item_id', how='left')
    filtered_data = merge_df_items_games[merge_df_items_games[column_name] == 1][['user_id', 'playtime_forever']]
    pivot_table = filtered_data.pivot_table(index='user_id', values='playtime_forever', aggfunc=np.sum)
    # Ordenar el resultado por 'playtime_forever' de mayor a menor
    pivot_df_sorted = pivot_table.sort_values(by='playtime_forever', ascending=False)
    # Obtener el top 5 del ranking
    top_users = pivot_df_sorted.head(5)
    # Convertir el resultado en un nuevo dataframe
    pivot_df_as_dataframe = pd.DataFrame(top_users).reset_index()
    # Agregar una columna con user_url buscado en df_review
    user_url_mapping = df_review.drop_duplicates(subset='user_id').set_index('user_id')['user_url']
    pivot_df_as_dataframe['user_url'] = pivot_df_as_dataframe['user_id'].map(user_url_mapping)
    # Agregar una columna enumerada
    pivot_df_as_dataframe['Rank'] = range(1, len(pivot_df_as_dataframe['user_url']) + 1)
    # Crear un diccionario con los resultados
    result_dict = {}
    for index, row in pivot_df_as_dataframe.iterrows():
        result_dict[row['user_id']] = {'Rank': row['Rank'], 'user_id': row['user_id'], 'user_url': row['user_url']}

    return result_dict

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
        
        # Agregar los resultados al diccionario
        results[int(year)] = {
            'unique_items_count': unique_items_count,
            'free_games_percentage': free_games_percentage
        }
    
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

