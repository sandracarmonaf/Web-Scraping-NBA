import requests
import os
import shutil
from io import StringIO
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup
from nba_api.stats.static import players
from nba_api.stats.endpoints import CommonPlayerInfo, PlayerCareerStats
import string
import json
import time
from requests.exceptions import ReadTimeout
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

abecedario = list(string.ascii_lowercase)
#abecedario = [chr(i) for i in range(97, 123)]

url_players ="https://www.basketball-reference.com/players/{}.html"


def extraccion(informacion,url,lista):

    #Creacion de la carpeta con el nombre de información
    carpeta = os.path.join(os.getcwd(), informacion)
    os.makedirs(carpeta, exist_ok=True)

    for l in lista:
        url_l = url.format(l)
        data = requests.get(url_l)

        # Creación de la ruta de archivo 
        file_path = os.path.join(carpeta, f"{l}.html")

        # Guarda el archivo en la carpeta creada
        with open(file_path, "w+", encoding="utf-8") as f:
            f.write(data.text)
            print(f"Escribió {informacion} : {l}")

def lectura(informacion, list, tipo, clase):

    dfs = []

    #Carpeta donde se encuentran los archivos
    carpeta = os.path.join(os.getcwd(), informacion)

    for l in list:

        file_path = os.path.join(carpeta, f"{l}.html")

        with open(file_path.format(l), "r", encoding="utf-8") as f:
            page = f.read()
            print(f"Leyo {informacion}: {l}")

        soup = BeautifulSoup(page, 'html.parser')
        soup.find(tipo, class_=clase).decompose()
        mvp_table = soup.find_all(id="mvp")[0]
        html_io = StringIO(str(mvp_table))  
        mvp_df = pd.read_html(html_io)[0]
        #mvp_df["Year"] = year
        dfs.append(mvp_df)

    mvps = pd.concat(dfs)
    mvps.to_csv(f"{informacion}.csv")

##############

#extraccion("name_player",url_players,abecedario)
#lectura("name_player", abecedario,'tr',"")

#extraccion("player",player_stats_url,years)


# Obtener todos los jugadores de la NBA
#all_players = players.get_players()

# Convertir los datos en un DataFrame de Pandas
#df = pd.DataFrame(all_players)

# Guardar en un archivo CSV
#df.to_csv("nba_players.csv", index=False)

#print("Archivo CSV guardado con éxito.")




# Función para obtener la URL de la imagen del jugador
def get_player_image(player_id):
    return f"https://cdn.nba.com/headshots/nba/latest/1040x760/{player_id}.png"

# Función para calcular la edad a partir de la fecha de nacimiento
def calculate_age(birthdate):
    if not birthdate:
        return None
    birth_year = int(birthdate[:4])
    birth_month = int(birthdate[5:7])
    birth_day = int(birthdate[8:10])
    today = datetime.today()
    return today.year - birth_year - ((today.month, today.day) < (birth_month, birth_day))


# Función para hacer solicitudes con reintentos

def get_data_with_retries(endpoint, player_id, max_retries=1, delay=5):
    retries = 0
    while retries < max_retries:
        try:
            return endpoint(player_id=player_id).get_data_frames()[0]
        except Exception as e:
            print(f"⚠️ Error para {player_id}: {e}. Reintentando ({retries + 1}/{max_retries})...")
            retries += 1
            time.sleep(delay)  # Espera antes de reintentar
    print(f"❌ No se pudo obtener datos para {player_id} tras {max_retries} intentos.")
    return None


# Carpeta para guardar imágenes
image_folder = "nba_player_images"
os.makedirs(image_folder, exist_ok=True)

# Función para descargar una imagen
def download_image(player_id, season, player_name):
    img_url = get_player_image(player_id)
    img_path = os.path.join(image_folder, f"{season}_{player_name.replace(' ', '_')}.png")

    # Evitar descargar si la imagen ya existe
    if not os.path.exists(img_path):
        response = requests.get(img_url, timeout=10)
        if response.status_code == 200:
            with open(img_path, "wb") as img_file:
                img_file.write(response.content)
        else:
            print(f"❌ No se pudo descargar la imagen de {player_name}")

# Obtener todos los jugadores
t_players = players.get_players()
all_players = [p for p in t_players if p["is_active"] or player['id'] > 2023]

# Lista para almacenar los datos de cada jugador
player_data = []

# Lista de tareas para las descargas de imágenes
download_tasks = []

# Obtener datos detallados de cada jugador
for player in all_players:
    player_id = player["id"]
    player_name = player["full_name"]
    
    # Obtener información del jugador con reintentos
    player_info = get_data_with_retries(CommonPlayerInfo, player_id)
    career_stats = get_data_with_retries(PlayerCareerStats, player_id)

    # Si no hay historial de equipos, continuar con el siguiente jugador
    if player_info is None or career_stats is None or career_stats.empty:
        continue
    
    # Extraer datos de altura, peso y fecha de nacimiento
    height = player_info.loc[0, "HEIGHT"]  # Altura en pies-pulgadas (ej. '6-7')
    weight = player_info.loc[0, "WEIGHT"]  # Peso en libras
    birthdate = player_info.loc[0, "BIRTHDATE"]  # Fecha de nacimiento (YYYY-MM-DD)
    age = calculate_age(birthdate)  # Calcular edad

    # Obtener temporadas jugadas y equipos
    seasons_played = career_stats["SEASON_ID"].tolist()
    team_ids = career_stats["TEAM_ID"].tolist()
    team_names = career_stats["TEAM_ABBREVIATION"].tolist()

    # Guardar cada temporada en la lista
    for i, season in enumerate(seasons_played):
        team_id = team_ids[i]
        team_name = team_names[i]

        # Guardar datos del jugador en la lista
        player_data.append({
            "player_id": player_id,
            "name": player_name,
            "height": height,
            "weight": weight,
            "age": age,
            "team_id": team_id,
            "team_abbreviation": team_name,
            "season": season,
            "image_filename": f"{season}_{player_name.replace(' ', '_')}.png"
        })

        # Agregar la tarea de descarga de imagen a la lista
        download_tasks.append((player_id, season, player_name))
        

# Guardar los datos en un archivo JSON (o Parquet)
with open("nba_players.json", "w", encoding="utf-8") as json_file:
    json.dump(player_data, json_file, indent=4)

# Convertir a DataFrame y guardar en formato Parquet
df = pd.DataFrame(player_data)
df.to_parquet("nba_players.parquet", index=False)


def fetch_player_data(player):
    try:
        return get_data_with_retries(CommonPlayerInfo, player["id"])
    except Exception as e:
        print(f"❌ Falló {player['full_name']}: {e}")
        return None

# Ejecutar las descargas en paralelo
with ThreadPoolExecutor(max_workers=10) as executor:
    future_to_player = {executor.submit(fetch_player_data, p): p for p in all_players}

    for future in as_completed(future_to_player, timeout=15):  # Máximo 15s por tarea
        try:
            data = future.result()
        except TimeoutError:
            print(f"⏳ Timeout en {future_to_player[future]['full_name']}")

print("Datos y imágenes guardados con éxito.")