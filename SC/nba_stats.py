import boto3
import os
import requests
import tempfile
import re

# # Configura tu bucket
s3_bucket = ''#'datalake-nba-dmc'
landing_path = ''#'landing'

# # Cliente S3
s3 = boto3.client('s3')

response = s3.list_buckets()

print([bucket['Name'] for bucket in response['Buckets']])

from nba_api.stats.static import players
from nba_api.stats.static import teams
from nba_api.stats.endpoints import TeamDetails
from nba_api.stats.endpoints import CommonPlayerInfo, PlayerCareerStats
from nba_api.stats.endpoints import PlayerGameLog
import pandas as pd
import time
inicio=time.time()

def descargar_imagenes(player_id,player_name):
    # Construcción nombre de archivo imagen
    player_name_clean = re.sub(r'[^A-Za-z0-9_\-]', '_', player_name)
    image_filename = f"{player_id}_{player_name_clean}.png"
    

    # URL NBA
    image_url = f"https://cdn.nba.com/headshots/nba/latest/1040x760/{player_id}.png"
    try:
        # Descargar imagen
        
        with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp_file:
        #open(local_img_path, 'wb') as f:
            #f.write(img_data)
            img_data = requests.get(image_url, stream=True)
            img_data.raise_for_status()
            for chunk in img_data.iter_content(1024):
                tmp_file.write(chunk)

            s3_img_path = tmp_file.name

        # Subir a S3
        s3.upload_file(s3_img_path, s3_bucket, f"{landing_path}/{img_dir}/{image_filename}")
        # Limpiar archivo local
        os.remove(s3_img_path)

    except Exception as e:
        print(f"⚠️ No se pudo procesar imagen de {player_name}: {e}")
        image_filename = None

    return image_filename

def creacion_carpetas(name,season):
    #carpeta_dir=f"nba_{name}_{season}"
    carpeta_s3=f"nba_{name}_{season}/"
    #os.makedirs(carpeta_dir,exist_ok=True)
    return  carpeta_s3#carpeta_dir


# Temporada de interés
target_season = '2024-25'
temporada = '20' + target_season.split('-')[1]

#nombres de carpetas
img_dir=creacion_carpetas("player_images",temporada)
season_dir=creacion_carpetas("season",temporada)
playoffs_dir=creacion_carpetas("playoffs",temporada)
gamelogs_dir=creacion_carpetas("gamelogs",temporada)
coach_dir=creacion_carpetas("coach",temporada)


# Obtener todos los jugadores activos
all_players = players.get_players()
active_players = [p for p in all_players if p['id']==2544]#is_active']]
#["2544","202695"]

print(f"Total de jugadores: {len(all_players)}")
print(f"Total de jugadores reducidos : {len(active_players)}")

team_list = teams.get_teams()
print(f"Total de equipos: {len(team_list)}")

# Contenedores
all_season_stats = []  #jugador-temporada 🟦 Temporada regular
all_playoff_stats=[]   #jugador-temporada 🟥 Playoffs 
all_game_logs=[]       #jugador-partido 🟨 Estadísticas por partido
coach_data = []

# Recorremos cada jugador
for player in active_players:
    player_id = player['id']
    player_name = player['full_name']
    image_filename=descargar_imagenes(player_id,player_name)
    
    
    try:
        # Obtener estadísticas por temporada
        stats = PlayerCareerStats(player_id=player_id, timeout=2)
        season_df = stats.get_data_frames()[0]
        playoffs_df = stats.get_data_frames()[1]

        # Temporada regular: solo si jugó esa temporada
        season_row = season_df[season_df['SEASON_ID'] == target_season]
        playoffs_row = playoffs_df#[playoffs_df['SEASON_ID'] == target_season]
        if season_row.empty:
            continue  # Saltar si no jugó en esa temporada regular
        
        # Obtener información general del jugador
        info = CommonPlayerInfo(player_id=player_id)
        info_df = info.get_data_frames()[0]

        # Agregar info general
        for df in [season_row, playoffs_df]:
            df['PLAYER_ID'] = player_id
            df['PLAYER_NAME'] = player_name
            df['TEAM_NAME_CURRENT'] = info_df.loc[0, 'TEAM_NAME']
            df['TEAM_CITY'] = info_df.loc[0, 'TEAM_CITY']
            df['POSITION'] = info_df.loc[0, 'POSITION']
            df['HEIGHT'] = info_df.loc[0, 'HEIGHT']
            df['WEIGHT'] = info_df.loc[0, 'WEIGHT']
            df['COUNTRY'] = info_df.loc[0, 'COUNTRY']
            df['BIRTHDATE'] = info_df.loc[0, 'BIRTHDATE']
            df['DRAFT_YEAR'] = info_df.loc[0, 'DRAFT_YEAR']
            df['DRAFT_NUMBER'] = info_df.loc[0, 'DRAFT_NUMBER']
            df['IMAGE_NAME'] = image_filename
            
        # Guardar temporada regular 
        season_row['DATA_TYPE'] = 'RegularSeason'

        all_season_stats.append(season_row)
        
        # Guardar playoffs si existen
        
        if not playoffs_row.empty:
            playoffs_row['SEASON_ID'] = target_season
            playoffs_row['DATA_TYPE'] = 'Playoffs'

            all_playoff_stats.append(playoffs_row)

        # Estadísticas por partido
        gamelog = PlayerGameLog(player_id=player_id, season=target_season)
        game_df = gamelog.get_data_frames()[0]
        if not game_df.empty:
            game_df['PLAYER_ID'] = player_id
            game_df['PLAYER_NAME'] = player_name
            game_df['IMAGE_NAME'] = image_filename

            all_game_logs.append(game_df)

        print(f"✅ {player_name} procesado.")

        time.sleep(0.6)  # Espera para evitar ser bloqueado por la API

    except Exception as e:
        print(f"⚠️ Error con {player_name}: {e}")
        continue

# 🧹 Unir y ordenar
if all_season_stats:
    df_season = pd.concat(all_season_stats, ignore_index=True).sort_values(by=['PLAYER_NAME', 'SEASON_ID'])
else:
    print("⚠️ No hay datos de temporada regular disponibles.")
    df_season = pd.DataFrame()

if all_playoff_stats:
    df_playoffs = pd.concat(all_playoff_stats, ignore_index=True).sort_values(by=['PLAYER_NAME', 'SEASON_ID'])
else:
    print("⚠️ No hay datos de temporada playoff disponibles.")
    df_season = pd.DataFrame()

if all_game_logs:
    df_gamelogs = pd.concat(all_game_logs, ignore_index=True).sort_values(by=['PLAYER_NAME', 'GAME_DATE'])
else:
    print("⚠️ No hay datos de las jugadas disponibles.")
    df_season = pd.DataFrame()


# 💾 Guardar archivos
df_season.to_csv(f'{season_dir}nba_season_stats_{temporada}.csv', index=False)
df_playoffs.to_csv(f'{playoffs_dir}nba_playoff_stats_{temporada}.csv', index=False)
df_gamelogs.to_csv(f'{gamelogs_dir}nba_game_logs_{temporada}.csv', index=False)


s3.upload_file(f'{season_dir}nba_season_stats_{temporada}.csv', s3_bucket, f"{landing_path}/{season_dir}nba_season_stats_{temporada}.csv")

s3.upload_file(f'{playoffs_dir}nba_playoffs_stats_{temporada}.csv', s3_bucket, f"{landing_path}/{playoffs_dir}nba_playoffs_stats_{temporada}.csv")

s3.upload_file(f'{season_dir}nba_game_logs_{temporada}.csv', s3_bucket, f"{landing_path}/{gamelogs_dir}nba_sgame_logs_{temporada}.csv")

# 🧮 Conteo de jugadores únicos
print(f"\n🎯 Temporada regular: {df_season['PLAYER_ID'].nunique()} jugadores")
print(f"🎯 Playoffs: {df_playoffs['PLAYER_ID'].nunique()} jugadores")
print(f"🎯 Juegos individuales: {df_gamelogs['PLAYER_ID'].nunique()} jugadores")
print("✅ Archivos guardados correctamente.")


for team in team_list:
    team_id = team['id']
    try:
        details = TeamDetails(team_id=team_id)
        coach_info = details.get_dict()['resultSets'][0]['rowSet'][0]

        coach_data.append({
            'nombre': coach_info[3],  # nombre del coach
            'equipo': team['full_name'],
            'abreviatura': team['abbreviation'],
            'temporada': '2023-24',  # puedes parametrizarlo
            'rol': 'Head Coach'
        })

        time.sleep(1)  # evitar bloqueo por rate limiting

    except Exception as e:
        print(f"❌ Error con el equipo {team['full_name']}: {e}")

df = pd.DataFrame(coach_data)
df.to_csv(f'{coach_dir}nba_coach_{temporada}.csv', index=False)
s3.upload_file(f'{coach_dir}nba_coach_{temporada}.csv', s3_bucket, f"{landing_path}/{coach_dir}nba_coach_{temporada}.csv")


fin=time.time()
print("✅ Entrenadores actuales guardados en 'entrenadores_actuales_nba.csv'")
print(fin-inicio)
####################################################################################

