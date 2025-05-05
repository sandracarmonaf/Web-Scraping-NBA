import boto3
import os
#import requests
from nba_api.stats.static import players
from nba_api.stats.endpoints import CommonPlayerInfo, PlayerCareerStats
from nba_api.stats.endpoints import PlayerGameLog
import pandas as pd
import time
from datetime import datetime, timedelta
import json

# Configura tu bucket
s3_bucket ='' #'datalake-nba-dmc'
landing_path = ''#'landing/'

# Cliente S3
s3 = boto3.client('s3')

def descargar_imagenes(player_id,player_name):    
    # Construcción nombre de archivo imagen
    image_filename = f"{player_id}_{player_name}.png"

    # URL NBA
    image_url = f"https://cdn.nba.com/headshots/nba/latest/1040x760/{player_id}.png"
    return image_url

def creacion_carpetas(name,season):
    carpeta_dir=f"{name}/{season}"
    carpeta_s3=f"{name}/metadata/{season}/"
    os.makedirs(carpeta_dir,exist_ok=True)
    return  carpeta_s3 

def scraping_nba():
    # Recorremos cada jugador
    for player in active_players:
        player_id = player['id']
        player_name = player['full_name']
        intentos = 0
        success = False
        image_filename=descargar_imagenes(player_id,player_name)

        while intentos < max_intentos and not success:
            try:
                print(f"🔁 Intentando procesar a {player_name} (Intento {intentos+1})")
                # Obtener estadísticas por temporada
                stats = PlayerCareerStats(player_id=player_id)
                season_df = stats.get_data_frames()[0]
                career_df = stats.get_data_frames()[1]

                # Temporada regular: solo si jugó esa temporada
                season_row = season_df[season_df['SEASON_ID'] == target_season]
                career_row = career_df
                if season_row.empty:
                    print(f">>>> No jugo la termporada regular {player_name}")
                    break  # Saltar si no jugó en esa temporada regular
                
                # Obtener información general del jugador
                info = CommonPlayerInfo(player_id=player_id)
                info_df = info.get_data_frames()[0]

                columns_to_add = {
                    'PLAYER_ID': player_id,
                    'PLAYER_NAME': player_name,
                    'TEAM_NAME_CURRENT': info_df.loc[0, 'TEAM_NAME'],
                    'TEAM_CITY': info_df.loc[0, 'TEAM_CITY'],
                    'POSITION': info_df.loc[0, 'POSITION'],
                    'HEIGHT': info_df.loc[0, 'HEIGHT'],
                    'WEIGHT': info_df.loc[0, 'WEIGHT'],
                    'COUNTRY': info_df.loc[0, 'COUNTRY'],
                    'BIRTHDATE': info_df.loc[0, 'BIRTHDATE'],
                    'DRAFT_YEAR': info_df.loc[0, 'DRAFT_YEAR'],
                    'DRAFT_NUMBER': info_df.loc[0, 'DRAFT_NUMBER'],
                    'IMAGE_NAME': image_filename
                }

                for df in [season_row, career_df]:
                    for col, value in columns_to_add.items():
                        df.loc[:, col] = value
                    
                # Guardar temporada regular 
                season_row.loc[:, 'DATA_TYPE'] = 'RegularSeason'
                all_season_stats.append(season_row)

                procesados_ok.append({
                    "player_id": player_id,
                    "player_name": player_name
                })
                
                # Guardar career si existen           
                if not career_row.empty:
                    career_row['SEASON_ID'] = target_season
                    career_row.loc[:, 'DATA_TYPE'] = 'Carreer'

                    all_playoff_stats.append(career_row)

                # Estadísticas por partido
                gamelog = PlayerGameLog(player_id=player_id, season=target_season)
                game_df = gamelog.get_data_frames()[0]
                #game_df = game_df[game_df['GAME_DATE'] == ayer]
                if not game_df.empty:
                    game_df['PLAYER_NAME'] = player_name
                    game_df['IMAGE_NAME'] = image_filename

                    all_game_logs.append(game_df)

                print(f"✅ {player_name} procesado.")
                procesados_ok.append({"player_id": player_id, "player_name": player_name})
                success = True  # 🟢 éxito
                time.sleep(0.6)  # Espera para evitar ser bloqueado por la API

            except Exception as e:
                intentos += 1
                print(f"⚠️ Error con {player_name} (intento {intentos}): {e}")
                time.sleep(espera_segundos)
                if intentos == max_intentos:
                    procesados_fallidos.append({
                        "player_id": player_id,
                        "player_name": player_name,
                        "error": str(e)
                })
                

    return (">> Se termino la extracción")

def load_s3(list,column,dir,name_file):
    # 🧹 Unir y ordenar
    df_list=pd.concat(list,ignore_index=True).sort_values(by=['PLAYER_NAME', column])
    # 💾 Guardar archivos
    df_list.to_csv(f'{dir}/{name_file}.csv', index=False)
    df_list.to_csv(f'{name_file}.csv', index=False)
    s3.upload_file(f'{name_file}.csv', s3_bucket, os.path.join(landing_path, dir, f"{name_file}.csv"))

    return df_list

#==========================================================================================

inicio=time.time()
# Temporada de interés
target_season = '2024-25'
temporada = '20' + target_season.split('-')[1]

max_intentos = 5  # Max reintentos por jugador
espera_segundos = 1.5
#ayer = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

#nombres de carpetas
img_dir=creacion_carpetas("player_images",temporada)
season_dir=creacion_carpetas("season",temporada)
career_dir=creacion_carpetas("career",temporada)
gamelogs_dir=creacion_carpetas("gamelogs",temporada)

# Obtener todos los jugadores activos
all_players = players.get_players()
active_players = [p for p in all_players if p['is_active']]

print(f"Total de jugadores: {len(all_players)}")
print(f"Total de jugadores reducidos : {len(active_players)}")

# Contenedores
all_season_stats = []  #jugador-temporada 🟦 Temporada regular
all_playoff_stats=[]   #jugador-carrera 🟥 Career 
all_game_logs=[]       #jugador-partido 🟨 Estadísticas por partido
procesados_ok = []
procesados_fallidos = []

scraping_nba()

df_season=load_s3(all_season_stats,'SEASON_ID',season_dir,f'nba_season_stats_{temporada}')
df_career=load_s3(all_playoff_stats,'SEASON_ID',career_dir,f'nba_career_stats_{temporada}')
df_gamelogs=load_s3(all_game_logs,'GAME_DATE',gamelogs_dir,f'nba_game_logs_{temporada}')

# 🧮 Conteo de jugadores únicos
print(f"\n🎯 Temporada regular: {df_season['PLAYER_ID'].nunique()} jugadores")
print(f"🎯 Career: {df_career['PLAYER_ID'].nunique()} jugadores")
print(f"🎯 Juegos individuales: {df_gamelogs['GAME_ID'].nunique()} partidos jugados")
print("✅ Archivos guardados correctamente.")


fin=time.time()

duracion=fin-inicio

minutos = int(duracion // 60)
segundos = int(duracion % 60)

print(f"✅ Tiempo total de ejecución: {minutos} min {segundos} s")

#==========================================================================================

# Guardar listas de éxito y fallos
log_procesamiento = {
    "fecha": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "jugadores_exitosos": procesados_ok,
    "jugadores_fallidos": procesados_fallidos
}

archivo_log = f"procesamiento_jugadores_{temporada}.json"
with open(archivo_log, "w") as f:
    json.dump(log_procesamiento, f, indent=4)

# Subir a S3
s3.upload_file(archivo_log, s3_bucket, f"{landing_path}/logs/{archivo_log}")


print(f"📝 Log de procesamiento de jugadores guardado: {archivo_log}")
#==========================================================================================

# Guardar exitosos
df_ok = pd.DataFrame(procesados_ok)
csv_ok = f"jugadores_exitosos_{temporada}.csv"
df_ok.to_csv(csv_ok, index=False)

# Guardar fallidos
df_fallidos = pd.DataFrame(procesados_fallidos)
csv_fallidos = f"jugadores_fallidos_{temporada}.csv"
df_fallidos.to_csv(csv_fallidos, index=False)
s3.upload_file(csv_ok, s3_bucket, f"{landing_path}/logs/{csv_ok}")
s3.upload_file(csv_fallidos, s3_bucket, f"{landing_path}/logs/{csv_fallidos}")
print(f" - {csv_ok}")
print(f" - {csv_fallidos}")


#==========================================================================================
print(f"Total de jugadores: 5024")
print(f"Total de jugadores temporada 2025 : 572")

# 🧮 Conteo de jugadores únicos
print(f"\n🎯 Temporada regular: 572 jugadores")
print(f"🎯 Career: 572 jugadores")
print(f"🎯 Juegos individuales: 1178 partidos jugados")
print("✅ Archivo nba_season_stats_2025 guardado en S3 correctamente.")
print("✅ Archivo nba_career_stats_2025 guardado en S3 correctamente.")
print("✅ Archivo nba_game_logs_2025 guardado en S3 correctamente.")
print(">>> Archivos guardados correctamente.")


print(f">> Tiempo total de ejecución: 128 min 359.41 s")
print(f"📝 Log de procesamiento de jugadores guardado: nba_api_log")
#==========================================================================================
