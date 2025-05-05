import os
#import requests
from nba_api.stats.static import players
from nba_api.stats.endpoints import PlayerGameLog
import pandas as pd
import time

def creacion_carpetas(name,season):
    carpeta_dir=f"{name}/{season}"
    os.makedirs(carpeta_dir,exist_ok=True)
    return  carpeta_dir

def scraping_nba():
    gamelog = PlayerGameLog(season=target_season)
    print(gamelog)
    game_df = gamelog.get_data_frames()[0]
    print(game_df)
    all_game_logs.append(game_df)
    return (">> Se termino la extracción")

def load_s3(list,column,dir,name_file):
    # 🧹 Unir y ordenar
    df_list=pd.concat(list,ignore_index=True)#.sort_values(by=['PLAYER_NAME', column])
    # 💾 Guardar archivos
    df_list.to_csv(f'{dir}/{name_file}.csv', index=False)
    df_list.to_csv(f'{name_file}.csv', index=False)

    return df_list

####################################################################################
inicio=time.time()
# Temporada de interés
target_season = '2024-25'
temporada = '20' + target_season.split('-')[1]

max_intentos = 3  # Número máximo de reintentos por jugador
espera_segundos = 5
#ayer = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

#nombres de carpetas
gamelogs_dir=creacion_carpetas("gamelogs",temporada)

# Obtener todos los jugadores activos
all_players = players.get_players()
active_players = [p for p in all_players if p['is_active']] #id']==2544]# #["2544","202695"]

print(f"Total de jugadores: {len(all_players)}")
print(f"Total de jugadores reducidos : {len(active_players)}")

# Contenedores
all_game_logs=[]       #jugador-partido 🟨 Estadísticas por partido


scraping_nba()

df_gamelogs=load_s3(all_game_logs,'GAME_DATE',gamelogs_dir,f'nba_game_logs_{temporada}')

# 🧮 Conteo de jugadores únicos
print(f"🎯 Juegos individuales: {df_gamelogs['PLAYER_ID'].nunique()} jugadores")
print("✅ Archivos guardados correctamente.")


fin=time.time()

duracion=fin-inicio

minutos = int(duracion // 60)
segundos = int(duracion % 60)

print(f"✅ Tiempo total de ejecución: {minutos} min {segundos} s")

####################################################################################
