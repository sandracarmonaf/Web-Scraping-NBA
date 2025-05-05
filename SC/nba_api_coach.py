import boto3
import os
import requests

from nba_api.stats.static import players
from nba_api.stats.static import teams
from nba_api.stats.endpoints import TeamDetails
from nba_api.stats.endpoints import CommonPlayerInfo, PlayerCareerStats
from nba_api.stats.endpoints import PlayerGameLog
import pandas as pd
import time
inicio=time.time()


def creacion_carpetas(name,season):
    carpeta_dir=f"{name}/metadata/{season}"
    carpeta_s3=f"{name}/metadata/{season}/"
    os.makedirs(carpeta_dir,exist_ok=True)
    return  carpeta_s3 #carpeta_dir


# Temporada de interés
target_season = '2024-25'
temporada = '20' + target_season.split('-')[1]

#nombres de carpetas
coach_dir=creacion_carpetas("coach",temporada)


# Obtener todos los jugadores activos

team_list = teams.get_teams()
print(f"Total de equipos: {len(team_list)}")

# Contenedores
coach_data = []


for team in team_list:
    team_id = team['id']
    try:
        
        details = TeamDetails(team_id=team_id)
        coach_info_ = details.get_dict()['resultSets'][0]['headers']
        coach_info = details.get_dict()['resultSets'][0]['rowSet'][0]
        print(coach_info_)
        print(coach_info)
        coach_data.append({
            'nombre': coach_info[9],  # nombre del coach
            'equipo': team['full_name'],
            'abreviatura': team['abbreviation'],
            'temporada': '2024-25',  # puedes parametrizarlo
            'rol': 'Head Coach'
        })

        time.sleep(1)  # evitar bloqueo por rate limiting

    except Exception as e:
        print(f"❌ Error con el equipo {team['full_name']}: {e}")

df = pd.DataFrame(coach_data)
df.to_csv(f'nba_coach_{temporada}.csv', index=False)


fin=time.time()
print("✅ Entrenadores actuales guardados en 'entrenadores_actuales_nba.csv'")
print(fin-inicio)
####################################################################################

