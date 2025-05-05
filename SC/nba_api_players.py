import os
from nba_api.stats.static import players
from nba_api.stats.endpoints import CommonPlayerInfo, PlayerCareerStats
import pandas as pd
import time
from datetime import datetime, timedelta
from nba_api.stats.static import teams


def descargar_imagenes(player_id,player_name):    
    # Construcción nombre de archivo imagen
    image_url = f"https://cdn.nba.com/headshots/nba/latest/1040x760/{player_id}.png"
    return image_url

def team_imagenes(team_name):    
    var=team_name.lower().replace(" ", "-")
    image_url = f"https://loodibee.com/wp-content/uploads/nba-{var}-logo.png"
    return image_url

def scraping_nba(list_player):
    # Recorremos cada jugador
    for player in active_players:
        player_id = player['id']
        player_name = player['full_name']
        image_filename=descargar_imagenes(player_id,player_name)
        list_player.append({
            'id_player': player_id,  # nombre del coach
            'name_player': player_name,
            'image_player': image_filename
        })               

    return (">> Se termino la extracción")


####################################################################################
inicio=time.time()
# Temporada de interés
target_season = '2024-25'
temporada = '20' + target_season.split('-')[1]

max_intentos = 5  # Número máximo de reintentos por jugador
espera_segundos = 1.5
#ayer = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

#nombres de carpetas

# Obtener todos los jugadores activos
all_players = players.get_players()
active_players = [p for p in all_players if p['is_active']] 

nba_teams = teams.get_teams()

nba_teams = teams.get_teams()
df_teams = pd.DataFrame(nba_teams)[['full_name', 'abbreviation']]
df_teams['url_format'] = df_teams['full_name'].str.lower().str.replace(" ", "-", regex=False)
df_teams['team_logo_url'] = "https://loodibee.com/wp-content/uploads/nba-" + df_teams['url_format'] + "-logo.png"
df_teams.to_csv(f'dim_nba_teams_{temporada}.csv', index=False)
print(">>>>Se descargo los teams")

print(f"Total de jugadores: {len(all_players)}")
print(f"Total de jugadores reducidos : {len(active_players)}")

# # Contenedores
players_list = []  #jugador-temporada 🟦 Temporada regular
# procesados_ok = []
# procesados_fallidos = []

scraping_nba(players_list)
df_players = pd.DataFrame(players_list)
df_players.to_csv(f'dim_nba_players_{temporada}.csv', index=False)


# #all_season_stats.to_csv(f'dim_nba_players_{temporada}', index=False)



# fin=time.time()

# duracion=fin-inicio

# minutos = int(duracion // 60)
# segundos = int(duracion % 60)

# print(f"✅ Tiempo total de ejecución: {minutos} min {segundos} s")


# ####################################################################################
# # Guardar exitosos
# df_ok = pd.DataFrame(procesados_ok)
# csv_ok = f"jugadores_exitosos_{temporada}.csv"
# df_ok.to_csv(csv_ok, index=False)

# # Guardar fallidos
# df_fallidos = pd.DataFrame(procesados_fallidos)
# csv_fallidos = f"jugadores_fallidos_{temporada}.csv"
# df_fallidos.to_csv(csv_fallidos, index=False)
# print(f" - {csv_ok}")
# print(f" - {csv_fallidos}")