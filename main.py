import requests
import os
import shutil
from io import StringIO
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup


years = list(range(2021,2025))

url_teams="https://www.basketball-reference.com/teams/" # info de los equipo, ciudades, etc
url_awards ="https://www.basketball-reference.com/awards/" #De todo
url_start = "https://www.basketball-reference.com/awards/awards_{}.html"
player_stats_url = "https://www.basketball-reference.com/leagues/NBA_{}_per_game.html"

def extraccion(informacion,url,years):

    #Creacion de la carpeta con el nombre de información
    carpeta = os.path.join(os.getcwd(), informacion)
    os.makedirs(carpeta, exist_ok=True)

    for year in years:
        url_year = url.format(year)
        data = requests.get(url_year)

        # Creación de la ruta de archivo 
        file_path = os.path.join(carpeta, f"{year}.html")

        # Guarda el archivo en la carpeta creada
        with open(file_path, "w+", encoding="utf-8") as f:
            f.write(data.text)
            print(f"Escribió {informacion} : {year}")

def lectura(informacion, years, tipo, clase):

    dfs = []

    #Carpeta donde se encuentran los archivos
    carpeta = os.path.join(os.getcwd(), informacion)

    for year in years:

        file_path = os.path.join(carpeta, f"{year}.html")

        with open(file_path.format(year), "r", encoding="utf-8") as f:
            page = f.read()
            print(f"Leyo {informacion}: {year}")

        soup = BeautifulSoup(page, 'html.parser')
        soup.find(tipo, class_=clase).decompose()
        mvp_table = soup.find_all(id="mvp")[0]
        html_io = StringIO(str(mvp_table))  
        mvp_df = pd.read_html(html_io)[0]
        mvp_df["Year"] = year
        dfs.append(mvp_df)

    mvps = pd.concat(dfs)
    mvps.to_csv(f"{informacion}.csv")

##############

extraccion("aws",url_start,years)
lectura("aws", years,'tr',"over_header")
extraccion("player",player_stats_url,years)