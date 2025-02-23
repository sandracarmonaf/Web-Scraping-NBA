import time
import pandas as pd

from selenium import webdriver
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

def SetUp():
    service = Service(ChromeDriverManager().install())
    option = webdriver.ChromeOptions()

    option.add_argument("--headless")
    #option.add_argument("start-maximized")
    driver = Chrome(service=service, options=option)
    driver.implicitly_wait(10)

    return driver


ConferenciaEste = []
ConferenciaOeste = []
Divisiones = []
Conferencias = []


def Scraping(driver, year):
    driver.get(f"https://www.basketball-reference.com/leagues/NBA_{year}_standings.html")
    time.sleep(2)
    
    #Equipos del Este
    path = f'//table[@id="divs_standings_E"]/tbody/tr[@class="full_table"]/th/a'
    EquiposEste = driver.find_elements(By.XPATH, path)
    print(f"elementos encontrados en Equipos del Este: {len(EquiposEste)}")

    for element in EquiposEste:
        ConferenciaEste.append(element.text)

    #Equipos del Oeste
    path = f'//table[@id="divs_standings_W"]/tbody/tr[@class="full_table"]/th/a'
    EquiposOeste = driver.find_elements(By.XPATH, path)
    print(f"elementos encontrados en Equipos del Oeste: {len(EquiposOeste)}")

    for element in EquiposOeste:
        ConferenciaOeste.append(element.text)

    #Divisiones

    #Parte Este
    path = f'//table[@id="divs_standings_E"]/tbody/tr[@class="thead"]/th/strong'
    DivisionesName = driver.find_elements(By.XPATH, path)
    for element in DivisionesName:
        for i in range(5):
            Divisiones.append(element.text)

    #Parte Oeste
    path = f'//table[@id="divs_standings_W"]/tbody/tr[@class="thead"]/th/strong'
    DivisionesName = driver.find_elements(By.XPATH, path)
    for element in DivisionesName:
        for i in range(5):
            Divisiones.append(element.text)

    print(ConferenciaEste)
    print(ConferenciaOeste)

    path = f'//table[@id="divs_standings_E"]/thead/tr/th'
    ConferenceName = driver.find_element(By.XPATH, path)
    for i in range(15):
        Conferencias.append(ConferenceName.text)

        path = f'//table[@id="divs_standings_W"]/thead/tr/th'
    ConferenceName = driver.find_element(By.XPATH, path)
    for i in range(15):
        Conferencias.append(ConferenceName.text)

    # driver.quit()


    EquiposTotales = ConferenciaEste + ConferenciaOeste

    return EquiposTotales, Divisiones, Conferencias

def CreateCSV(Equipos, Divisiones, Conferencias, year):
    df = pd.DataFrame({
        "Team": Equipos,
        "Division": Divisiones,
        "Conference": Conferencias
    })
    print(df.head(30))

    df.to_csv(f"./team_data/{year}.csv", index=False)


if __name__ == "__main__":
    driver = SetUp()
    print("SetUp Completo")

    for year in range(2020,2022):
        Equipos, Divisones, Conferencias = Scraping(driver, year)
        CreateCSV(Equipos, Divisiones, Conferencias, year)

    driver.quit()




