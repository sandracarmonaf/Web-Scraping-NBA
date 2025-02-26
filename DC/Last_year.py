import time
import pandas as pd

from selenium import webdriver
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def SetUp():
    service = Service(ChromeDriverManager().install())
    option = webdriver.ChromeOptions()

    option.add_argument("--headless")
    option.page_load_strategy = "eager"
    #option.add_argument("start-maximized")
    driver = Chrome(service=service, options=option)
    driver.implicitly_wait(10)
    
    return driver

Equipos = []
Divisiones = []
Conferencias = []
year_clasificacion = []
Name_Tag = []

thead_E = []
full_table_E = []
thead_name_E = []

thead_W = []
full_table_W = []
thead_name_W = []

def Scraping(driver):
    #year y clasificacion
    path_year_clasificacion = f'//div[@id="meta"]/div/h1/span'
    elementos = driver.find_elements(By.XPATH, path_year_clasificacion)
    for element in elementos:
        year_clasificacion.append(element.text)
    #------------------------Validador1------------------------------#
    path_validador = '//*[@id="divs_standings_"]'
    validador = driver.find_elements(By.XPATH, path_validador)
    if validador:
        #------------------------Validador2------------------------------#
        path_validador2 = '//*[@id="divs_standings_"]/tbody/tr[@class="thead"]'
        validador2 = driver.find_elements(By.XPATH, path_validador2)
        if validador2:
        #------------------------Logica------------------------------#
            path_indice_head = '//table[@id="divs_standings_"]/tbody/tr[@class="thead"]'
            indice_head = driver.find_elements(By.XPATH, path_indice_head)
            for atributo in indice_head:
                indice = atributo.get_attribute('data-row')
                thead_E.append(int(indice))
                thead_name_E.append(atributo.text)
            print(thead_E)
            print(thead_name_E)

            #Indices de los equipos
            path_indice_team = '//table[@id="divs_standings_"]/tbody/tr[@class="full_table"]'
            indice_team = driver.find_elements(By.XPATH, path_indice_team)
            #-------------------------Equipos-----------------------------------
            path_team_name = '//table[@id="divs_standings_"]/tbody/tr[@class="full_table"]/th/a'
            team_name = driver.find_elements(By.XPATH, path_team_name)
            for x in team_name:
                Equipos.append(x.text)
                href = x.get_attribute('href')
                Name_Tag.append(href[43:46])
            #--------------------------------------------------------------------
            for atributo in indice_team:
                indice = atributo.get_attribute('data-row')
                full_table_E.append(int(indice))
            print(full_table_E)
        
            for index, valor in enumerate(full_table_E):
                x = 0
                if thead_E[x] < full_table_E[index] and full_table_E[index] < thead_E[x+1]:
                    Divisiones.append(thead_name_E[x])

                elif thead_E[x+1] < full_table_E[index] and full_table_E[index] < thead_E[-1]:
                    Divisiones.append(thead_name_E[x+1]) 

                else:
                    Divisiones.append(thead_name_E[-1])
            return Equipos, Divisiones, Conferencias, year_clasificacion, Name_Tag
        else:
            path_team_name = '//table[@id="divs_standings_"]/tbody/tr[@class="full_table"]/th/a'
            team_name = driver.find_elements(By.XPATH, path_team_name)
            for x in team_name:
                Equipos.append(x.text)
                href = x.get_attribute('href')
                Name_Tag.append(href[43:46])
            return Equipos, Divisiones, Conferencias, year_clasificacion, Name_Tag

    else:
    #------------------------Logica------------------------------#

        #Indices de los encabezados
        path_indice_head = '//table[@id="divs_standings_E"]/tbody/tr[@class="thead"]'
        indice_head = driver.find_elements(By.XPATH, path_indice_head)
        for atributo in indice_head:
            indice = atributo.get_attribute('data-row')
            thead_E.append(int(indice))
            thead_name_E.append(atributo.text)
        print(thead_E)
        print(thead_name_E)

        #Indices de los equipos
        path_indice_team = '//table[@id="divs_standings_E"]/tbody/tr[@class="full_table"]'
        indice_team = driver.find_elements(By.XPATH, path_indice_team)
        #--------------------------------------------------------------------
        path_conference_E = f'//table[@id="divs_standings_E"]/thead/tr/th'
        ConferenceName_E = driver.find_element(By.XPATH, path_conference_E)
        #--------------------------------------------------------------------
        path_team_name = '//table[@id="divs_standings_E"]/tbody/tr[@class="full_table"]/th/a'
        team_name = driver.find_elements(By.XPATH, path_team_name)
        for x in team_name:
            Equipos.append(x.text)
            href = x.get_attribute('href')
            Name_Tag.append(href[43:46])
        #--------------------------------------------------------------------
        for atributo in indice_team:
            indice = atributo.get_attribute('data-row')
            full_table_E.append(int(indice))
        print(full_table_E)
        
        for index, valor in enumerate(full_table_E):
            x = 0
            if thead_E[x] < full_table_E[index] and full_table_E[index] < thead_E[x+1]:
                Divisiones.append(thead_name_E[x])

            elif thead_E[x+1] < full_table_E[index] and full_table_E[index] < thead_E[-1]:
                Divisiones.append(thead_name_E[x+1]) 

            else:
                Divisiones.append(thead_name_E[-1])
        
            Conferencias.append(ConferenceName_E.text)
        
        #---------------------------------------------------------------------------------------------------------------
        path_indice_head = '//table[@id="divs_standings_W"]/tbody/tr[@class="thead"]'

        indice_head = driver.find_elements(By.XPATH, path_indice_head)
        for atributo in indice_head:
            indice = atributo.get_attribute('data-row')
            thead_W.append(int(indice))
            thead_name_W.append(atributo.text)
        print(thead_W)
        print(thead_name_W)

        #Indices de los equipos
        path_indice_team = '//table[@id="divs_standings_W"]/tbody/tr[@class="full_table"]'
        #Conferencia
        indice_team = driver.find_elements(By.XPATH, path_indice_team)
        #--------------------------------------------------------------------
        path_conference_W = '//table[@id="divs_standings_W"]/thead/tr/th'
        ConferenceName_W = driver.find_element(By.XPATH, path_conference_W)
        #--------------------------------------------------------------------
        path_team_name = '//table[@id="divs_standings_W"]/tbody/tr[@class="full_table"]/th/a'
        team_name = driver.find_elements(By.XPATH, path_team_name)
        for x in team_name:
            Equipos.append(x.text)
            href = x.get_attribute('href')
            Name_Tag.append(href[43:46])
        #---------------------------------------------------------------------
        for atributo in indice_team:
            indice = atributo.get_attribute('data-row')
            full_table_W.append(int(indice))

        print(full_table_W)

        for index, valor in enumerate(full_table_W):
            x = 0
            if thead_W[x] < full_table_W[index] and full_table_W[index] < thead_W[x+1]:
                Divisiones.append(thead_name_W[x])

            elif thead_W[x+1] < full_table_W[index] and full_table_W[index] < thead_W[-1]:
                Divisiones.append(thead_name_W[x+1]) 

            else:
                Divisiones.append(thead_name_W[-1])
        
            Conferencias.append(ConferenceName_W.text)

        return Equipos, Divisiones, Conferencias, year_clasificacion, Name_Tag

def CreateCSV(Equipos, Divisiones, Conferencias, Tag):
    if Conferencias:
        df = pd.DataFrame({
            "Team": Equipos,
            "NameTag": Tag,
            "Division": Divisiones,
            "Conference": Conferencias,
        })
    else:
        if Divisiones:
            df = pd.DataFrame({
                "Team": Equipos,
                "NameTag": Tag,
                "Division": Divisiones
            })
        else:
            df = pd.DataFrame({
                "Team": Equipos,
                "NameTag": Tag
            })

    df["Year"] = yearAndClassification[0][0:2] + yearAndClassification[0][-2:]
    df["Classification"] = yearAndClassification[1]
    print(df.head(len(Equipos)))

    df.to_csv(f"{yearAndClassification[0]}_{yearAndClassification[1]}.csv", index=False)
    print(f"archivo guardado en un CSV como {yearAndClassification[0]}_{yearAndClassification[1]}.csv")

if __name__ == "__main__":

    #Configuracion del navegador
    driver = SetUp()
    print("SetUp Completo")

    
    #Accediendo a la pagina inicial para el scraping
    driver.get('https://www.basketball-reference.com/leagues/')


    #--------------------------------------------------------------------------------------------------------------
    link = driver.find_element(By.XPATH, '//tr[@class="thead"]/following-sibling::tr/th[@scope="row"]/a[1]')
    url = link.get_attribute("href")  # Obtiene la URL del enlace
    
    print(f"Abriendo: {url}")

    # Abrir el enlace en una nueva pestaña
    driver.execute_script(f"window.open('{url}', '_blank');")
    time.sleep(2)  # Esperar un poco para que la página cargue

    # Cambiar a la nueva pestaña
    driver.switch_to.window(driver.window_handles[-1])
    time.sleep(2)
    e_input = driver.find_element(By.XPATH, '//ul[@class="hoversmooth"]/li[2]')
    e_input.click()
    time.sleep(2)  # Esperar un poco más para evitar errores de carga

    EquiposTotales, DivisonesTotales, ConferenciasTotales, yearAndClassification, tag = Scraping(driver)
    CreateCSV(EquiposTotales, DivisonesTotales, ConferenciasTotales, tag)
        

    driver.close()
    time.sleep(1)
    driver.switch_to.window(driver.window_handles[0])

    thead_E = []
    full_table_E = []
    thead_name_E = []

    thead_W = []
    thead_name_W = []
    full_table_W = []

    Equipos = []
    Divisiones = []
    Conferencias = []
    year_clasificacion = []
    Name_Tag = []


    #--------------------------------------------------------------------------------------------------------------
    driver.quit()