import pandas as pd
import requests 
import os
from itertools import product
from Variables import seasontype,permode,statcategory,year,urls


def extraccion(seasontype,permode,statcategory,year,urls):

    for st,pm,sc,y in product(seasontype,permode,statcategory,year):
        #Urls en función a las variables seteadas
        urls.append(f'https://stats.nba.com/stats/leagueLeaders?LeagueID=00&PerMode={pm}&Scope=S&Season={y}&SeasonType={st}&StatCategory={sc}')
        
        #Acorde a la cantidad de Urls la cantidad de veces que realizará la operación de extracción
        max_retries = len(urls)
        i = 0

        while i<max_retries:
           
            try:
                
                r = requests.get(url=urls[i]).json()
                encabezados= r['resultSet']['headers']
                df_col = ['Year']+encabezados
                df = pd.DataFrame(columns=df_col)
                df1_temp = pd.DataFrame(r['resultSet']['rowSet'],columns=encabezados)
                df2_temp = pd.DataFrame({'Year':[y for i in range(len(df1_temp))]})
                df3_temp = pd.concat([df1_temp,df2_temp],axis=1)
                df = pd.concat([df,df3_temp],axis=0)

                #Creación de nuevas carpetas segun el seasontype 
                carpeta = os.path.join(os.getcwd(), seasontype[i])
                os.makedirs(carpeta, exist_ok=True)
                file_path = os.path.join(carpeta, f'NBA_datos_jugadores_{st}_{pm}_{sc}.csv')
                df.to_csv(file_path,index=False)
                

            #Si da error por la llave, cuando no tiene registros la combinación de variables,
            #continuar con la siguiente combinación
            except KeyError:
                i+=1
                continue

            #Otro tipo de errores
            except Exception as e:
                print(f'el error es: {e}')
                i+=1
                continue


            i+=1


    print('Termino de ejecutar')
    
if __name__ == "__main__":
    extraccion(seasontype,permode,statcategory,year,urls)