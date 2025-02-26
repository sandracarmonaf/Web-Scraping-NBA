import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

options = Options()
#options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
time.sleep(2)
driver.maximize_window() 

url_root = 'https://www.basketball-reference.com/'

def search_data(driver):
    id_input = 'header_leagues'
    e_input = driver.find_element(By.ID, id_input)
    e_input.click()
    print(f'==> Se dio click en la 1era ventana.')

if __name__ == "__main__":
    driver.get(url_root)
    time.sleep(2)
    search_data(driver)
    time.sleep(2) 
    driver.quit()