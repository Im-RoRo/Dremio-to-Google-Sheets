import requests
import pandas as pd
import gspread
import os
import json
import datetime
from dotenv import load_dotenv


load_dotenv()

URL = os.getenv('URL')
HEADERS = {'Content-Type': 'application/json'}

# Путь к расшифрованному файлу
file_path = os.path.expanduser("~/secrets/secret_payload.json")

with open(file_path, 'r') as file:
    PAYLOADS = json.load(file)
    

def export_data_from_dremio_to_google_sheets():
    #получаю данные от Dremio test
    response = requests.post(URL, headers=HEADERS, json=PAYLOADS["test"])
    if response.status_code == 200:
        data = response.json()
    else:
        print('Error:', response.status_code)
    
    COLUMNS_FOR_FEED = ['ID', 'ID2', 'Title', 'URL', 'Image', 'Price', 'Currency']
    
    df = pd.DataFrame(data['rows'], columns=[el['name'] for el in data['schema']])
    df_a = df[COLUMNS_FOR_FEED + ['desc_from_cms']].rename(columns={"desc_from_cms": "Description"})
    df_b = df[COLUMNS_FOR_FEED + ['group_desc']].rename(columns={"group_desc": "Description"})
    df_c = df[COLUMNS_FOR_FEED + ['desc_from_gpt']].rename(columns={"desc_from_gpt": "Description"})
    
    # Путь к расшифрованному файлу
    file_path = os.path.expanduser("~/secrets/secret_cred.json")

    with open(file_path, 'r') as file:
        creds = json.load(file)

    response_best_objects = requests.post(URL, headers=HEADERS, json=PAYLOADS['best_objects'])
    if response_best_objects.status_code == 200:
        data_best_objects = response_best_objects.json()
    else:
        print('Error:', response_best_objects.status_code)
    df_best_objects = pd.DataFrame(data_best_objects['rows'], columns=[el['name'] for el in data_best_objects['schema']])
    df_best_objects = df_best_objects[COLUMNS_FOR_FEED + ['desc_from_gpt']].rename(columns={"desc_from_gpt": "Description"})
    
    
    #авторизация как сервис аккаунт
    gc = gspread.service_account_from_dict(creds)
    
    wks = gc.open("feed_for_yandex")
    last_update = wks.worksheet("date_last_update")
    feed_a = wks.worksheet("feed_a")
    feed_b = wks.worksheet("feed_b")
    feed_c = wks.worksheet("feed_c")
    feed_best_objects = wks.worksheet('feed_best_objects')
    
    #очищаю листы перед загрузкой новых данных
    last_update.clear()
    feed_a.clear()
    feed_b.clear()
    feed_c.clear()
    feed_best_objects.clear()
    
    
    now = (datetime.datetime.now() + datetime.timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")
    #загружаю новые данные
    last_update.update([[now]], 'A1')
    feed_a.update([df_a.columns.values.tolist()] + df_a.values.tolist())
    feed_b.update([df_b.columns.values.tolist()] + df_b.values.tolist())
    feed_c.update([df_c.columns.values.tolist()] + df_c.values.tolist())
    feed_best_objects.update([df_best_objects.columns.values.tolist()] + df_best_objects.values.tolist())


if __name__ == "__main__":
    export_data_from_dremio_to_google_sheets()