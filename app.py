import requests
import pandas as pd
import gspread
import os
import json
from datetime import datetime
from dotenv import load_dotenv


load_dotenv()

URL = os.getenv('URL')
print(URL)
HEADERS = {
    'Content-Type': 'application/json'
}
with open('secret_payload.json', 'r') as file:
    PAYLOAD = json.load(file)

def export_data_from_dremio_to_google_sheets():
    #получаю данные от Dremio
    response = requests.post(URL, headers=HEADERS, json=PAYLOAD)
    if response.status_code == 200:
        data = response.json()
    else:
        print('Error:', response.status_code)
    
    df = pd.DataFrame(data['rows'], columns=[el['name'] for el in data['schema']])
    df_a = df[['ID', 'ID2', 'Title', 'URL', 'Image', 'Price', 'Currency', 'desc_from_cms']].rename(columns={"desc_from_cms": "Description"})
    df_b = df[['ID', 'ID2', 'Title', 'URL', 'Image', 'Price', 'Currency', 'group_desc']].rename(columns={"group_desc": "Description"})
    df_c = df[['ID', 'ID2', 'Title', 'URL', 'Image', 'Price', 'Currency', 'desc_from_gpt']].rename(columns={"desc_from_gpt": "Description"})
    
    with open('secret_cred.json', 'r') as file:
        creds = json.load(file)

    #авторизация как сервис аккаунт
    gc = gspread.service_account_from_dict(creds)
    
    wks = gc.open("feed_for_yandex")
    last_update = wks.worksheet("date_last_update")
    test = wks.worksheet("test")
    feed_a = wks.worksheet("feed_a")
    feed_b = wks.worksheet("feed_b")
    feed_c = wks.worksheet("feed_c")
    
    #очищаю листы перед загрузкой новых данных
    last_update.clear()
    feed_a.clear()
    feed_b.clear()
    feed_c.clear()
    
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    #загружаю новые данные
    last_update.update([[now]], 'A1')
    feed_a.update([df_a.columns.values.tolist()] + df_a.values.tolist())
    feed_b.update([df_b.columns.values.tolist()] + df_b.values.tolist())
    feed_c.update([df_c.columns.values.tolist()] + df_c.values.tolist())


if __name__ == "__main__":
    export_data_from_dremio_to_google_sheets()