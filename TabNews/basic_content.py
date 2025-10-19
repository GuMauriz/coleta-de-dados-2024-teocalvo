# %%

import datetime
import json
import time

import requests
import pandas as pd

# %%

def get_response(**kwargs):
    url = "https://www.tabnews.com.br/api/v1/contents/"
    resp = requests.get(url, params=kwargs)
    return resp

def save_data(data, option="json"):
    
    now = datetime.datetime.now().strftime("%Y-%m-%d")

    if option == "json":
        with open(f"data/contents/json/{now}.json", "w") as open_file:
            json.dump(data, open_file, indent=4)
    elif option == "parquet":
        df = pd.DataFrame(data)
        df.to_parquet(f"data/contents/parquet/{now}.parquet", index=False)
    else:
        print("Nenhum método de salvamento foi definido.")


# %%

# Loop para capturar dados apenas do dia marcado no sistema
init_page = 1;
stop_date = pd.to_datetime(datetime.datetime.now()).date()
while True:
    print("Página " + str(init_page))
    if resp.status_code == 200:
        resp = get_response(page=init_page, per_page=5, strategy="new")
        data = resp.json()
        save_data(data, option="json")

        if len(data) < 100 or pd.to_datetime(data[-1]["updated_at"]).date() < stop_date:
            break

        init_page += 1
        time.sleep(2)

    else:
        print(resp.status_code)
        time.sleep(60 * 5)
