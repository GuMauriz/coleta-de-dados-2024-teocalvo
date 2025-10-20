# %%

import time
import json
import datetime

import pandas as pd
import requests

# %%

class Collector:

    def __init__(self, url, instance_name):
        self.url = url
        self.instance_name = instance_name

    def get_response(self, **kwargs):
        resp = requests.get(self.url, params=kwargs)
        return resp
    
    def save_parquet(self, data, filename):
        df = pd.DataFrame(data)
        df.to_parquet(filename, index=False)
    
    def save_json(self, data, filename):
        with open(filename, "w") as open_file:
            json.dump(data, open_file, indent=4)

    def save_data(self, data, format="json"):
        filepath = f"data/{self.instance_name}/{format}/"
        current_time = datetime.datetime.now().strftime("%Y-%m-%d, %H_%M_%S.%f")
        filename = filepath + current_time + f"{format}"
        if format == "json":
            self.save_json(data, filename)
        elif format == "parquet":
            self.save_parquet(data, filename)
        else:
            print("Undefined format.")

    def get_n_save(self, format="json", **kwargs):
        resp = self.get_response(**kwargs)
        if resp.status_code == 200:
            data = resp.json()
            self.save_data(data, format)
        else:
            data = None
            print("Unsucessful request. Error code: " + str(resp.status_code))
        return data
    
    def auto_exec(self, format = "json", date_stop="2000-01-01"):
        page = 1
        while True:
            print(page)
            data = self.get_n_save(format = format, page=page, per_page=1000)
            if data == None:
                print("Error on collecting data. Waiting...")
                time.sleep(60*5)
            else:
                date_last = pd.to_datetime(data[-1]["published_at"]).tz_localize(None)
                if date_last < pd.to_datetime(date_stop):
                    break
                elif len(data) < 1000:
                    break
                page += 1
                time.sleep(5)


# %%

url = "https://api.jovemnerd.com.br/wp-json/jovemnerd/v1/nerdcasts"
collect = Collector(url, "contents")
collect.auto_exec()