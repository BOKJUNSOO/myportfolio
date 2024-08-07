# get data
import requests
import json
import random
import time

headers = {
    "x-nxopen-api-key": "",
    "User-agent": "Mozilla/5.0"
}

# set a year and month
target_date = "2024-06"

for i in range(11,13):  # set a day
    mydata = []
    if i == 12:
        time.sleep(10)
        for j in range(1,151):  # set a page
            if j == 81:
                time.sleep(random.uniform(10))
            else:
                urlString = f"https://open.api.nexon.com/maplestory/v1/ranking/overall?date={target_date}-{i}&world_name=%EC%97%98%EB%A6%AC%EC%8B%9C%EC%9B%80" + "&page=" + str(j)
                req = requests.get(urlString, headers = headers)
                data = req.json()
                mydata.append(data)
                file_path = f'C:/Users/brian/Desktop/JUNSOO/Project/Game_domain/data/ranking_{target_date}-{i}.json'
                with open(file_path, "w", encoding= "UTF-8-SIG") as f:
                    json.dump(mydata
                              ,f
                              ,ensure_ascii=False
                              ,indent='\t')
        
    else:
        for j in range(1,151):  # set a page
            if j == 81:
                time.sleep(random.uniform(10))
            else:
                urlString = f"https://open.api.nexon.com/maplestory/v1/ranking/overall?date={target_date}-{i}&world_name=%EC%97%98%EB%A6%AC%EC%8B%9C%EC%9B%80" + "&page=" + str(j)
                req = requests.get(urlString, headers = headers)
                data = req.json()
                mydata.append(data)
                file_path = f'C:/Users/brian/Desktop/JUNSOO/Project/Game_domain/data/ranking_{target_date}-{i}.json'
                with open(file_path, "w", encoding= "UTF-8-SIG") as f:
                    json.dump(mydata
                              ,f
                              ,ensure_ascii=False
                              ,indent='\t')
        