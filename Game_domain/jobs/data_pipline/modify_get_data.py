import requests
import json
import time
from datetime import datetime 

headers = {
    "x-nxopen-api-key":"my api",
    "User-agent": "Mozilla/5.0"
}

# set a target date (without daytime)
target_date = datetime.now().strftime("%Y-%m")

# download with requests method
for i in range(1,31): # set a target daytime
    if i < 10:
        time.sleep(30)
        url = f"https://open.api.nexon.com/maplestory/v1/ranking/overall?date={target_date}-0{i}&world_name=%EC%97%98%EB%A6%AC%EC%8B%9C%EC%9B%80" + "&page=" + str(j)
        mydata = []
    else:              
        url = f"https://open.api.nexon.com/maplestory/v1/ranking/overall?date={target_date}-{i}&world_name=%EC%97%98%EB%A6%AC%EC%8B%9C%EC%9B%80" + "&page=" + str(j)
        mydata = []
    for j in range(1,151) : # set a target page range
        if j % 20 == 0: 
            time.sleep(15)
            req = requests.get(url = url, headers=headers)
            data = req.json()
            mydata.append(data)

        else :
            req = requests.get(url= url, headers= headers)
            data = req.json()
            mydata.append(data)

# save a data in local
    if i < 10:
        file_path = f'C:/Users/brian/Desktop/JUNSOO/Project/Game_domain/data/ranking_{target_date}-0{i}.json'
        with open(file_path, "w", encoding= "UTF-8-SIG") as f:
            json.dump(mydata
                      ,f
                      ,ensure_ascii=False
                      ,indent='\t')
    else:
        file_path = f'C:/Users/brian/Desktop/JUNSOO/Project/Game_domain/data/ranking_{target_date}-{i}.json'    
        with open(file_path, "w", encoding= "UTF-8-SIG") as f:
            json.dump(mydata
                   ,f
                   ,ensure_ascii=False
                   ,indent='\t')