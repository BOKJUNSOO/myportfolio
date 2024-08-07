import requests
import json
import time

headers = {
    "x-nxopen-api-key":"",
    "User-agent": "Mozilla/5.0"
}

# set a target date (without daytime)
target_date = "2024-06"

for i in range(12,31): # set a target daytime
    mydata = [] 
    for j in range(1,151) : # set a target page range
        if j == 20: 
            time.sleep(10)
            url = f"https://open.api.nexon.com/maplestory/v1/ranking/overall?date={target_date}-{i}&world_name=%EC%97%98%EB%A6%AC%EC%8B%9C%EC%9B%80" + "&page=" + str(j)
            req = requests.get(url = url, headers=headers)
            data = req.json()
            mydata.append(data)

        elif j == 40 :
            time.sleep(30)
            url = f"https://open.api.nexon.com/maplestory/v1/ranking/overall?date={target_date}-{i}&world_name=%EC%97%98%EB%A6%AC%EC%8B%9C%EC%9B%80" + "&page=" + str(j)
            req = requests.get(url= url, headers= headers)
            data = req.json()
            mydata.append(data)

        elif j == 60 :
            time.sleep(10)
            url = f"https://open.api.nexon.com/maplestory/v1/ranking/overall?date={target_date}-{i}&world_name=%EC%97%98%EB%A6%AC%EC%8B%9C%EC%9B%80" + "&page=" + str(j)
            req = requests.get(url= url, headers= headers)
            data = req.json()
            mydata.append(data)
        
        elif j == 80 :
            time.sleep(30)
            url = f"https://open.api.nexon.com/maplestory/v1/ranking/overall?date={target_date}-{i}&world_name=%EC%97%98%EB%A6%AC%EC%8B%9C%EC%9B%80" + "&page=" + str(j)
            req = requests.get(url= url, headers= headers)
            data = req.json()
            mydata.append(data)

        elif j == 100 :
            time.sleep(10)
            url = f"https://open.api.nexon.com/maplestory/v1/ranking/overall?date={target_date}-{i}&world_name=%EC%97%98%EB%A6%AC%EC%8B%9C%EC%9B%80" + "&page=" + str(j)
            req = requests.get(url= url, headers= headers)
            data = req.json()
            mydata.append(data)
        
        elif j == 120 :
            time.sleep(10)
            url = f"https://open.api.nexon.com/maplestory/v1/ranking/overall?date={target_date}-{i}&world_name=%EC%97%98%EB%A6%AC%EC%8B%9C%EC%9B%80" + "&page=" + str(j)
            req = requests.get(url= url, headers= headers)
            data = req.json()
            mydata.append(data)

        
        elif j == 140 :
            time.sleep(10)
            url = f"https://open.api.nexon.com/maplestory/v1/ranking/overall?date={target_date}-{i}&world_name=%EC%97%98%EB%A6%AC%EC%8B%9C%EC%9B%80" + "&page=" + str(j)
            req = requests.get(url= url, headers= headers)
            data = req.json()
            mydata.append(data)

        else :
            url = f"https://open.api.nexon.com/maplestory/v1/ranking/overall?date={target_date}-{i}&world_name=%EC%97%98%EB%A6%AC%EC%8B%9C%EC%9B%80" + "&page=" + str(j)
            req = requests.get(url= url, headers= headers)
            data = req.json()
            mydata.append(data)

    # save a daytime data
    file_path = f'C:/Users/brian/Desktop/JUNSOO/Project/Game_domain/data/ranking_{target_date}-{i}.json'
    with open(file_path, "w", encoding= "UTF-8-SIG") as f:
        json.dump(mydata
                  ,f
                  ,ensure_ascii=False
                  ,indent='\t')