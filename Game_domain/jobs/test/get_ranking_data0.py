import requests
import json
headers = {
    "x-nxopen-api-key":""
}

target_date = "2024-06"

for i in range(11,31):
    mydata = []
    for j in range(1,151):
        urlString = f"https://open.api.nexon.com/maplestory/v1/ranking/overall?date={target_date}-{i}&world_name=%EC%97%98%EB%A6%AC%EC%8B%9C%EC%9B%80" + "&page=" + str(j)
        req = requests.get(urlString,headers=headers)
        data = req.json()
        mydata.append(data)
        file_path = f'C:/Users/brian/Desktop/JUNSOO/Project/Game_domain/data/ranking_{target_date}-{i}.json'
        with open(file_path, "w", encoding="UTF-8-SIG") as f:
            json.dump(mydata
                      ,f
                      ,ensure_ascii=False
                      ,indent="\n")