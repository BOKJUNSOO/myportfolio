import requests
import json
import time

headers = {
    "x-nxopen-api-key":"test_d93d63d3d8fc3be70ce088ad5bb258a892599cb01685015ad82bd5ea841c499befe8d04e6d233bd35cf2fabdeb93fb0d",
    "User-agent": "Mozilla/5.0"
}

# set a target date (without daytime)
target_date = "2024-07"

for i in range(13,32): # set a target daytime
    if i % 2 != 0:
        time.sleep(30)
        mydata = []
    else:                
        mydata = []
    for j in range(1,151) : # set a target page range
        if j % 20 == 0: 
            time.sleep(15)
            url = f"https://open.api.nexon.com/maplestory/v1/ranking/overall?date={target_date}-{i}&world_name=%EC%97%98%EB%A6%AC%EC%8B%9C%EC%9B%80" + "&page=" + str(j)
            req = requests.get(url = url, headers=headers)
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