# get Data
"""매일 갱신되는 랭킹정보를 가져온다."""

import requests
import json
from datetime import datetime

headers = {
    "x-nxopen-api-key": "",
    'User-agent': 'Mozilla/5.0'
}
date = datetime.now()
date = date.strftime("%Y-%m-%d")
mydata = []

for page in range (1,151):
    urlString = f"https://open.api.nexon.com/maplestory/v1/ranking/overall?date={date}&world_name=%EC%97%98%EB%A6%AC%EC%8B%9C%EC%9B%80" + "&page=" + str(page)
    req = requests.get(urlString, headers = headers)
    data = req.json()
    mydata.append(data)

file_path = f'C:/Users/brian/Desktop/JUNSOO/Project/Game_domain/data/ranking_{date}.json'
with open(file_path, "w", encoding= "UTF-8-SIG") as f:
    json.dump(mydata
              ,f
              ,ensure_ascii= False
              ,indent = "\t")