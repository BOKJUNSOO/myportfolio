import requests
import json
headers = {
    "x-nxopen-api-key":"test_d93d63d3d8fc3be70ce088ad5bb258a89c36d83c201c17f80186f66e7c2946ccefe8d04e6d233bd35cf2fabdeb93fb0d"
  }

urlString = "https://open.api.nexon.com/maplestory/v1/history/potential?count=1000&date=2024-05-05"
response = requests.get(urlString, headers = headers)
myapi = response.json()
file_path = 'C:/Users/brian/Desktop/JUNSOO/NexonApi/data/myapi.json'
# save
with open(file_path,'w', encoding = "UTF-8-sig") as f:
    json.dump(myapi,
              f,
              ensure_ascii=False,
              indent = '\t')