import json

result_list = []
for i in range(1,4) :
    file_path = f"C:/Users/brian/Desktop/JUNSOO/Project/Game_domain/data/ranking_B_{i}.json"
    with open(file_path, encoding = "UTF-8-SIG") as file:
        result_list.extend(json.load(file))

save_path = f'C:/Users/brian/Desktop/JUNSOO/Project/Game_domain/data/ranking_B.json'
with open(save_path, "w" , encoding = "UTF-8-SIG") as f:
    json.dump(result_list
              ,f
              ,ensure_ascii = False
              ,indent = "\t")