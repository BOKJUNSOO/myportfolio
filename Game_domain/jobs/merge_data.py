import json

target_date = "2024-07" # merge target month
result_list = []
for i in range(13,32):
    file_path = f"C:/Users/brian/Desktop/JUNSOO/Project/Game_domain/data/ranking_{target_date}-{i}.json"
    with open(file_path, encoding = "UTF-8-SIG") as file:
        result_list.extend(json.load(file)) # need json.load
                                            # not append // need extend

save_path = f'C:/Users/brian/Desktop/JUNSOO/Project/Game_domain/data/ranking_A_.json'
with open(save_path, "w", encoding = "UTF-8-SIG") as f:
    json.dump(result_list
              ,f
              ,ensure_ascii=False
              ,indent='\t')