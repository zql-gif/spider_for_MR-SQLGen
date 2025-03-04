import json
import pandas as pd
import os

# 读取 JSONL 文件
input_file = "../Output/glm-4-plus_2.0/merged_info.jsonl"
output_file = "../Output/glm-4-plus_2.0/merged_info.xlsx"

with open(input_file, "r", encoding="utf-8") as r:
    lines = r.readlines()

# **确保目标目录存在**
os.makedirs(os.path.dirname(output_file), exist_ok=True)

# **解析所有数据**
data_list = []
for line in lines:
    json_data = json.loads(line)
    json_data_new = {
        "id": json_data["id"],
        "db_id": json_data["db_id"],
        "tables": json.dumps(json_data["tables"]),
        "question": json_data["question"],
        "query": json_data["query"],
        "predict": json_data["predict"],
        "gold_exec_result": json.dumps(json_data["gold_exec_result"]["result"]),
        "gold_exec_error_message": json.dumps(json_data["gold_exec_result"]["error_message"]),
        "gold_exec_able": json_data["gold_exec_result"]["exec_able"],
        "predict_exec_result": json.dumps(json_data["predict_exec_result"]["result"]),
        "predict_exec_error_message": json.dumps(json_data["predict_exec_result"]["error_message"]),
        "predict_exec_able": json_data["predict_exec_result"]["exec_able"],
        "exec_acc": json_data["exec_acc"]
    }
    data_list.append(json_data_new)

# **转换为 DataFrame（横向存储，键为列名）**
df = pd.DataFrame(data_list)

# **写入 Excel**
if not os.path.exists(output_file):
    # 如果文件不存在，写入表头
    df.to_excel(output_file, index=False, engine="openpyxl")
else:
    # 如果文件已存在，追加数据但不写入表头
    with pd.ExcelWriter(output_file, mode="a", if_sheet_exists="overlay", engine="openpyxl") as writer:
        df.to_excel(writer, index=False, header=False)

print(f"所有数据已成功写入 {output_file}")
