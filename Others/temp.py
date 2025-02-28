import json
import os.path

from prompt.PromptReprTemplate import NumberSignCOTPrompt
def load_db_list(file):
    with open(file, "r", encoding="utf-8") as r:
        list = json.load(r)

    # print(len(list))

    db_id_list = []
    for item in list:
        if item["db_id"] not in db_id_list:
            db_id_list.append(item["db_id"])

    # print(len(db_id_list))
    return db_id_list


# train_spider_db_list = load_db_list("spider_data/train_spider.json")
# train_others_db_list = load_db_list("spider_data/train_others.json")
#
# tables_db_list = load_db_list("spider_data/tables.json")  # 106,dou bao han
# test_tables_db_list = load_db_list("spider_data/test_tables.json") # 206
#
# flag = True
# for db in train_spider_db_list + train_others_db_list:
#     if db not in test_tables_db_list:
#         flag = False
#
# print(flag)




# 创建一个示例输入字典
example = {
    "db_id": "db1",
    "question": "What are the names of all users?",
    "tables": [
        {
            "name": "users",
            "schema": ["id:number", "name:text", "email:text"]
        },
        {
            "name": "orders",
            "schema": ["order_id:number", "user_id:number", "amount:number"]
        }
    ]
}

# # 创建 NumberSignCOTPrompt 类的实例
# prompt_generator = NumberSignCOTPrompt()
# # 如果你需要格式化目标，也可以调用 format_target
# formatted_target = prompt_generator.format_target(example)
# print(formatted_target)



# # Please install OpenAI SDK first: `pip3 install openai`
# from openai import OpenAI
# client = OpenAI(api_key="<DeepSeek API Key>", base_url="https://api.deepseek.com")
# response = client.chat.completions.create(
#     model="deepseek-chat",
#     messages=[
#         {"role": "system", "content": "You are a helpful assistant"},
#         {"role": "user", "content": "Hello"},
#     ],
#     stream=False
# )
# print(response.choices[0].message.content)


import shutil
source_sqlite_file = os.path.join("..", "spider_data", "database", "academic", "academic.sqlite")
target_sqlite_file = os .path.join("..", "academic.sqlite")
shutil.copy2(source_sqlite_file, target_sqlite_file)