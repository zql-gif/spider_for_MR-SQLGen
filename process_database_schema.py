import os
import json
from altair.vegalite.v5.display import json_renderer

current_file_path = os.path.abspath(__file__)
current_dir = os.path.dirname(current_file_path)

def process_table_schema(db: dict):
    """
    函数功能：获取table.json中的table schema，改变其格式并存储。主要是将db中的多个表格和其column names对应起来。
    输入：db,即spider的db，dict格式的数据，包含db_id,table_names,column_types等等
    输出：table_schema,list格式的数据，包含若干个表示表名和对应列名的dict，dict中包含"name":和"schema"两个键值
    """
    table_schema_list = []

    table_names = db["table_names_original"]
    column_names = db["column_names_original"]
    column_types = db["column_types"]

    # 遍历column_name,逐个处理列名。根据列名所属的表index将列名和其列类型分配到对应的table_schema中
    for index in range(len(column_names)):
        column_name = column_names[index]
        table_index, name = column_name[0], column_name[1]
        if table_index < 0:
            continue
        if len(table_schema_list) <= table_index:
            table_schema_list.append({})  # 当table_schema_list中不存在相应的table_schema时，添加一个空{}
        if "name" not in table_schema_list[table_index]:
            table_schema_list[table_index]["name"] = table_names[table_index]
        if "schema" not in table_schema_list[table_index]:
            table_schema_list[table_index]["schema"] = [name+":"+column_types[index]]
        else:
            table_schema_list[table_index]["schema"].append(name + ":" + column_types[index])
    return table_schema_list


def load_database_table_schema(db_name):
    file_database_temp = os.path.join(current_dir, "spider_data", "database", db_name, "schema.json")
    file_test_database_temp = os.path.join(current_dir, "spider_data", "test_database", db_name, "schema.json")
    if os.path.exists(file_database_temp):
        with open(file_database_temp, "r", encoding="utf-8") as r:
            return json.load(r)
    if os.path.exists(file_test_database_temp):
        with open(file_test_database_temp, "r", encoding="utf-8") as r:
            return json.load(r)

def process_tables_definition_json(file:str):
    """
    处理table schema json文件中的所有db，提取其table schema
    """
    with open(file, "r", encoding="utf-8") as r:
        contents = json.load(r)
    if not contents:
        return

    database_sub_dic = os.listdir(os.path.join(current_dir, "spider_data", "database"))
    test_database_sub_dic = os.listdir(os.path.join(current_dir, "spider_data", "test_database"))
    for db in contents:
        table_schema_list = process_table_schema(db)
        # 将db schema存储到对应database文件夹的对应子目录下（这里database和test_database两个目录都存）
        db_name = db["db_id"]

        # 检测database中是否存在当前db的文件夹,如果有则存储一份table的schema信息
        if db_name in database_sub_dic:
            temp = os.path.join(current_dir, "spider_data", "database", db_name, "schema.json")
            if not os.path.exists(temp):
                with open(temp, "w", encoding="utf-8") as w:
                    json.dump(table_schema_list, w, indent=4)


        # 检测test_database中是否存在当前db的文件夹,如果有则存储一份table的schema信息
        if db_name in test_database_sub_dic:
            temp = os.path.join(current_dir, "spider_data", "test_database", db_name, "schema.json")
            if not os.path.exists(temp):
                with open(temp, "w", encoding="utf-8") as w:
                    json.dump(table_schema_list, w, indent=4)

    # # 检测所有的database文件夹下的子文件夹内是否已经填充好对应的table schema 的json文件
    # for sub_dic in os.listdir("spider_data/database"):
    #     if not os.path.exists(os.path.join(current_dir, "spider_data", "database", sub_dic, "schema.json")):
    #         print(sub_dic)
    #
    # for sub_dic in os.listdir("spider_data/test_database"):
    #     if not os.path.exists(os.path.join(current_dir, "spider_data", "test_database", sub_dic, "schema.json")):
    #         print(sub_dic)

# process_tables_definition_json(os.path.join("spider_data", "tables.json"))
# process_tables_definition_json(os.path.join("spider_data", "test_tables.json"))

