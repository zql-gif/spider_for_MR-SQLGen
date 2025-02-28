
"""
用chatgpt对spider的test数据集作一个简单的baseline测试，不涉及提示工程等
input :
   test.json,存有测试数据的详细信息，包括"db_id"，"query"，"question"等等
           {
            "db_id": "soccer_3",
            "query": "SELECT count(*) FROM club",
            "query_toks": [
            ],
            "query_toks_no_value": [
            ],
            "question": "How many clubs are there?",
            "question_toks": [
            ],
            "sql": {
            }
        }

   test_tables.json, 存有测试集所需的database schema信息
output :
   test_predict_1.0.sql: 存储对input内的数据进行text-to-sql任务转换的结果
"""
import json
import os
import random
from langchain.prompts import ChatPromptTemplate
from langchain.output_parsers import ResponseSchema
from langchain.output_parsers import StructuredOutputParser
from langchain.callbacks import get_openai_callback
from langchain.chains import ConversationChain
from langchain.chat_models import ChatOpenAI
import openai
import tiktoken
from openai import OpenAI
import argparse
from prompt.PromptReprTemplate import NumberSignCOTPrompt


# 获取当前文件的绝对路径
current_file_path = os.path.abspath(__file__)
# 获取当前文件所在目录
current_dir = os.path.dirname(current_file_path)

def load_db_schema_info(gold_tables_file, db_id):
    with open(gold_tables_file, "r", encoding="utf-8") as r:
        contents = json.load(r)
    for content in contents:
        if content["db_id"].lower() == db_id.lower():
            return content
    return ""

def chatgpt_text_to_sql_agent(conversation, model, llm_key, content, gold_tables_file):
    # 1.0版本：raw llm
    # prompt 模板 ：包含question，基本的schema信息和解释
    db_schema = load_db_schema_info(gold_tables_file, content["db_id"])

    description = """
    database schema json contains the following information for each database:
    db_id: database id
    table_names_original: original table names stored in the database.
    table_names: cleaned and normalized table names. We make sure the table names are meaningful. [to be changed]
    column_names_original: original column names stored in the database. Each column looks like: [0, "id"]. 0 is the index of table names in table_names, which is city in this case. "id" is the column name.
    column_names: cleaned and normalized column names. We make sure the column names are meaningful. [to be changed]
    column_types: data type of each column
    foreign_keys: foreign keys in the database. [3, 8] means column indices in the column_names. These two columns are foreign keys of two different tables.
    primary_keys: primary keys in the database. Each number is the index of column_names.
    """

    llm_string = """
    Let's think step by step.You are an expert in sqls.\
    Please generate the corresponding SQLite sql for the following question based on the provided database schema information and schema description, and provide a brief explanation.\
    question : {question}\
    Answer the following information: {format_instructions}\
    database schema description : {description}\
    database schema : {schema}
    """

    prompt_template = ChatPromptTemplate.from_template(llm_string)

    response_schemas = [
        ResponseSchema(type="string", name="sql", description='The sql answer to the question.'),
        ResponseSchema(type="string", name="explanation", description='Explain the basis for the sql answer in less than 100 words.')
    ]
    output_parser = StructuredOutputParser.from_response_schemas(response_schemas)
    format_instructions = output_parser.get_format_instructions()

    prompt_messages = prompt_template.format_messages(
        question=content["question"],
        schema=str(db_schema),
        description=description,
        format_instructions=format_instructions
    )

    # 获取格式化后的 prompt 字符串
    encoding = tiktoken.get_encoding('cl100k_base')
    formatted_prompt = prompt_messages[0].content
    # 计算格式化后的 prompt 的 token 数量
    tokens = encoding.encode(formatted_prompt)
    # 如果 token 数量超过最大限制，则裁剪
    if len(tokens) > 7800:
        # 截断为最大 token 数
        truncated_tokens = tokens[:7800]
        formatted_prompt = encoding.decode(truncated_tokens)
    # 生成最终的 prompt 消息
    prompt_messages[0].content = formatted_prompt
    response = conversation.predict(input=prompt_messages[0].content)
    output_dict = output_parser.parse(response)
    return output_dict


def load_database_table_schema(db_name):
    file_database_temp = os.path.join(current_dir, "..", "spider_data", "database", db_name, "schema.json")
    file_test_database_temp = os.path.join(current_dir, "..", "spider_data", "test_database", db_name, "schema.json")
    if os.path.exists(file_database_temp):
        with open(file_database_temp, "r", encoding="utf-8") as r:
            return json.load(r)
    if os.path.exists(file_test_database_temp):
        with open(file_test_database_temp, "r", encoding="utf-8") as r:
            return json.load(r)


def chatgpt_text_to_sql_agent_DAIL_SQL(conversation, model, llm_key, content, gold_tables_file):
    # 2.0版本：DAIL_SQL的prompt

    # prompt 模板 ：包含question，基本的schema信息和解释
    # 获取对应的database的table schema（json格式文件）
    table_schema = load_database_table_schema(content["db_id"])

    content["tables"] = table_schema

    # 创建 NumberSignCOTPrompt 类的实例
    prompt_generator = NumberSignCOTPrompt()
    formatted_target_prompt = prompt_generator.format_target(content)

    # llm_string = """
    # Answer in the the following information: {format_instructions}\
    # """
    #
    # prompt_template = ChatPromptTemplate.from_template(llm_string)
    #
    # response_schemas = [
    #     ResponseSchema(type="string", name="sql", description='The sql answer to the question.'),
    #     ResponseSchema(type="string", name="explanation", description='Explain the basis for the sql answer in less than 100 words.')
    # ]
    # output_parser = StructuredOutputParser.from_response_schemas(response_schemas)
    # format_instructions = output_parser.get_format_instructions()
    #
    # prompt_messages = prompt_template.format_messages(
    #     question=content["question"],
    #     format_instructions=format_instructions
    # )
    #
    # # 获取格式化后的 prompt 字符串
    # encoding = tiktoken.get_encoding('cl100k_base')
    # formatted_prompt = prompt_messages[0].content
    # # 计算格式化后的 prompt 的 token 数量
    # tokens = encoding.encode(formatted_prompt)
    # # 如果 token 数量超过最大限制，则裁剪
    # if len(tokens) > 7800:
    #     # 截断为最大 token 数
    #     truncated_tokens = tokens[:7800]
    #     formatted_prompt = encoding.decode(truncated_tokens)
    # # 生成最终的 prompt 消息
    # prompt_messages[0].content = formatted_prompt
    # response = conversation.predict(input=prompt_messages[0].content)
    # output_dict = output_parser.parse(response)
    # return output_dict

    response = conversation.predict(input=formatted_target_prompt).replace("\n", " ").replace("```", "").replace("json", "").replace("sql", "").strip()

    return {"sql": response}


def chatgpt_text_to_sql_process(gold_input_json, gold_tables_json, test_cnt, model, exp_id, temperature, llm_key):
    output_dic = os.path.join(current_dir,"..","Output",(model+"_"+str(exp_id)).lower())
    # 检测目录是否存在
    if not os.path.exists(output_dic):
        # 创建目录
        os.makedirs(output_dic, exist_ok=True)
    gold_file_output = os.path.join(output_dic, "gold.txt") # 以行格式存储gold.txt，方便后续evaluate
    detailed_gold_info_file = os.path.join(output_dic, "detailed_gold_info.jsonl") # 加载出处理过的所有gold input case的原始json格式详细信息
    predicted_file = os.path.join(output_dic, "predict.txt")  # 以行格式存储llm 生成的predict.txt，方便后续evaluate
    response_file= os.path.join(output_dic, "predict.jsonl") # 以行格式存储llm生成的predict sql,以DAIL-SQL的思路，这里是没有explanation的

    with open(gold_input_json, "r", encoding="utf-8") as r:
        contents = json.load(r)
    if os.path.exists(predicted_file):
        with open(predicted_file, "r", encoding="utf-8") as r:
            lines = r.readlines()
        finished_cnt = len(lines)
    else:
        finished_cnt = 0

    os.environ["OPENAI_API_KEY"] = llm_key
    chat = ChatOpenAI(temperature=temperature, model=model)
    conversation = ConversationChain(
        llm=chat,
        verbose=False  # 为true的时候是展示langchain实际在做什么
    )

    while finished_cnt < test_cnt and finished_cnt < len(contents):
        print("text-to-sql task: " + str(finished_cnt))
        cost = {}
        with get_openai_callback() as cb:
            # response = chatgpt_text_to_sql_agent(conversation, model, llm_key, contents[finished_cnt], gold_tables_json)
            response = chatgpt_text_to_sql_agent_DAIL_SQL(conversation, model, llm_key, contents[finished_cnt], gold_tables_json)
            cost["Total Tokens"] = cb.total_tokens
            cost["Prompt Tokens"] = cb.prompt_tokens
            cost["Completion Tokens"] = cb.completion_tokens
            cost["Total Cost (USD)"] = cb.total_cost
            response["cost"] = cost
        with open(gold_file_output, "a", encoding="utf-8") as w:
            w.write(contents[finished_cnt]["query"]+"\t"+contents[finished_cnt]["db_id"]+"\n")
        with open(predicted_file, "a", encoding="utf-8") as w:
            w.write(response["sql"]+"\n")
        with open(response_file, "a", encoding="utf-8") as w:
            json.dump(response, w)
            w.write("\n")
        with open(detailed_gold_info_file, "a", encoding="utf-8") as w:
            json.dump(contents[finished_cnt], w)
            w.write("\n")
        finished_cnt += 1


if __name__ == "__main__":
    # os.environ["http_proxy"] = "http://127.0.0.1:10809"
    # os.environ["https_proxy"] = "http://127.0.0.1:10809"
    os.environ["OPENAI_API_KEY"] = ""
    os.environ["OPENAI_API_BASE"] = "https://api.ai-yyds.com/v1"

    parser = argparse.ArgumentParser(description="Run chatgpt_text_to_sql_process function.")

    # 添加命令行参数
    parser.add_argument('--gold_input_json', dest='gold_input_json', type=str, required=True)
    parser.add_argument('--gold_tables_json', dest='gold_tables_json', type=str, required=True)
    parser.add_argument('--test_num', dest='test_num', type=int, required=True)
    parser.add_argument('--model', dest='model', type=str, required=True)
    parser.add_argument('--exp_id', dest='exp_id', type=str, required=True)
    parser.add_argument('--temperature', dest='temperature', type=float, required=True)
    parser.add_argument('--llm_key', dest='llm_key', type=str, required=True)

    # 解析命令行参数
    args = parser.parse_args()

    # 调用函数并传递参数
    chatgpt_text_to_sql_process(
        gold_input_json=args.gold_input_json,
        gold_tables_json=args.gold_tables_json,
        test_cnt=args.test_num,
        model=args.model,
        exp_id=args.exp_id,
        temperature=args.temperature,
        llm_key=args.llm_key
    )


    # gold_input_json = "../spider_data/test.json"
    # gold_tables_json = "../spider_data/test_tables.json"
    # test_cnt = 1000
    # model = "gpt-3.5-turbo"
    # exp_id = "2.0"
    # temperature = 0.0
    # llm_key = os.environ["OPENAI_API_KEY"]
    #
    #
    # chatgpt_text_to_sql_process(
    #     gold_input_json=gold_input_json,
    #     gold_tables_json=gold_tables_json,
    #     test_cnt=test_cnt,
    #     model=model,
    #     exp_id=exp_id,
    #     temperature=temperature,
    #     llm_key=llm_key
    # )


