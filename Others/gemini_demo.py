import pathlib
import textwrap
import google.generativeai as genai

# # Used to securely store your API key
# from google.colab import userdata
from IPython.display import display
from IPython.display import Markdown

### jupyter notebook 无法获取宿主机的系统环境变量
import os
# Or use `os.getenv('GOOGLE_API_KEY')` to fetch an environment variable.
# GOOGLE_API_KEY=userdata.get('GOOGLE_API_KEY')
GEMINI_API_KEY = ""
genai.configure(api_key=GEMINI_API_KEY, transport="rest")  # 修改这里，加一个 transport="rest"

"""
def to_markdown(text):
  text = text.replace('•', '  *')
  return Markdown(textwrap.indent(text, '> ', predicate=lambda _: True))
"""

"""
for m in genai.list_models():
  if 'generateContent' in m.supported_generation_methods:
    print(m.name)
"""

model = genai.GenerativeModel('gemini-pro')

response = model.generate_content("告诉我你是谁？并做一分钟自我介绍")
print(response.text)
