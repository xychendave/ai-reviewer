import gradio as gr

from util.conf import get_conf
from app.chat_log_stat import chat_log_tab
from app.chat_log_with_question_stat import chat_log_with_question_tab

conf = get_conf()

# Gradio 界面
with gr.Blocks() as app:
    chat_log_with_question_tab()
    chat_log_tab()


if __name__ == '__main__':
    s_conf = conf["server"]
    app.launch(auth=s_conf["auth"], server_name=s_conf["host"], server_port=s_conf["port"])
