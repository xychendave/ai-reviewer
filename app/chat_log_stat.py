import gradio as gr
import pandas as pd
import plotly.express as px
from ashe import get_interval_days, yesterday

from util.db import get_mongo_client
from util.conf import get_conf
from util.franchisee import get_org_domain_id_map

conf = get_conf()
client = get_mongo_client()


def fetch_data(days, selected_franchisees):
    data_list = []
    for day in get_interval_days(interval=days, end=yesterday()):
        day_count = 0
        for fr in selected_franchisees:
            db = client[f"{fr}_thands"]
            col = db[f"chat_log_{day}"]
            count = col.estimated_document_count()
            data_list.append({
                "franchisee": fr,
                "day": day,
                "count": count,
            })
            day_count += count

        if len(selected_franchisees) > 1:
            data_list.append({
                "franchisee": "total",
                "day": day,
                "count": day_count
            })

    df = pd.DataFrame(data_list)
    print(df.head())
    return df


# Gradio 可视化函数
def plot_chat_logs(days, selected_franchisees):
    # 确保输入的天数是有效的整数
    days = int(days)
    if days < 1:
        days = conf["franchisee"]["days"]  # 使用默认值

    # 更新 fetch_data 调用
    df = fetch_data(days, selected_franchisees)
    if df.empty:
        return "No data available"

    df['day'] = pd.to_datetime(df['day'])
    fig = px.line(
        df,
        x='day',
        y='count',
        color='franchisee',
        title='Chat Log Counts per Day by Franchisee',
        markers=True
    )
    fig.update_layout(
        xaxis_title='Date',
        yaxis_title='Log Count',
        legend_title='Franchisee',
        xaxis=dict(tickformat='%Y-%m-%d'),
        height=800
    )
    return fig


def chat_log_tab():
    with gr.Tab("聊天记录的分析"):
        gr.Markdown("# 📊 Chat Log From Mongodb Statistics")

        plot_output = gr.Plot()

        org_domain_id_map = get_org_domain_id_map()
        with gr.Row():
            days_input = gr.Number(value=conf["franchisee"]["days"], label="查询天数", minimum=1, step=1)
            franchisee_input = gr.Checkboxgroup(
                choices=list(org_domain_id_map.keys()),
                value=list(org_domain_id_map.keys()),
                label="选择加盟商"
            )
            plot_button = gr.Button("刷新数据", variant="primary")

        # 更新点击事件
        plot_button.click(
            plot_chat_logs,
            inputs=[days_input, franchisee_input],
            outputs=plot_output
        )
