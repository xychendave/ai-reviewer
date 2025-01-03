import gradio as gr
import pandas as pd
import plotly.express as px
from ashe import get_interval_days, yesterday

from util.db import get_ch_client
from util.conf import get_conf
from util.franchisee import get_org_domain_id_map

conf = get_conf()
org_domain_id_map = get_org_domain_id_map()
clients = {}
for fr_ns in org_domain_id_map.keys():
    client = get_ch_client(f"{fr_ns}_bigbai")
    clients[fr_ns] = client


def fetch_data(days, selected_franchisees):
    data_list = []
    for day in get_interval_days(interval=days, end=yesterday()):
        day_ques_count = 0
        day_shop_count = 0
        day_user_count = 0
        for fr in selected_franchisees:
            chdb = clients[fr]
            res = chdb.execute(f"""
                SELECT count(*), count(distinct shop_id), count(distinct user_id)
                FROM {fr}_bigbai.chat_log_with_question
                WHERE toDate(msg_create_time) = '{day}';
            """)
            data_list.append({
                "franchisee": fr,
                "day": day,
                "ques_count": res[0][0],
                "shop_count": res[0][1],
                "user_count": res[0][2],
            })
            day_ques_count += res[0][0]
            day_shop_count += res[0][1]
            day_user_count += res[0][2]

        if len(selected_franchisees) > 1:
            data_list.append({
                "franchisee": "total",
                "day": day,
                "ques_count": day_ques_count,
                "shop_count": day_shop_count,
                "user_count": day_user_count
            })

    df = pd.DataFrame(data_list)
    print(df.head())
    return df


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

       # 创建 shop count 图表
    fig0 = px.line(
        df,
        x='day',
        y='ques_count',
        color='franchisee',
        title='Question Count per Day by Franchisee',
        markers=True
    )
    fig0.update_layout(
        xaxis_title='Date',
        yaxis_title='Question Count',
        legend_title='Franchisee',
        xaxis=dict(tickformat='%Y-%m-%d'),
        height=400
    )

    # 创建 shop count 图表
    fig1 = px.line(
        df,
        x='day',
        y='shop_count',
        color='franchisee',
        title='Shop Count per Day by Franchisee',
        markers=True
    )
    fig1.update_layout(
        xaxis_title='Date',
        yaxis_title='Shop Count',
        legend_title='Franchisee',
        xaxis=dict(tickformat='%Y-%m-%d'),
        height=400
    )

    # 创建 user count 图表
    fig2 = px.line(
        df,
        x='day',
        y='user_count',
        color='franchisee',
        title='User Count per Day by Franchisee',
        markers=True
    )
    fig2.update_layout(
        xaxis_title='Date',
        yaxis_title='User Count',
        legend_title='Franchisee',
        xaxis=dict(tickformat='%Y-%m-%d'),
        height=400
    )

    return [fig0, fig1, fig2]


def chat_log_with_question_tab():
    with gr.Tab("聊天记录的问题分析"):
        gr.Markdown("# 📊 Chat Log With Question From Clickhouse Statistics")

        plot_output0 = gr.Plot()
        plot_output1 = gr.Plot()
        plot_output2 = gr.Plot()

        with gr.Row():
            days_input = gr.Number(value=conf["franchisee"]["days"], label="查询天数", minimum=1, step=1)
            franchisee_input = gr.Checkboxgroup(
                choices=list(org_domain_id_map.keys()),
                value=list(org_domain_id_map.keys()),
                label="选择加盟商"
            )
            plot_button = gr.Button("刷新数据", variant="primary")

        # 更新点击事件，输出两个图表
        plot_button.click(
            plot_chat_logs,
            inputs=[days_input, franchisee_input],
            outputs=[plot_output0, plot_output1, plot_output2]
        )
