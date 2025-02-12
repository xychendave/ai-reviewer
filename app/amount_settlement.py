from datetime import datetime
from pathlib import Path

import gradio as gr
from gradio_calendar import Calendar
import pandas as pd
from ashe import get_month_days, today

from util.db import get_mongo_client, get_ch_client
from util.conf import get_conf
from util.franchisee import get_user_info, get_user_department, get_online_time, get_org_domain_id_map

conf = get_conf()

DEFAULT_RECEPTION_RATIO = 4
PLATFORM_RECEPTION_RATIO = {
    "淘系": 4,
    "抖音": 5,
    "拼多多": 3.5
}
PRICE = 0.01

# 获取情景回复数据
def get_df_scene(franchisee, start_date, end_date):
    mongo = get_mongo_client()
    data_dist_result = mongo[f'{franchisee}_thands']['chat_data_distribution'].aggregate([
        {
            '$match': {
                'date': {
                    '$gte': start_date,
                    '$lte': end_date
                },
                'source': 'GroupReplyByService',
                'intent_data_type': {
                    '$in': [
                        9998, 9997, 9996, 9995, 9994, 9993, 9992, 9991
                    ]
                }
            }
        }, {
            '$group': {
                '_id': [
                    '$user_id', '$platform'
                ],
                'n': {
                    '$sum': '$total'
                }
            }
        }
    ])
    results = []
    for data in data_dist_result:
        results.append({
            "用户ID": int(data["_id"][0]),
            "平台": data["_id"][1],
            "情景回复数": data["n"]
        })
    mongo.close()
    df_scene = pd.DataFrame(results)

    # 合并平台中淘宝和天猫，为淘系，情景回复数相加
    df_scene['平台'] = df_scene['平台'].replace(['淘宝', '天猫'], '淘系')
    df_scene = df_scene.groupby(['平台', '用户ID'], as_index=False).agg({'情景回复数': 'sum'})
    return df_scene

def get_df_stat(franchisee, start_date, end_date):
    ch = get_ch_client(f"{franchisee}_service_statistic")
    sql_stat = f"""
    select platform, user_id, sum(num_reception), sum(num_question), sum(num_question_to_answer), sum(num_question_identify), sum(num_question_answered)
    from {franchisee}_service_statistic.statistic_service_user
    where statistic_date >= '{start_date}' and statistic_date <= '{end_date}'
    group by platform, user_id;
    """
    df = pd.DataFrame(ch.execute(sql_stat), columns=['平台', '用户ID', '总接待人次', '总问题数', '机器人应回复问题数',
                                                '机器人识别问题数', '机器人回复问题数'])
    df["净识别率"] = round(df["机器人识别问题数"] / df["总问题数"], 4)
    df["净回复率"] = round(df["机器人回复问题数"] / df["总问题数"], 4)
    df["配置率"] = round(df["净回复率"] / df["净识别率"], 4)

    user_map = get_user_info(franchisee)
    user_id_to_department = get_user_department(franchisee)
    df["客服姓名"] = df["用户ID"].apply(lambda x: user_map.get(x, ['UNKNOWN'] * 3)[1])
    df["手机号"] = df["用户ID"].apply(lambda x: user_map.get(x, ['UNKNOWN'] * 3)[2])
    df["客服部门"] = df["用户ID"].apply(lambda x: user_id_to_department.get(x, "UNKNOWN"))
    print(f"查询结果数量: {len(df)}")
    return df

def get_df_shop_count(franchisee, start_date, end_date):
    ch = get_ch_client(f"{franchisee}_service_statistic")
    sql_shop_count = f"""
    select shop_platform, user_id, shop_id
    from {franchisee}_service_statistic.statistic_service_user_shop
    where statistic_date >= '{start_date}' and statistic_date <= '{end_date}'
    """
    df_shop_count = pd.DataFrame(ch.execute(sql_shop_count), columns=['平台', '用户ID', '店铺ID'])
    df_shop_count["平台"] = df_shop_count["平台"].apply(lambda x: "淘系" if x in ["淘宝", "天猫"] else x)

    # 统计每个平台和用户钉钉ID的不重复 shop_id 个数
    df_shop_count_uni = df_shop_count.groupby(['平台', '用户ID'])['店铺ID'].nunique().reset_index()
    df_shop_count_uni.columns = ['平台', '用户ID', '店铺数量']
    return df_shop_count_uni

def get_settlement(franchisee, start_date, end_date, discount_input, use_cache="是"):
    print(franchisee, start_date, end_date, discount_input)
    if type(start_date) == datetime:
        start_date = start_date.date().isoformat()
    if type(end_date) == datetime:
        end_date = end_date.date().isoformat()

    if franchisee == "total":
        org_domain_id_map = get_org_domain_id_map()
        fr_list = list(org_domain_id_map.keys())
    else:
        fr_list = [franchisee]

    output_file = Path(f'./data/{franchisee}_{discount_input}折_{start_date}至{end_date}_结算金额表.xlsx')

    # 检查文件是否存在且是否使用缓存
    if output_file.exists() and use_cache == "是":
        print(f"文件已存在，直接读取: {output_file}")
        df_total = pd.read_excel(output_file, sheet_name='汇总结算')
        return df_total, str(output_file)
    
    if franchisee == "total" and (not output_file.exists() or use_cache == "否"):
        gr.Warning("全部加盟商数据一起计算，请耐心等待...")

    fr_total_sum = []
    for franchisee in fr_list:
        print(franchisee)
        df_scene_data = get_df_scene(franchisee, start_date, end_date)
        df_stat_data = get_df_stat(franchisee, start_date, end_date)
        df_shop_count_unique = get_df_shop_count(franchisee, start_date, end_date)

        # 拼接 df，按照平台和用户ID匹配
        df_with_shop_count = pd.merge(df_stat_data, df_shop_count_unique, how='left', on=['平台', '用户ID']).fillna(0)
        df_with_shop_count_and_scene = pd.merge(df_with_shop_count, df_scene_data, how='left', on=['平台', '用户ID']).fillna(0)

        df_with_shop_count_and_scene["机器人接待人次"] = df_with_shop_count_and_scene.apply(
            lambda row: round((row["机器人回复问题数"] + (row["情景回复数"] / 2)) / PLATFORM_RECEPTION_RATIO.get(row["平台"], DEFAULT_RECEPTION_RATIO), 2), axis=1)
        df_with_shop_count_and_scene["预计人力成本（按照流量计算）"] = round(df_with_shop_count_and_scene["机器人接待人次"] * 0.134, 2)
        df_with_shop_count_and_scene["总回复数"] = df_with_shop_count_and_scene["机器人回复问题数"] + df_with_shop_count_and_scene["情景回复数"]
        df_with_shop_count_and_scene["流量结算金额"] = df_with_shop_count_and_scene["总回复数"] * PRICE
        df_with_shop_count_and_scene["折扣"] = f"{discount_input}折"
        df_with_shop_count_and_scene["流量结算金额（折扣）"] = df_with_shop_count_and_scene["总回复数"] * PRICE * discount_input * 0.1

        df_merge = df_with_shop_count_and_scene.groupby(["用户ID", "客服姓名", "手机号", "客服部门"]).sum(
            ["总接待人次", "总问题数", "机器人应回复问题数", "机器人识别问题数", "机器人回复问题数", "总回复数"])
        online_hours = get_online_time(franchisee, start_date, end_date)
        df_merge["总在线时长"] = [online_hours.get(x, 0) for x in df_merge.index.get_level_values("用户ID").values]
        df_merge["预计坐席基数"] = df_merge["总在线时长"].apply(lambda x: max(0, x - (30 * 2)) // (30 * 8) + 1)
        df_merge["预计结算金额"] = df_merge.apply(lambda x: 180 * x["预计坐席基数"], axis=1)
        df_merge["坐席基数（新人优惠活动）"] = df_merge.apply(lambda x: 0 if x["预计人力成本（按照流量计算）"] < 180 else x["预计坐席基数"], axis=1)
        df_merge["实际结算金额（新人优惠活动）"] = df_merge.apply(lambda x: (180 if x["预计人力成本（按照流量计算）"] >= 180 else 0) * x["坐席基数（新人优惠活动）"], axis=1)
        df_merge["净识别率"] = round(df_merge["机器人识别问题数"] / df_merge["总问题数"], 4)
        df_merge["净回复率"] = round(df_merge["机器人回复问题数"] / df_merge["总问题数"], 4)
        df_merge["配置率"] = round(df_merge["净回复率"] / df_merge["净识别率"], 4)
        df_merge["结算率"] = df_merge.apply(lambda x: round(x["实际结算金额（新人优惠活动）"] * 100 / x["预计人力成本（按照流量计算）"], 2), axis=1)

        # 将索引转化为正常的列
        df_merge_new = df_merge.reset_index()
        df_merge_new = df_merge_new[df_merge_new["客服部门"].apply(lambda x: x.split("/")[-1] != "系统部门")]
        df_merge_new = df_merge_new.drop(columns=["用户ID"])
        df_merge_new["流量结算金额"] = df_merge_new["总回复数"] * PRICE
        df_merge_new["折扣"] = f"{discount_input}折"
        df_merge_new["流量结算金额（折扣）"] = df_merge_new["总回复数"] * PRICE * discount_input * 0.1

        df_merge_new = df_merge_new[[
            '客服姓名', '手机号', '客服部门', '总接待人次', '总问题数', '机器人应回复问题数', '机器人识别问题数', '机器人回复问题数',
            '净识别率', '净回复率', '配置率', '店铺数量', '情景回复数', '机器人接待人次', '预计人力成本（按照流量计算）', '总在线时长',
            '预计坐席基数', '预计结算金额', '坐席基数（新人优惠活动）', '实际结算金额（新人优惠活动）', '结算率', '总回复数',
            '流量结算金额', '折扣', '流量结算金额（折扣）'
        ]]
        # 汇总结算
        total_sum = [{
            "公司名称": franchisee,
            "业务结算区间": f"{start_date}至{end_date}",
            "实际结算时间": today(),
            "结算方式": "按实际坐席数量（新人优惠活动）",
            "坐席单价": 180,
            "预计坐席数量": df_merge_new["预计坐席基数"].sum(),
            "实际坐席数量（新人优惠活动）": df_merge_new["坐席基数（新人优惠活动）"].sum(),
            "预计人力成本（按照流量计算）": int(df_merge_new["预计人力成本（按照流量计算）"].sum()),
            "预计结算金额": int(df_merge_new["预计结算金额"].sum()),
            "实际结算金额（新人优惠活动）": int(df_merge_new["实际结算金额（新人优惠活动）"].sum()),
            "预计节省人力总成本": int(int(df_merge_new["预计人力成本（按照流量计算）"].sum()) - int(df_merge_new["实际结算金额（新人优惠活动）"].sum())),
            "新人优惠活动减免成本": int(int(df_merge_new["预计结算金额"].sum()) - int(df_merge_new["实际结算金额（新人优惠活动）"].sum())),
            "结算率": str(round(int(df_merge_new["实际结算金额（新人优惠活动）"].sum()) * 100 / int(df_merge_new["预计人力成本（按照流量计算）"].sum()), 2)) + "%",
            "总回复数": df_merge_new["总回复数"].sum(),
            "流量结算金额": int(df_merge_new["总回复数"].sum() * PRICE),
            "折扣": f"{discount_input}折",
            "流量结算金额（折扣）": int(df_merge_new["总回复数"].sum() * PRICE * discount_input * 0.1)
        }]

        if len(fr_list) == 1:
            df_total = pd.DataFrame(total_sum)

            # 按平台拆分数据
            df_with_shop_count_and_scene = df_with_shop_count_and_scene[df_with_shop_count_and_scene["客服部门"].apply(
                lambda x: x.split("/")[-1] != "系统部门")]
            df_with_shop_count_and_scene = df_with_shop_count_and_scene.drop(columns=["用户ID"])
            df_taobao = df_with_shop_count_and_scene[df_with_shop_count_and_scene['平台'] == '淘系']
            df_douyin = df_with_shop_count_and_scene[df_with_shop_count_and_scene['平台'] == '抖音']
            df_pinduoduo = df_with_shop_count_and_scene[df_with_shop_count_and_scene['平台'] == '拼多多']
            df_xiaohongshu = df_with_shop_count_and_scene[df_with_shop_count_and_scene['平台'] == '小红书']

            # 导出表格为Excel文件，每个平台一个sheet
            with pd.ExcelWriter(output_file) as writer:
                df_total.to_excel(writer, sheet_name='汇总结算', index=False)
                df_merge_new.to_excel(writer, sheet_name='总表', index=False)
                df_taobao.to_excel(writer, sheet_name='淘系', index=False)
                df_douyin.to_excel(writer, sheet_name='抖音', index=False)
                df_pinduoduo.to_excel(writer, sheet_name='拼多多', index=False)
                df_xiaohongshu.to_excel(writer, sheet_name='小红书', index=False)

            print(f"表格已导出至 {output_file}")
            return df_total, str(output_file)
        else:
            fr_total_sum.extend(total_sum)

    df_total = pd.DataFrame(fr_total_sum)
    with pd.ExcelWriter(output_file) as writer:
        df_total.to_excel(writer, sheet_name='汇总结算', index=False)

    return df_total, str(output_file)

def check_date(s_date, e_date):
    if e_date < s_date:
        gr.Warning("开始日期不能大于结束日期")

def validate_discount(discount):
    if discount < 1 or discount > 10:
        return 5  # 返回默认值
    return discount

def settlement_tab():
    org_domain_id_map = get_org_domain_id_map()
    with gr.Tab("加盟商金额结算"):
        gr.Markdown("# 💰 Franchisee Amount Settlement")
        with gr.Column():
            with gr.Row():
                with gr.Column():
                    start_date = Calendar(label="开始日期", value=get_month_days(-1)[0],
                                          interactive=True)
                    end_date = Calendar(label="结束日期", value=get_month_days(-1)[-1],
                                        interactive=True)
                with gr.Column():
                    franchisee_input = gr.Radio(
                        choices=list(org_domain_id_map.keys()) + ["total"],
                        value=list(org_domain_id_map.keys())[0],
                        label="选择加盟商",
                        interactive=True,
                        type="value"
                    )
                    use_cache = gr.Radio(
                        choices=["是", "否"],
                        value="是",
                        label="使用缓存数据",
                        interactive=True,
                        type="value"
                    )
                    discount_input = gr.Number(
                        label="折扣（1-10）",
                        value=5,
                        precision=1,
                        step=0.1,
                        minimum=1,
                        maximum=10,
                    )

            with gr.Row():
                file_output = gr.File(label="下载结算表格")
                button = gr.Button("计算金额", variant="primary")

        with gr.Column():
            df_preview = gr.DataFrame(label="结算表格预览")

        start_date.change(check_date, inputs=[start_date, end_date])
        end_date.change(check_date, inputs=[start_date, end_date])
        discount_input.change(
            validate_discount,
            inputs=[discount_input],
            outputs=[discount_input]
        )

        # 点击按钮调用get_settlement生成表格
        button.click(
            get_settlement,
            inputs=[franchisee_input, start_date, end_date, discount_input, use_cache],
            outputs=[df_preview, file_output]
        )

if __name__ == '__main__':
    start, *_, end = get_month_days(-1)
    # for fr in ["hengyang", "syzy01", "hongjun", "xiaoyi"]:
    for fr in ["syzy01"]:
        print(fr)
        get_settlement(fr, start, end, False)
