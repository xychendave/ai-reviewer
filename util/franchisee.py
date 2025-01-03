import re

from ashe import get_month_days

from util.db import get_mysql_client, get_mongo_client

mongodb = get_mongo_client()

def get_org_id(ns):
    mysql = get_mysql_client("franchisee_center")
    sql = f"""
    SELECT id FROM organization
    WHERE domain = '{ns}'
    """
    with mysql.cursor() as cursor:
        cursor.execute(sql)
        result = cursor.fetchone()

    if result is None:
        return None
    return result[0]

def get_org_domain_id_map():
    mysql = get_mysql_client("franchisee_center")
    sql = f"""
    SELECT id, domain FROM organization
    """
    with mysql.cursor() as cursor:
        cursor.execute(sql)
        res = {}
        for row in cursor.fetchall():
            if row[1] not in ["", "rzgk01"]:
                res[row[1]] = row[0]
    return res

def get_user_info(franchisee):
    mysql = get_mysql_client(f"{franchisee}_manage_center")
    sql = "SELECT id, ding_user_id, name, mobile  FROM user"
    with mysql.cursor() as cursor:
        cursor.execute(sql)
        user_dict = {}
        for row in cursor.fetchall():
            name = re.sub(r'[\u0000-\u001F\u007F-\u009F\u202A-\u202E]', '', row[2])
            user_dict[row[0]] = [row[1], name, row[3][:3] + "****" + row[3][-4:]]
    return user_dict

def get_user_department(franchisee):
    # user_ding_id to user_id
    user_map = get_user_info(franchisee)
    user_id_map = {}
    for user_id, user_item in user_map.items():
        user_id_map[user_item[0]] = user_id

    mysql = get_mysql_client(f"{franchisee}_manage_center")
    sql = """
    WITH RECURSIVE DepartmentHierarchy AS (
    -- 基本查询，选择所有客服部门作为起点
    SELECT
        d.ding_department_id AS dept_id,
        d.name AS dept_name,
        d.parent_ding_department_id AS parent_id,
        1 AS level,
        CAST(d.name AS CHAR(1000)) AS path
    FROM
        department d

    UNION ALL

    -- 递归部分，连接子客服部门与父客服部门
    SELECT
        d.ding_department_id AS dept_id,
        d.name AS dept_name,
        d.parent_ding_department_id AS parent_id,
        dh.level + 1 AS level,
        CONCAT(dh.path, '/', d.name) AS path
    FROM
        department d
    JOIN
        DepartmentHierarchy dh
    ON
        d.parent_ding_department_id = dh.dept_id AND d.parent_ding_department_id != 0
    ),

    MaxLevelPaths AS (
    -- 计算每个客服部门的最大层级
    SELECT
        dept_id,
        MAX(level) AS max_level
    FROM
        DepartmentHierarchy
    GROUP BY
        dept_id
    ),

    FinalPaths AS (
    -- 从 DepartmentHierarchy 中选择具有最大层级的路径
    SELECT
        dh.dept_id,
        dh.path,
        dh.level
    FROM
        DepartmentHierarchy dh
    JOIN
        MaxLevelPaths mlp
    ON
        dh.dept_id = mlp.dept_id
        AND dh.level = mlp.max_level
    )

    -- 最终选择客服部门 ID、最大层级及路径
    SELECT
    ud.ding_user_id,
    path
    FROM
    FinalPaths
    right outer join
    user_department ud
    ON
    dept_id = ud.ding_department_id;"""

    user_id_to_department = {}
    with mysql.cursor() as cursor:
        cursor.execute(sql)
        for item in cursor.fetchall():
            if item[0] not in user_id_map.keys():
                print(f"WARN: DEPARTMENT USER DING USER ID {item[0]} NOT FOUND!")
                user_id = 0
            else:
                user_id = user_id_map[item[0]]
            user_id_to_department[user_id] = item[1]

    return user_id_to_department

def get_online_time(franchisee, start_date, end_date):
    online_res = mongodb['franchisee_center']['franchisee_online_data'].aggregate([
        {
            "$match": {
                "organization_id": get_org_id(franchisee),
                "date_origin": {"$gte": start_date, "$lte": end_date}
            }
        },
        {
            '$group': {
                '_id': '$user_id',
                'online_second': {
                    '$sum': '$online_duration'
                }
            }
        }
    ])
    online_hours = {}
    for item in online_res:
        online_hours[item["_id"]] = round(item["online_second"] / (60 * 60), 2)

    return online_hours

if __name__ == '__main__':
    # print(get_org_id('xiaoyi'))
    # print(get_user_info('xiaoyi'))
    # print(get_user_department('xiaoyi'))
    # start, *_, end = get_month_days(-1)
    # print(get_online_time('xiaoyi', start, end))
    print(get_org_domain_id_map())
