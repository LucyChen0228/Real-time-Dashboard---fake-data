# -*- coding:utf-8 -*-
import pymysql
from sshtunnel import SSHTunnelForwarder
from datetime import datetime, timedelta
import random
import json

#confidential
with SSHTunnelForwarder(
        ("", ),
        ssh_username="",
        # ssh_pkey="",
        ssh_password="",
        remote_bind_address=('', ),
        local_bind_address=('', )
) as tunnel:
    print("connected")
    conn = pymysql.connect(host='',  # confidential 
                           port=,
                           user='',
                           passwd='',
                           database="")

    cur = conn.cursor()
    # 使用 execute() 方法执行 SQL，如果表存在则删除
    cur.execute("DROP TABLE IF EXISTS qk_test")
    print"Drop table"
    # 使用预处理语句创建表
    sql = """CREATE TABLE qk_test (
             student_id  INT NOT NULL,
             action  CHAR(100),
             time datetime,  
             params CHAR(100) )"""
    cur.execute(sql)
    print"Create table"
    # 插入数据
    # sql = "INSERT INTO qk_test(student_id, \
    #    action, time, params) \
    #    VALUES ('%s', '%s',  str_to_date('%s','%%Y-%%m-%%d'),  '%s');" % \
    #       (str(001), 'student_log_in', str(datetime.now().strftime("%Y-%m-%d")), str({}))
    # print sql
    # print(cur.execute(sql))
    start_date = datetime.strptime("2019-04-01", "%Y-%m-%d")
    # 随机一千个学生的行为
    for student_id in range(11000):
        if student_id % 100 == 0:
            print student_id
        # 注册
        student_id = 100001 + student_id
        enroll_date = start_date + timedelta(days=random.randint(1, 30 * 2))
        sql_enroll = "INSERT INTO qk_test(student_id, \
                       action, time, params) \
                       VALUES ('%s', '%s',  str_to_date('%s','%%Y-%%m-%%d'),  '%s');" % \
                     (str(student_id), 'student_register', str(enroll_date.strftime("%Y-%m-%d")),
                      json.dumps({'phone': random.randint(13000000000, 14000000000)}))
        # print sql_enroll
        cur.execute(sql_enroll)
        if random.random() < 0.5:
            # 付费
            payment_date = enroll_date + timedelta(days=random.randint(0, 7))
            sql_payment = "INSERT INTO qk_test(student_id, \
                                   action, time, params) \
                                   VALUES ('%s', '%s',  str_to_date('%s','%%Y-%%m-%%d'),  '%s');" % \
                          (student_id, 'student_payment', str(payment_date.strftime("%Y-%m-%d")),
                           json.dumps({}))
            # print sql_payment
            cur.execute(sql_payment)
        else:
            continue

        if random.random() < 0.99:
            # 约课
            trial_appoint_date = payment_date + timedelta(days=random.randint(0, 3))
            trial_date = trial_appoint_date + timedelta(days=random.randint(1, 2))
            lessonbatchid = 100 + random.randint(0, 3)
            sql_trial = "INSERT INTO qk_test(student_id, \
                                               action, time, params) \
                                               VALUES ('%s', '%s',  str_to_date('%s','%%Y-%%m-%%d'),  '%s');" % \
                        (student_id, 'student_appointment', str(trial_appoint_date.strftime("%Y-%m-%d")),
                         json.dumps({"lessonBatchId": lessonbatchid,
                                     "lesson_datetime": str(trial_date.strftime("%Y-%m-%d"))}))
            # print sql_trial
            cur.execute(sql_trial)
            if random.random() < 0.90:
                # 登入
                sql_trial = "INSERT INTO qk_test(student_id, \
                                       action, time, params) \
                                       VALUES ('%s', '%s',  str_to_date('%s','%%Y-%%m-%%d'),  '%s');" % \
                            (student_id, 'student_login', str(trial_date.strftime("%Y-%m-%d")),
                             json.dumps({"lessonBatchId": lessonbatchid}))
                cur.execute(sql_trial)
                # print sql_trial
                if random.random() < 0.8:
                    # 登出
                    sql_trial = "INSERT INTO qk_test(student_id, \
                                                           action, time, params) \
                                                           VALUES ('%s', '%s',  str_to_date('%s','%%Y-%%m-%%d'),  '%s');" % \
                                (student_id, 'student_logout', str(trial_date.strftime("%Y-%m-%d")),
                                 json.dumps({"lessonBatchId": lessonbatchid}))
                    # print sql_regular
                    cur.execute(sql_trial)
                    # print sql_trial
                else:
                    continue
            else:
                continue
        else:
            continue

        regular_date = trial_date
        lessonbatchid = 200
        while True:
            if random.random() < 0.9:
                # 正价
                regular_appoint_date = regular_date + timedelta(days=random.randint(0, 2))
                regular_date = regular_appoint_date + timedelta(days=random.randint(1, 2))
                lessonbatchid = lessonbatchid + 1
                if lessonbatchid > 210:
                    break
                sql_regular = "INSERT INTO qk_test(student_id, \
                                                       action, time, params) \
                                                       VALUES ('%s', '%s',  str_to_date('%s','%%Y-%%m-%%d'),  '%s');" % \
                              (student_id, 'student_appointment', str(regular_appoint_date.strftime("%Y-%m-%d")),
                               json.dumps({"lessonBatchId": lessonbatchid,
                                           "lesson_datetime": str(regular_date.strftime("%Y-%m-%d"))}))
                # print sql_regular
                cur.execute(sql_regular)
                if random.random() < 0.90:
                    # 登入
                    sql_regular = "INSERT INTO qk_test(student_id, \
                                           action, time, params) \
                                           VALUES ('%s', '%s',  str_to_date('%s','%%Y-%%m-%%d'),  '%s');" % \
                                  (student_id, 'student_login', str(regular_date.strftime("%Y-%m-%d")),
                                   json.dumps({"lessonBatchId": lessonbatchid}))
                    # print sql_regular
                    cur.execute(sql_regular)
                    if random.random() < 0.98:
                        # 登出
                        sql_regular = "INSERT INTO qk_test(student_id, \
                                                               action, time, params) \
                                                               VALUES ('%s', '%s',  str_to_date('%s','%%Y-%%m-%%d'),  '%s');" % \
                                      (student_id, 'student_logout', str(regular_date.strftime("%Y-%m-%d")),
                                       json.dumps({"lessonBatchId": lessonbatchid}))
                        # print sql_regular
                        cur.execute(sql_regular)
                    else:
                        break
                else:
                    break
            else:
                break
        conn.commit()