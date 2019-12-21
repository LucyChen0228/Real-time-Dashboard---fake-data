# -*- coding:utf-8 -*-
import random

from flask import Flask, render_template, request
from pyecharts import Bar, configure, Polar
import pymysql
from sshtunnel import SSHTunnelForwarder
from datetime import datetime, timedelta
import random
import json

app = Flask(__name__)

REMOTE_HOST = "https://pyecharts.github.io/assets/js"


@app.route("/")
def default():
    # 将这行代码置于首部
    configure(global_theme='chalk')
    with SSHTunnelForwarder( #confidential
            ("", ),
            ssh_username="",
            # ssh_pkey="",
            ssh_password="",
            remote_bind_address=('', ),
            local_bind_address=('', )
    ) as tunnel:
        conn = pymysql.connect(host='',  # 此处必须是是127.0.0.1
                               port=,
                               user='',
                               passwd='',
                               database="")
        cur = conn.cursor()
        start_date = request.args.get('start_date', '20190401')
        if not request.args.get('end_date', None):
            end_date = datetime.strptime(start_date, "%Y%m%d") + timedelta(days=30)
            end_date = end_date.strftime("%Y%m%d")
        else:
            end_date = request.args.get('end_date', '20190401')

        sql = "select action, DATE_FORMAT(time,'%%Y%%m%%d'), count(*)  from qk_test \
            where '%s' <= DATE_FORMAT(time,'%%Y%%m%%d') and  DATE_FORMAT(time,'%%Y%%m%%d') <= '%s' \
            group by action, DATE_FORMAT(time,'%%Y%%m%%d');" % (
            start_date, end_date)
        try:
            # 执行SQL语句
            cur.execute(sql)
            # 获取所有记录列表
            results = cur.fetchall()
        except Exception as e:
            print ("Error: unable to fetch data")
            print e
    date_action_dict = {}
    for row in results:
        tmp = date_action_dict.get(row[1], {})
        tmp[row[0]] = row[2]
        date_action_dict[row[1]] = tmp
    date_list = []
    student_register_list = []
    student_payment_list = []
    student_appointment_list = []
    student_login_list = []
    for k, v in sorted(date_action_dict.items(), key=lambda x: x[0]):
        date_list.append(k)
        student_register_list.append(v.get("student_register", 0))
        student_payment_list.append(v.get("student_payment", 0))
        student_appointment_list.append(v.get("student_appointment", 0))
        student_login_list.append(v.get("student_login", 0))
    bar = Bar("活跃指数", width="100%", height="800%")  #active index
    bar.add("注册人次", date_list, student_register_list, is_stack=True, datazoom_type="both", is_datazoom_show=True) #enrollment
    bar.add("付费人次", date_list, student_payment_list, is_stack=True, datazoom_type="both", is_datazoom_show=True) #students who have payment actions
    bar.add("约课人次", date_list, student_appointment_list, #the students who made appointments for classes.
            is_stack=True, datazoom_type="both", is_datazoom_show=True)  # , datazoom_type="both", is_datazoom_show=True
    bar.add("消课人次", date_list, student_login_list, is_stack=True, datazoom_type="both", is_datazoom_show=True) #students who took classes

    return render_template(
        "pyecharts.html",
        myechart=bar.render_embed(),
        host=REMOTE_HOST,
        script_list=bar.get_js_dependencies(),
    )


@app.route("/lesson")
def lesson():
    # 将这行代码置于首部
    configure(global_theme='chalk')
    with SSHTunnelForwarder( #confidential
            ("", ),
            ssh_username="",
            # ssh_pkey="",
            ssh_password="",
            remote_bind_address=('', ),
            local_bind_address=('', )
    ) as tunnel:
        conn = pymysql.connect(host='',  # confidential
                               port=,
                               user='',
                               passwd='',
                               database="")
        cur = conn.cursor()
        start_date = request.args.get('start_date', '20190401')
        if not request.args.get('end_date', None):
            end_date = datetime.strptime(start_date, "%Y%m%d") + timedelta(days=30)
            end_date = end_date.strftime("%Y%m%d")
        else:
            end_date = request.args.get('end_date', '20190401')
        action_lesson_dict = {}

        # 2
        sql = "SELECT JSON_EXTRACT(params, '$.lessonBatchId') AS lessonBatchId, COUNT(*) FROM qk_test " \
              "WHERE action = 'student_logout' GROUP BY lessonBatchId;"
        # 执行SQL语句
        cur.execute(sql)
        # 获取所有记录列表
        for row in cur.fetchall():
            stage = 2
            lessonBatchId = row[0]
            stage_num = row[1]
            tmp = action_lesson_dict.get(lessonBatchId, {})
            tmp[stage] = stage_num
            action_lesson_dict[lessonBatchId] = tmp

        # 1
        sql = "SELECT JSON_EXTRACT(params, '$.lessonBatchId') AS lessonBatchId, COUNT(*) FROM qk_test " \
              "WHERE action = 'student_login' GROUP BY lessonBatchId;"
        # 执行SQL语句
        cur.execute(sql)
        # 获取所有记录列表
        for row in cur.fetchall():
            stage = 1
            lessonBatchId = row[0]
            stage_num = row[1] - action_lesson_dict[lessonBatchId].get(2, 0)
            tmp = action_lesson_dict.get(lessonBatchId, {})
            tmp[stage] = stage_num
            action_lesson_dict[lessonBatchId] = tmp

        # 0
        sql = "SELECT JSON_EXTRACT(params, '$.lessonBatchId') AS lessonBatchId, COUNT(*) FROM qk_test " \
              "WHERE action = 'student_appointment' GROUP BY lessonBatchId;"
        # 执行SQL语句
        cur.execute(sql)
        # 获取所有记录列表
        for row in cur.fetchall():
            stage = 0
            lessonBatchId = row[0]
            stage_num = row[1] - action_lesson_dict[lessonBatchId].get(1, 0) \
                        - action_lesson_dict[lessonBatchId].get(2, 0)
            tmp = action_lesson_dict.get(lessonBatchId, {})
            tmp[stage] = stage_num
            action_lesson_dict[lessonBatchId] = tmp

    lesson_list = []
    lesson_appointment_stage_list = []
    lesson_unfinish_stage_list = []
    lesson_finish_stage_list = []
    # print action_lesson_dict
    for k, v in sorted(action_lesson_dict.items(), key=lambda x: x[1].get(2, 0), reverse=False):
        lesson_list.append(k)
        lesson_appointment_stage_list.append(v.get(0, 0))
        lesson_unfinish_stage_list.append(v.get(1, 0))
        lesson_finish_stage_list.append(v.get(2, 0))
    radius = lesson_list
    polar = Polar("lesson指数", width="100%", height="800%")
    polar.add(
        "约课状态", #appointment
        lesson_appointment_stage_list,
        radius_data=radius,
        type="barRadius",
        is_stack=True,
    )
    polar.add(
        "未完课状态", # attended classes but not finished
        lesson_unfinish_stage_list,
        radius_data=radius,
        type="barRadius",
        is_stack=True,
    )
    polar.add(
        "完课状态", #finished classes
        lesson_finish_stage_list,
        radius_data=radius,
        type="barRadius",
        is_stack=True,
    )

    return render_template(
        "pyecharts.html",
        myechart=polar.render_embed(),
        host=REMOTE_HOST,
        script_list=polar.get_js_dependencies(),
    )


if __name__ == '__main__':
    app.run(  #confidential
        host='',
        port=,
        debug=False
    )