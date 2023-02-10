import psycopg2
from psycopg2 import Error
from datetime import datetime, timedelta
import requests
from copy import deepcopy
import os


# def get_credentials():
#     return{"user": os.environ["USER"],
#             "password": os.environ["PASSWORD"],
#             "host": os.environ["HOST"],
#             "database": os.environ["DATABASE"]}


def app_function():
    try:
        connection = psycopg2.connect(user=os.environ['USER'],
                                      password=os.environ['PASSWORD'],
                                      host=os.environ['HOST'],
                                      port="5432",
                                      database=os.environ['DATABASE'])

        cursor = connection.cursor()
        print("Информация о сервере PostgreSQL")
        print(connection.get_dsn_parameters(), "\n")

        data1 = datetime.now() - timedelta(days=3)
        data2 = datetime.now()

        cursor.execute("""with
            last_source_task as (
            select
                t.task_uid,
                pipeline_id,
                t.source_id,
                t.type,
                t.state,
                t.created_at as task_created_at,
                p.name
            from task t left join pipeline p using (pipeline_id)
            ),
            task_data_wo_error as (
            select
                pipeline_id,
                t1.name,
                t2.source_id,
                t2.type as source_type,
                t1.type as task_type,
                t1.state as task_state,
                t1.task_created_at
            from last_source_task t1
            full join source t2 using (pipeline_id, source_id)
            where t2.deleted_at is null
                and t1.state ~* 'FAILED'
                and t2.created_at BETWEEN %s and %s
            order by pipeline_id
            )
            select *
            from task_data_wo_error t1
            where true""", (data1, data2))
        records = cursor.fetchall()

        record_dict = dict()
        record_dict_appends = list()
        for count, record in enumerate(records):
            record_dict["№"] = count + 1
            record_dict["Name:"] = record[1] + ' (' + f"{record[0]}" + ')'
            record_dict["type:"] = record[3] + ' - ' + record[4]
            record_dict["time:"] = f"{record[6]:%d-%m-%Y  %H:%M:%S}"
            record_dict_appends.append(deepcopy(record_dict))

        record_str = ""
        for record_dict_append in record_dict_appends:
            for key, value in record_dict_append.items():
                record_str += "".join([f"{key} ", f"{value} "])
            record_str += "".join("\n")

        print(record_str)
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Соединение с PostgreSQL закрыто")

            body = {
                    "text": record_str
                    }
            headers = {"Content-Type": "application/json"}

            url = os.environ['URL']
            response = requests.post(url, headers=headers, json=body)


if __name__ == '__main__':
    app_function()

