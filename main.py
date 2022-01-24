import psycopg2
from sshtunnel import SSHTunnelForwarder

import csv

def listToString(l):
    s = ""
    for i in l:
        s += (i + ",")
    s = s[:-1]
    return s

def castData(curs):
    data = []
    for row in curs:
        data.append(row)
    print("Data casted")
    return data

def toCSV(filename,columns,content):
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(columns)
        writer.writerows(content)
    print("CSV output file ready")

try:
    with SSHTunnelForwarder(
        ('161.35.123.231', 22),
        ssh_username="postgres",
        ssh_password="dbConn2021!", 
        remote_bind_address=('localhost', 5432)) as server:
        
        print("server connected")

        keepalive_kwargs = {
            "keepalives": 1,
            "keepalives_idle": 60,
            "keepalives_interval": 10,
            "keepalives_count": 5
        }

        params = {
            'database': 'tweetproject',
            'user': 'postgres',
            'password': 'padova2021',
            'host': server.local_bind_host,
            'port': server.local_bind_port,
            **keepalive_kwargs
            }

        conn = psycopg2.connect(**params)
        curs = conn.cursor()
        print("database connected")

        columns_name = ['id']

        curs.execute("SELECT user_id  FROM public.tweet where in_reply_twitter_id is null;")

        content = castData(curs)

        toCSV('nodes.csv',columns_name,content)

        for record in curs:
            print(record)

except (Exception) as error:
    print("Connection Failed")
    print(error)