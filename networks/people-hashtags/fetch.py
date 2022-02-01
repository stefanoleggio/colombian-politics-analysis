import psycopg2
from sshtunnel import SSHTunnelForwarder
import pandas as pd

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

        curs.execute("SELECT * FROM public.users;")

        politicians = pd.DataFrame.from_records(curs, columns = ['id', 'label'])


        curs.execute("SELECT * FROM hashtag;")

        hashtags = pd.DataFrame.from_records(curs, columns = ['id', 'label'])

        nodes = pd.concat([politicians, hashtags])

        curs.execute("SELECT user_id, hashtag_id FROM public.tweet JOIN tweet_hashtag ON id = tweet_id where in_reply_twitter_id is null;")

        edges = pd.DataFrame.from_records(curs, columns = ['source', 'target'])

        nodes.to_csv('nodes.csv', columns=['id','label'], index=False)

        edges.to_csv('edges.csv', columns=['source','target'], index=False)

except (Exception) as error:
    print("Connection Failed")
    print(error)