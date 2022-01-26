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

        curs.execute("SELECT user_id FROM public.tweet group by user_id;")

        nodes = pd.DataFrame.from_records(curs, columns = ['id'])

        curs.execute("SELECT * FROM public.users;")

        politicians = pd.DataFrame.from_records(curs, columns = ['id', 'username'])

        nodes['label'] = pd.NaT

        for i in range(len(nodes['id'])):
            for j in range(len(politicians['id'])):
                if(nodes['id'][i] == politicians['id'][j]):
                    nodes['label'][i] = politicians['username'][j]

        curs.execute("SELECT user_id, in_reply_twitter_id FROM public.tweet where in_reply_twitter_id is not null;")

        edges = pd.DataFrame.from_records(curs, columns = ['source', 'tweet_id'])

        edges['target'] = pd.NaT

        curs.execute("SELECT user_id, twitter_id FROM public.tweet where in_reply_twitter_id is null;")

        replies = pd.DataFrame.from_records(curs, columns = ['user_id', 'tweet_id'])

        for i in range(len(replies['tweet_id'])):
            tweet_id = replies['tweet_id'][i]
            user_id = replies['user_id'][i]
            edges['target'].loc[edges['tweet_id'] == int(tweet_id)] = user_id

        edges.drop(['tweet_id'], axis=1)
        edges.to_csv('edges.csv', columns=['source','target'], index=False)
        nodes.to_csv('nodes.csv', index=False)

except (Exception) as error:
    print("Connection Failed")
    print(error)