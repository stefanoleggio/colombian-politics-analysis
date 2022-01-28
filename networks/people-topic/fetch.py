import psycopg2
from sshtunnel import SSHTunnelForwarder
import pandas as pd

def nodes(curs):
	curs.execute("SELECT id,topic_name FROM public.topic;")
	topics = pd.DataFrame.from_records(curs, columns = ['id', 'label'])
		
	curs.execute("SELECT user_id FROM public.tweet group by user_id;")

	nodes = pd.DataFrame.from_records(curs, columns = ['id'])

	curs.execute("SELECT * FROM public.users;")

	politicians = pd.DataFrame.from_records(curs, columns = ['id', 'username'])

	nodes['label'] = pd.NaT

	for i in range(len(nodes['id'])):
		for j in range(len(politicians['id'])):
		  	if(nodes['id'][i] == politicians['id'][j]):
		        	nodes['label'][i] = politicians['username'][j]
		            
	nodes = nodes.append(topics)
	nodes.to_csv('nodes.csv', index=False)

def edges(curs):
	curs.execute("SELECT user_id,id_topic FROM public.tweet_topic JOIN public.tweet ON id_tweet = id;") 
	edges = pd.DataFrame.from_records(curs, columns = ['source','target'])
	edges.to_csv('edges.csv', index=False)

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
		edges(curs)
		
except (Exception) as error:
	    print("Connection Failed")
	    print(error)
