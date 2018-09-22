import json
import os
import operator
from cassandra.cluster import Cluster
directory_name = './workshop_dataset/'
cluster = Cluster()
session = cluster.connect('twitter')


directory_name = './workshop_dataset/'
cluster = Cluster()
session = cluster.connect('twitter')


create_table_query_2 = "CREATE TABLE IF NOT EXISTS tweet_lab_2 (single_hashtag text, single_mention text, quote_count int, reply_count int, hashtags list<text>, datetime timestamp, date date, like_count int, verified boolean, sentiment int, author text, location text, tid text, retweet_count int, type text, media_list map<int,frozen<map<varchar,varchar>>>, quoted_source_id text, url_list list<text>, tweet_text text, author_profile_image text, author_screen_name text, author_id text, lang text, keywords_processed_list list<text>, retweet_source_id text, mentions list<text>, replyto_source_id text, PRIMARY KEY ( single_mention, single_hashtag, tid));"
create_table_query_11 = "CREATE TABLE IF NOT EXISTS tweet_lab_11 ( single_mention text, quote_count int, reply_count int, hashtags list<text>, datetime timestamp, date date, like_count int, verified boolean, sentiment int, author text, location text, tid text, retweet_count int, type text, media_list map<int,frozen<map<varchar,varchar>>>, quoted_source_id text, url_list list<text>, tweet_text text, author_profile_image text, author_screen_name text, author_id text, lang text, keywords_processed_list list<text>, retweet_source_id text, mentions list<text>, replyto_source_id text, PRIMARY KEY (date, single_mention, location));"

session.execute(create_table_query_2)
session.execute(create_table_query_11)
count = 0
for subdir, dirs, files in os.walk(directory_name):
    print subdir
    for file in files:
		filename = open(os.path.join(subdir, file))
		data = json.load(filename)
		for ele in data.values():
			count += 1
			print count,ele['tid']
			# tweet_lab_2
			temp_2 = ele.copy()
			if temp_2['mentions'] is not None:
				for mnt in temp_2['mentions']:
					temp_2['single_mention'] = mnt
					if not temp_2['single_mention']:
						a = 2
					else:
						if temp_2['hashtags'] is not None:
							for ht in temp_2['hashtags']:
								temp_2['single_hashtag'] = ht
								if not temp_2['single_hashtag'] :
									a = 3
								else:
									query_2 = "INSERT INTO twitter.tweet_lab_2 JSON '"+json.dumps(temp_2).replace("'","''")+"';"
									session.execute(query_2)
			#tweet_11
			temp_11 = ele.copy()
			if temp_11['mentions'] is not None:
				for mnt in temp_11['mentions']:
					temp_11['single_mention'] = mnt
					if not temp_11['single_mention']:
						a = 2
					else:
						if temp_11['location'] is not None:
							query_11 = "INSERT INTO twitter.tweet_lab_11 JSON '"+json.dumps(temp_11).replace("'","''")+"';"
							session.execute(query_11)

cur_hashtag = '1YearOfRaees'
query = "SELECT single_hashtag, single_mention FROM twitter.tweet_lab_2 WHERE single_hashtag = '1YearOfRaees' ALLOW FILTERING;"
# colums = ['hashtag', 'mention', 'co-occurences']
# data = 1
results = session.execute(query)
pre_mention = ""
cur_mention = ""
count = 0
results_2 = {}
sorted_results_2 = {}
for res in results:
    cur_mention = (res.single_mention + '.')[:-1]
    # print cur_mention
    if not pre_mention:
    	# print "pre_mention",pre_mention
        pre_mention = (cur_mention + '.')[:-1] 
        count = -1
    # print pre_mention,cur_mention
    if pre_mention == cur_mention:
    	# print cur_mention
        count += 1
    else:
    	# print pre_mention,count
        results_2[pre_mention] = count + 1
        pre_mention = (cur_mention + '.')[:-1]
        count = 0
results_2[cur_mention] = count + 1
# print results_2
sorted_results_2 = sorted(results_2.items(),reverse=True)

total = 0
print "question 2 results:"

for key,value in results_2.items():
	total += 1
	print cur_hashtag,",",key,",",value

print "count: ",total

cur_date = '2017-12-30'
query = "SELECT date, single_mention, location FROM twitter.tweet_lab_11 WHERE date = '"+cur_date+"' ALLOW FILTERING;"
results = session.execute(query)

print "question 11 results:"
for res in results:
	print cur_date,",",res.single_mention,",",res.location.replace(",","")
