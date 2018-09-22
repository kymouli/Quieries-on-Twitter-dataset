from py2neo import Graph
from py2neo import Node,Relationship
import os
import json

graph = Graph()
graph.cypher.execute("MATCH (n) DETACH DELETE n")
graph.cypher.execute("CREATE INDEX ON :Author(author_name)")
graph.cypher.execute("CREATE INDEX ON :Tweet(tid)")
graph.cypher.execute("CREATE INDEX ON :Htag(hashtag)")
graph.cypher.execute("CREATE INDEX ON :Word(keyword)")

l1 = ('A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T')
l2 = ("author","author_id","author_screen_name","author_profile_image","tid","quote_count","reply_count","datetime","date","like_count","verified","sentiment","location","retweet_count","type","tweet_text","lang","retweet_source_id","quoted_source_id","replyto_source_id")

file = open('dataset.json',"r")
print file.name
json_data = json.load(file)
for k in json_data.keys():
	temp_dict = {}
	# print k
	# print "ok"
	for idx,alpha in enumerate(l1):
		temp_dict[alpha]=json_data[k][l2[idx]]

	graph.cypher.execute("MERGE (a:Author {author_screen_name:{C}}) SET a.id={B},a.author_name={A},a.image={D} MERGE (tweet_object:Tweet {tid:{E}}) SET tweet_object.quote_count={F},tweet_object.reply_count={G},tweet_object.datetime={H},tweet_object.date={I},tweet_object.like_count={J},tweet_object.verified={K},tweet_object.sentiment={L},tweet_object.location={M},tweet_object.retweet_count={N},tweet_object.type={O},tweet_object.tweet_text={P},tweet_object.lang={Q} MERGE (a)-[:POSTS]-(tweet_object)",temp_dict)
	# print temp_dict[k['tid']]
	if temp_dict['R'] is not None and temp_dict['R'] != '':
		graph.cypher.execute("MERGE (tweet_object:Tweet {tid:{E}}) MERGE (reply:Tweet {tid:{R}}) MERGE (tweet_object)-[:RETWEET_SOURCE]-(reply)",temp_dict)
	if temp_dict['S'] is not None and temp_dict['S'] != '':
		graph.cypher.execute("MERGE (tweet_object:Tweet {tid:{E}}) MERGE (quoted:Tweet {tid:{S}}) MERGE (tweet_object)-[:QUOTE_SOURCE]-(quoted)",temp_dict)
	if temp_dict['T'] is not None and temp_dict['T'] != '':
		# if temp_dict['A'] == "Amitabh Bachchan":
			# print temp_dict
		graph.cypher.execute("MERGE (tweet_object:Tweet {tid:{E}}) MERGE (replyto:Tweet {tid:{T}}) MERGE (tweet_object)-[:REPLY_SOURCE]-(replyto)",temp_dict)

	# print temp_dict
	if json_data[k]["hashtags"] is not None:
		for h in json_data[k]["hashtags"]:
			if h != '':
				temp_dict['U']=h
				graph.cypher.execute("MERGE (tweet_object:Tweet {tid:{E}}) MERGE (h_tag:Htag {hashtag:{U}}) MERGE (tweet_object)-[:TWEET_HASHTAG]-(h_tag)",temp_dict)
	if json_data[k]["keywords_processed_list"] is not None:
		for h in json_data[k]["keywords_processed_list"]:
			if h != '':
				temp_dict['V']=h
				graph.cypher.execute("MERGE (tweet_object:Tweet {tid:{E}}) MERGE (key_word:Word {keyword:{V}}) MERGE (tweet_object)-[:TWEET_KEYWORD]-(key_word)",temp_dict)
	if json_data[k]["mentions"] is not None:
		for h in json_data[k]["mentions"]:
			if h != '':
				temp_dict['W']=h
				graph.cypher.execute("MERGE (tweet_object:Tweet {tid:{E}}) MERGE(mention:Author {author_name:{W}}) MERGE (tweet_object)-[:TWEET_MENTION]-(mention)",temp_dict)	
file.close()

q11 = graph.cypher.execute("MATCH (u1:Author)-[:POSTS]->(t:Tweet)<-[:REPLY_SOURCE]-(t2:Tweet)<-[:POSTS]-(u2:Author) WHERE u1.author_screen_name = 'SrBachchan' RETURN u1.author_name AS User1, t.tid AS Tweet, u2.author_name, COLLECT(t2.tid) AS Reply, count(t2.tid) AS reply_count ORDER BY reply_count DESC")