# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.shortcuts import render
from django.http import HttpResponse
from django.template import loader
from cassandra.cluster import Cluster
import operator
from py2neo import Graph
from py2neo import Node,Relationship


def index(request):
    # template = loader.get_template('index.html')
    context = {
        'checking': 1,
    }
    return render(request, 'index.html', context)

def submit(request):
    query_number = request.POST.get('query_number')
    # print query_number
    graph = Graph()
    query = request.POST.get('query_number')

def submit_old(request):
    query_number = request.POST.get('query_number')
    print query_number
    cluster = Cluster()
    session = cluster.connect('twitter')
    data = 0
    results = {}
    columns = []
    list_of_results = []
    results_2 = {}
    result_keys = []
    result_values = []
    if query_number == '2':
        cur_hashtag = '1YearOfRaees'
        query = "SELECT single_hashtag, single_mention FROM twitter.tweet_lab_2 WHERE single_hashtag = '1YearOfRaees' ALLOW FILTERING;"
        columns = ['hashtag', 'mention', 'co-occurences']
        data = 1
        results = session.execute(query)
        pre_mention = ""
        cur_mention = ""
        count = 0

        for res in results:
            print res
            '''
            cur_mention = res.single_mention
            if not pre_mention:
                pre_mention = res.single_mention
                count = -1
            if pre_mention == cur_mention:
                count += 1
            else:
                results_2[pre_mention] = count + 1
                pre_mention = cur_mention
                count = 0
        sorted_results_2 = sorted(results_2.items(), key=operator.itemgetter(1))
        '''
        # result_keys = sorted_results_2.keys()
        # result_values = sorted_results_2.values()
        print results
        context = {
            'columns': columns,
            'hashtag' : cur_hashtag,
            'result': results,
            'query_number': query_number
        }
    elif query_number == '11':
        cur_date = '2017-12-30'
        query = "SELECT date, single_mention, location FROM twitter.tweet_lab_11 WHERE date = '"+cur_date+"' ALLOW FILTERING;"
        # SELECT date, single_mention, location FROM twitter.tweet_lab_11 WHERE date = '2017-12-30' ALLOW FILTERING;
        columns = ['date', 'mention', 'location', 'co-occurences']
        data = 1
        results = session.execute(query)
        context = {
            'result' : results,
            'query_number': query_number,
            'cur_date': cur_date,
            'columns' : columns
        }
    return  render(request, 'submit.html', context)