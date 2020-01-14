# -*- coding: utf-8 -*-
"""
Created on Thu Dec 12 22:05:10 2019

@author: Laura Kane
"""
import os
import json
import pandas as pd
import matplotlib.pyplot as plt

from kafka import KafkaProducer, KafkaConsumer
from sentiment_analysis import SentimentAnalysis


def create_pieplot_percent(dict_result, output_path):
    # format data
    pie_graph_data = pd.DataFrame(list(dict_result.items()), columns=['SentimentType','SentimentScore'])
    pie_graph_data.index = pie_graph_data['SentimentType']
    del pie_graph_data['SentimentType']
    
    # plot pie chart
    fig = pie_graph_data.plot.pie(y='SentimentScore', figsize=(11, 11), autopct='%.2f%%')
    plt.ylabel('')
    plt.title('Sentiment Analysis')
    plt.rcParams.update({'font.size': 22})
    plt.legend('')
    plt.savefig(output_path)
    plt.show(fig)
        
    
def run_process(access_key, secret_access_key, opinions, output_path):
    pos = neg = neutral = mixed = 0
    
    # run sentiment analysis
    for i in opinions:
        obj = SentimentAnalysis(access_key, secret_access_key, i)
        data = obj.run_single_sentiment_analysis()
        pos += data['SentimentScore']['Positive']
        neg += data['SentimentScore']['Negative']
        neutral += data['SentimentScore']['Neutral']
        mixed += data['SentimentScore']['Mixed']
    
    cnt = len(opinions)
    dict_result = {'positive': pos/cnt,
                   'negative': neg/cnt,
                   'neutral': neutral/cnt,
                   'mixed': mixed/cnt}
   
    create_pieplot_percent(dict_result, output_path)
    
    return json.dumps(dict_result)

    
def run_producer(access_key, secret_access_key, opinions, first_run, output_path):
    if first_run:
        sentiment = json.dumps({'positive': 0,
                                'negative': 0,
                                'neutral': 0,
                                'mixed': 0})
    else:
        sentiment = run_process(access_key, secret_access_key, opinions, output_path) 

    print(sentiment)

    producer = KafkaProducer(bootstrap_servers='3.84.130.41:9092')
    sending = producer.send('sentiment', sentiment.encode('utf-8'))
    sending.get(timeout=60)


def run_consumer(access_key, secret_access_key):
    lst = []
    consumer = KafkaConsumer('sample', bootstrap_servers='3.84.130.41:9092')
    output_path = os.path.join(os.path.abspath(os.getcwd()), 'pie')

    print('Run first')
    run_producer(access_key, secret_access_key, lst, True, output_path)

    print('Continue')
    for txt in consumer:
        print(f'Sentiment: {txt.value.decode("utf-8")}')
        lst.append(txt.value.decode("utf-8"))
        run_producer(access_key, secret_access_key, lst, False, output_path)
