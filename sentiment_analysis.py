# -*- coding: utf-8 -*-
"""
Created on Sun Dec 15 15:18:10 2019

@author: Laura Kane
"""
import boto3


class SentimentAnalysis:
    def __init__(self, access_key, secret_access_key, opinions):
        self.access_key = access_key
        self.secret_access_key = secret_access_key
        self.opinions = opinions
        
    def run_single_sentiment_analysis(self):
        aws_comprehend = boto3.client('comprehend',
                                      aws_access_key_id=self.access_key,
                                      aws_secret_access_key=self.secret_access_key,
                                      region_name='eu-west-1')
        
        language = aws_comprehend.detect_dominant_language(Text=self.opinions)
        language_code = language['Languages'][0]['LanguageCode']
        try:
            response = aws_comprehend.detect_sentiment(Text=self.opinions,
                                                       LanguageCode=language_code)
        except:
            response = aws_comprehend.detect_sentiment(Text=self.opinions,
                                                       LanguageCode='en')
        
        return response
