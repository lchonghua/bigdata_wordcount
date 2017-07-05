#!/usr/bin/env python
import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp
import os.path
import string
import re
import sys

class Mapper(api.Mapper):
    def map(self, context):
        stopWords = ['able', 'about', 'across', 'after', 'all', 'almost', 'also', 'am', 'among', 'an', 'and', 'any', 'are', 'as', 'at', 'be', 'because', 'been', 'but', 'by', 'can', 'cannot', 'could', 'dear', 'did', 'do', 'does', 'either', 'else', 'ever', 'every', 'for', 'from', 'get', 'got', 'had', 'has', 'have', 'he', 'her', 'hers', 'him', 'his', 'how', 'however', 'i', 'if', 'in', 'into', 'is', 'it', 'its', 'just', 'least', 'let', 'like', 'likely', 'may', 'me', 'might', 'most', 'must', 'my', 'mr', 'ms', 'mrs','neither', 'no', 'nor', 'not', 'of', 'off', 'often', 'on', 'only', 'or', 'other', 'our', 'own', 'oh','rather', 'said', 'say', 'says', 'she', 'should', 'since', 'so', 'some', 'than', 'that', 'the', 'their', 'them', 'then', 'there', 'these', 'they', 'this', 'to', 'too', 'was', 'us', 'wants', 'we', 'were', 'what', 'when', 'where', 'which', 'while', 'who', 'whom', 'why', 'will', 'with', 'would', 'yet', 'you', 'your', 'ain', 'aren', 'couldn', 'didn', 'doesn', 'don','mightn', 'mustn', 'shan', 'll', 're', 've']

        fileName = str(os.path.basename(context.input_split.filename))

        inputText = re.sub('[^A-Za-z]+', ' ', str(context.value))
        words = inputText.split()
        for w in words:
            w = w.lower()
            if w not in stopWords and len(w)>1:
                pair = (w, fileName)
                context.emit(pair, 1)


class Reducer(api.Reducer):
    def reduce(self, context):
        s = sum(context.values)
        context.emit(context.key, s)


class Partitioner(api.Partitioner):
    def partition(self, key, num_reduces):
        reducer_id = (hash(key.split()[0])& sys.maxint)% num_reduces
        return reducer_id

def __main__():
        pp.run_task(pp.Factory(Mapper, Reducer, partitioner_class = Partitioner, combiner_class=Reducer))

	
