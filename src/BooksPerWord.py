#!/usr/bin/env python
import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp
import string

class Mapper(api.Mapper):
    def map(self, context):
        word = (str(context.value)).translate(None, string.punctuation).split()[0]
        context.emit(word, 1)


class Reducer(api.Reducer):
    def reduce(self, context):
        s = sum(context.values)
        context.emit(context.key, s)


def __main__():
        pp.run_task(pp.Factory(Mapper, Reducer))

	
