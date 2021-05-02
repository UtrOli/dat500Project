from mrjob.job import MRJob
import sys
import re
import ast


class ExtractValues(MRJob):
    
    def mapper(self, _, line):
        line = ast.literal_eval(line)
        if "asin" in line:
            asin = line['asin']
            if "price" in line:
                price = line['price']
                yield asin,float(price)


    def reducer(self, key, values):
        for value in values:
            yield key, value

if __name__ == '__main__':  
    ExtractValues.run()
