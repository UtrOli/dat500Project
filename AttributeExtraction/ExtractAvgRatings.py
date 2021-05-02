from mrjob.job import MRJob
import sys
import json
import ast

class MRRatings(MRJob):
    
    
    def mapper(self, _, line):
        line = json.loads(line)
        if "asin" in line:
            asin = line["asin"]
            if "overall" in line:
                rating = line['overall']
        
                yield asin,(rating,1)
            
        
    def combiner(self, key, values):
        avg = 0
        count = 0
        for value in values:
            avg += value[0]
            count += value[1]
        yield key, (avg,count)

        
    def reducer(self, key, values):
        avg = 0
        count = 0
        for value in values:
            avg += value[0]
            count += value[1]
        avg = avg/count
        yield key, float(avg)
        #yield key, int(count)

if __name__ == '__main__':  
    MRRatings.run()
