from mrjob.job import MRJob
from mrjob.step import MRStep
import sys
import re
import ast


class ExtractDescription(MRJob):
    
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper
            )
        ] 
    
    def mapper(self, _, line):
        line = ast.literal_eval(line)
        #Place with asins wanted here
        asins = ["B001GMX2JM","B00KIR5F64","B0041QZHH0","B003XXFFNS","B003XXFCS6","B003XAQ6FC","B001BL3MHK","B002ALRW9O",\
                "B005XF9RR4","B003MCOPWW","B002ZF8SDY"]
        if "asin" in line:
            asin = line['asin']
            if asin in asins:
                yield asin,line['title']

if __name__ == '__main__':  
    ExtractDescription.run()
