from mrjob.job import MRJob
from mrjob.step import MRStep
import sys
import re
import ast
import time


class MRSearch(MRJob):

    
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                reducer=self.reducer_top
            )
        ]
    
    
    def stopwords(self):
        stop =["i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself",\
               "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself",\
               "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that",\
               "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had",\
               "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as",\
               "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through",\
               "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off",\
               "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how",\
               "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not",\
               "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should",\
               "now"]
        return set(stop)
    
    
    def create_word_set(self,sentence):
        if sentence == None:
            return set()
        sentence = sentence.lower()
        sentence = re.sub(r"[^a-zA-Z0-9]+", ' ', sentence)
        sentence = sentence.strip()
        sentence = set(sentence.split())
        return sentence
    
    
    def calculate_score(self,title,description,price,brand,avgrating,rcount):
        stop = self.stopwords()
        query = self.create_word_set(self.search)
        query = query-stop

        if avgrating != 0 and avgrating is not None:
            avgrating = (avgrating-1)/4

        if rcount != 0 and rcount is not None:
            rcount = rcount/10000000

        if price != 0 and price != None:
            price = (10000000-price)/10000000

        
        if len(title) > 0:
            tscore = len(query & title)/len(query)
        else:
            tscore = 0
        if len(description) > 0:
            dscore = len(query & description)/len(query)
        else:
            dscore = 0

        if len(brand) > 0:
            bscore =  len(query & brand)/len(brand)
        else:
            bscore = 0

        score = tscore*10 + dscore * 2 + bscore * 3 + avgrating + rcount + price
        
        return score
    
    
    def mapper(self, _, line):
        #search sentence
        #self.search ="Walter Sickert: A Biography"
        #self.search="sony camera"
        self.search = "Everyday Italian (with Giada de Laurentiis)"

        line = ast.literal_eval(line)
        if "asin" in line:
            asin = line['asin']
        else:
            return
        
        if "title" in line:
            title = line['title']
        else:
            return
        if "description" in line:
            description = line['description']
        else:
            description = None
        if "price" in line:
            price = line['price']
        else:
            price = 0
        
        if "brand" in line:
            brand = line['brand']
        else:
            brand = None
        
        title = self.create_word_set(title)
        description = self.create_word_set(description)
        brand = self.create_word_set(brand)
        
        score = self.calculate_score(title,description,price,brand,0,0)
        if score>0:
            yield None, (score,str(asin))
        
        
    def reducer_top(self, _, values):
        sValues = sorted(values, reverse=True)
        for i in range(10):
            yield sValues[i][0], sValues[i][1]
            
if __name__ == '__main__':
    MRSearch.run()
