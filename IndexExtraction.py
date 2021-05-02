from mrjob.job import MRJob
from mrjob.step import MRStep
import sys
import re
import ast

class MRWordIndex(MRJob):
    
    
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
        if sentence == None:
            return set()
        return sentence
    
    
    def mapper(self, _, line):
        line = ast.literal_eval(line)
        stop = self.stopwords()
        if "asin" in line:
            asin = line['asin']
            if "title" in line:
                title = line['title']
                sT = self.create_word_set(title)
                s  = sT-stop
                if s != None:
                    for word in s:
                        yield word, asin
                
            
    def reducer(self, key, values):
        st = ""
        for value in values:
            st += str(value)+","
        yield key, st
        
        
if __name__ == '__main__':  
    MRWordIndex.run()
