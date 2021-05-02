from mrjob.job import MRJob
import sys
import re
import ast


class ExtractDescription(MRJob):
    
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
        sentence = sentence.lower()
        sentence = re.sub(r"[^a-zA-Z0-9]+", ' ', sentence)
        sentence = sentence.strip()
        sentence = set(sentence.split())
        return set(sentence)
    
    def mapper(self, _, line):
        line = ast.literal_eval(line)
        if "asin" in line:
            asin = line['asin']
            if "brand" in line:
                brand = line['brand']
                if brand != "" and brand != "Unknown":
                    sBrand = self.create_word_set(brand)
                    stop = self.stopwords()
                    sBrand = sBrand-stop
                    if len(sBrand)>0:
                        st = ""
                        for word in sBrand:
                            st += word + ","
                        yield asin,st


    def reducer(self, key, values):
        for value in values:
            yield key, value

if __name__ == '__main__':  
    ExtractDescription.run()
