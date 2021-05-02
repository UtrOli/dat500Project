from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
import time
import re

search=""

def stopwords():
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

def create_word_set(sentence):
    if sentence == None:
        return set()
    sentence = sentence.lower()
    sentence = re.sub(r"[^a-zA-Z0-9]+", ' ', sentence)
    sentence = sentence.strip()
    sentence = set(sentence.split())
    return sentence

def calculateScore(row):
    stop = stopwords()
    query = create_word_set(search)
    sTitle = set(row.title.split(','))
    if row.description is not None:   
        sDescription = set(row.description.split(','))
    else:
        sDescription = set()
    if row.brand is not None:   
        sBrand = set(row.brand.split(','))
    else:
        sBrand = set()
        
    if row.avgrating is not None:
        sAvg = (row.avgrating-1)/4
    else:
        sAvg = 0
    if row.ratingcount is not None:
        sRating = row.ratingcount/10000000
    else:
        sRating = 0
    if row.price is not None:
        sPrice = (10000000-row.price)/100000000
    else:
        sPrice = 0
    query = query-stop
    
    tscore = len(query & sTitle)/len(query)
    dscore = len(query & sDescription)/len(query)
    if len(sBrand) > 0:
        bscore =  len(query & sBrand)/len(sBrand)
    else:
        bscore = 0
    
    score = tscore*10 + dscore * 2 + bscore * 3 + sAvg + sRating + sPrice
    
    return score, row.asin

schema1 = StructType([
    StructField("asin", StringType(), True),
    StructField("title", StringType(), True)])

schema2 = StructType([
    StructField("asin", StringType(), True),
    StructField("description", StringType(), True)])

schema3 = StructType([
    StructField("asin", StringType(), True),
    StructField("brand", StringType(), True)])

schema4 = StructType([
    StructField("asin", StringType(), True),
    StructField("price", DoubleType(), True)])

schema5 = StructType([
    StructField("asin", StringType(), True),
    StructField("avgrating", DoubleType(), True)])

schema6 = StructType([
    StructField("asin", StringType(), True),
    StructField("ratingcount", IntegerType(), True)])

schema7 = StructType([
    StructField("word", StringType(), True),
    StructField("asins", StringType(), True)])

#Files
ftitle = "hdfs://node-master:9000/result/output14/part-00000"
fdescription = "hdfs://node-master:9000/result/output10/descr"
fbrand = "hdfs://node-master:9000/result/output11/brnd"
fprice = "hdfs://node-master:9000/result/output15/part-00000"
favgrating = "hdfs://node-master:9000/result/output1/avgr"
fratingcount = "hdfs://node-master:9000/result/output8/rcount"

spark = SparkSession.builder.appName("testspark").getOrCreate()

title = spark.read.options(delimiter='\t').csv(ftitle,header=False,schema=schema1).cache()
description = spark.read.options(delimiter='\t').csv(fdescription,header=False,schema=schema2).cache()
brand = spark.read.options(delimiter='\t').csv(fbrand,header=False,schema=schema3).cache()
price = spark.read.options(delimiter='\t').csv(fprice,header=False,schema=schema4).cache()
avgrating = spark.read.options(delimiter='\t').csv(favgrating,header=False,schema=schema5).cache()
ratingcount = spark.read.options(delimiter='\t').csv(fratingcount,header=False,schema=schema6).cache()

dfnew = title.join(description, on=["asin"], how='left').join(brand, on=["asin"], how='left') \
    .join(price, on=["asin"], how='left').join(avgrating, on=["asin"], how='left') \
    .join(ratingcount, on=["asin"], how='left')

while True:
    print("Type search string:")
    search = input()
    start = time.time()
    scores = dfnew.rdd.map(calculateScore)
    print(scores.takeOrdered(10, key = lambda x: -x[0]))
    end = time.time()
    print(end - start)
    print("New search? (y/n)")
    exit = input()
    if exit != "y":
        break

spark.stop()
