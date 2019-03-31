import findspark
findspark.init()
# Import the necessary Spark library classes
from pyspark import SparkConf, SparkContext
from operator import add
from __future__ import division
import sys

def generateSignatures(userMovies):
    signatures=[]
    for i in range(0, 20):
        minHash = map(lambda x: (3*x+13*i)%100, userMovies[1])
        Min = sorted(minHash)[0]
        signatures.append(Min)
    return (userMovies[0], signatures)

def generateBandRow(r,b,x):
    #x :'U1': [0,1,2,3,4]
    return ((i,(x[0],x[1][i*r:(i+1)*r])) for i in range(b))

def swap(bandRow):
    sigUser = map(lambda x: (str(bandRow[0]) + str(x[1]), x[0]), bandRow[1])
    return (sigUser)
    
def calculateJ(rdd):
    pairList = map(lambda x: str(x), rdd[1])
    return (pairList)

def unique(pairList): 
    unique_list = [] 
    for x in pairList: 
        if x not in unique_list: 
            unique_list.append(x)
    return unique_list

def findDup(unionList):
    d = []
    dCount = 0
    for elem in unionList:
        if elem not in d:
            d.append(elem)
        else:
            dCount+=1
    return (len(d),dCount)

def calJaccard(pairList):
    jDict = {}
    jList = []
    for pair in pairList:
        thisPair = []
        user1 = int(pair[0].strip("U"))
        thisPair.append(user1)
        u1Watched = userMoviesList[user1-1][1]
        user2 = int(pair[1].strip("U"))
        u2Watched = userMoviesList[user2-1][1]
        thisPair.append(user2)
        unionList = u1Watched + u2Watched
        union, intersection = findDup(unionList)
        jaccard = intersection/union
        jaccard = float(jaccard)
        jList.append(list(thisPair))
        jDict[tuple(thisPair)] = jaccard
    return jList, jDict

def findSimilarU(jList):
    result = {}
    reList = []
    for pair in jList:
        recomToU1 = []
        recomToU2 = []
        u1 = pair[0]
        u2 = pair[1]
        u1M = userMoviesList[u1-1][1]
        u2M = userMoviesList[u2-1][1]
        for movie in u2M:
            if movie not in u1M:
                recomToU1.append(movie)
        for movie in u1M:
            if movie not in u2M:
                recomToU2.append(movie)
        result[u1] = recomToU1
        result[u2] = recomToU2
        reList.append(u1)
        reList.append(recomToU1)
        reList.append(u2)
        reList.append(recomToU2)
    return result, reList
        
# Read txt file and proccess
file = sys.argv[1]
sc = SparkContext()
userMovies= sc.textFile(file)
# userMovies= sc.textFile("input_sample_large.txt")

#for each input row, create userID, and a list of movies
userMovies = userMovies.map(lambda x: (x.split(",")))
userMovies = userMovies.map(lambda x: (x[0], map(int,x[1:])))
userMoviesList = userMovies.collect()
userMoviesList = map(lambda x: (str(x[0]),x[1]),userMoviesList)
#generate 20 signatures
signatures = userMovies.map(generateSignatures)
#generate 5 bands and 4 rows in each -> band#,[(user#,[4 rows of signatures]),(u2,[s1,s2...])]
bandRow = signatures.flatMap(lambda x: generateBandRow(4,5,x)).groupByKey()
# swap key & values, find similar pair
similarPair = bandRow.flatMap(swap)
similarPair = similarPair.groupByKey()
rdd = similarPair.filter(lambda x: len(x[1])>1)
pairList = rdd.map(calculateJ).collect()
pairList = unique(pairList)
similarUserDict, reList = findSimilarU(pairList)

sys.stdout = open("output.txt", "w+")
print(sorted(similarUserDict.items()))

sc.stop()
