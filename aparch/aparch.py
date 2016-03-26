#coding:utf-8

from pyspark import SparkContext,SparkConf
from operator import add
import re

appName="aparch"
conf=SparkConf().setAppName(appName).setMaster("local")
sc=SparkContext(conf=conf)


logFile="/home/feige/aparch/access.log"

logRDD=sc.textFile(logFile)
logPattern="^(\\S+) (\\S+) (\\S+) \\[([\\w/]+)([\\w:/]+)\s([+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)"

def filterWithParse(s):
    m=re.match(logPattern,s)
    if m:
        return True
    else:
        return False

def parseLog(s):
    m=re.match(logPattern,s)
    clientIP=m.group(1)
    requestDate=m.group(4)
    requestURL=m.group(8)
    status=m.group(10)

    return(clientIP,requestDate,requestURL,status)

logRDDv1=logRDD.filter(filterWithParse).map(parseLog)
logRDDv1.count()
logRDDv1.saveAsTextFile('/home/feige/aparch/OutPutRDDv1')

logRDDv2=logRDDv1.map(lambda x:(x[1],1)).reduceByKey(add)
logRDDv2.sortByKey().saveAsTextFile('/home/feige/aparch/OutPutRDDv2')

logRDDv3=logRDDv1.map(lambda x:(x[1],x[0]))
logRDDv3.saveAsTextFile('/home/feige/aparch/OutPutRDDv3')

logRDDv4=logRDDv3.distinct()
logRDDv4.saveAsTextFile('/home/feige/aparch/OutPutRDDv4')

logRDDv5=logRDDv4.map(lambda x:(x[0],1)).reduceByKey(add)
logRDDv5.saveAsTextFile('/home/feige/aparch/OutPutRDDv5')

DIP=logRDDv5.collect()

logRDDv6=logRDDv1.map(lambda x:(x[3],1)).reduceByKey(add)
logRDDv6.saveAsTextFile('/home/feige/aparch/OutPutRDDv6')

StatusPV=logRDDv6.sortByKey().collect()

logRDDv7=logRDDv1.map(lambda x:(x[0],1)).reduceByKey(add)
logRDDv7.saveAsTextFile('/home/feige/aparch/OutPutRDDv7')

IPPV=logRDDv7.sortBy(lambda x:x[1],ascending=False).collect()

logRDDv8=logRDDv1.map(lambda x:(x[2],1)).reduceByKey(add)
logRDDv8.saveAsTextFile('/home/feige/aparch/OutPutRDDv8')
PagePV=logRDDv8.sortBy(lambda x:x[1],ascending=False).collect()

stopList=['jpg','ico','png','gif','css','txt','asp']

def filterWithStop(s):
    for c in stopList:
        if s.endswith('.'+c):
            return False

    return True

logRDDv9=logRDDv1.filter(lambda x:filterWithStop(x[2]))
logRDDv9.saveAsTextFile('/home/feige/apach/OutPutRDDv9')

logRDDv10=logRDDv9.map(lambda x:(x[2],1)).reduceByKey(add)
logRDDv10.saveAsTextFile('/home/feige/apach/OutPutRDDv10')

PagePV=logRDDv8.sortBy(lambda x:x[1],ascending=False).collect()





