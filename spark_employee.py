import sys
if(len(sys.argv)!=3):
    print("wrong")
    sys.exit(0)
from pyspark import SparkContext
sc=SparkContext()
f=sc.textFile(sys.argv[1])
temp=f.map(lambda x:(x.split('\t')[3],float(x.split('\t')[8])))
data=temp.reduceByKey(lambda a,b:a+b)
data.saveAsTextFile(sys.argv[2])
