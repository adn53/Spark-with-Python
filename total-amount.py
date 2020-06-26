from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("total-amount")
sc = SparkContext(conf = conf)

def parseline(line):
    fields = line.split(',')
    id = int(fields[0])
    amount = float(fields[2])
    return (id,amount)

lines = sc.textFile("file:///SparkCourse/customer-orders.csv")
rdd = lines.map(parseline)
totalsByID = rdd.reduceByKey(lambda x,y:x+y)
totalsByIDSorted = totalsByID.map(lambda x:(x[1],x[0])).sortByKey()
results = totalsByIDSorted.collect()

for result in results:
    print(result)