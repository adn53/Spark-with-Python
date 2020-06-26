#most important in any spark problem
from pyspark import SparkConf, SparkContext
import collections

'''here we create the Spark Contest. This will be very similar in 
almost every Spark program we write.'''

'''We call sparkConf() that we loaded earlier, and set master node 
as our local machine. We are not doing any distribution here'''
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
#With above defined configuartion, we create the SparkContest object
sc = SparkContext(conf = conf)

#we are reading the data file now. 
lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")
'''we are considering just the rating value. If we open the u.data
 file in any text editor, we can see that it has 4 sections
UserID, MovieID, Ratings, Time at which the user rated it(epoch seconds). 
We do it by spliting the data and extract the 2nd position'''
ratings = lines.map(lambda x: x.split()[2])
'''CountByValue simply counts the unique value and how many times 
it occurs in the SparkContext. If rating 3 appear twice, it will 
return (3,2)'''
result = ratings.countByValue()

'''We sort the result in increasing order using Python collection 
library'''
sortedResults = collections.OrderedDict(sorted(result.items()))

'''Print every key value pair of the sorted dictionary'''
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
    
    
