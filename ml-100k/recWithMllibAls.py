from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql.functions import lit
import platform  

def loadMovieNames():
    movieNames = {}
    with open('u.item') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1].decode('ascii', 'ignore')
    return movieNames

def parseInput(line):
    fields = line.value.split()
    return Row(userID = int(fields[0]), movieID = int(fields[1]), rating = float(fields[2]))


if __name__=="__main__":
    spark = SparkSession.builder.appName("MovieRecs").getOrCreate()

    movieNames = loadMovieNames()
    
    # get the raw data
    lines = spark.read.text("hdfs:///user/maria_dev/ml-100k/u.data").rdd

    # convert it to a of row projects
    ratingsRDD = lines.map(parseInput)
    
    # convert to a df and cache it
    ratings = spark.createDataFrame(ratingsRDD).cache()

    als = ALS(maxIter=5, regParam = 0.01, userCol = "userID", itemCol = "movieID", ratingCol = "rating")
    model = als.fit(ratings)

    # print out ratings from user 0:
    print "\nRatings for user ID 0:" 
    print(platform.python_version())
    userRatings = ratings.filter("userID = 0")
    for rating in userRatings.collect():
        print (movieNames[rating['movieID']], rating['rating'])

    print("\nTop 20 recommendations:")
    ratingCounts = ratings.groupBy("movieID").count().filter("count > 100")
    popularMovies = ratingCounts.select("movieID").withColumn('userID', lit(0))

    # Run our model on that list of popular movies for user ID 0
    recommendations = model.transform(popularMovies)

    # Get the top 20 movies with the highest predicted rating for this user
    topRecommendations = recommendations.sort(recommendations.prediction.desc()).take(20)

    for recommendation in topRecommendations:
        print (movieNames[recommendation['movieID']], recommendation['prediction'])


    
    
    spark.stop()