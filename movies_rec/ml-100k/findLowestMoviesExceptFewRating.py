from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

def loadMovieNames():
    movieNames = {}
    with open('../datasets/ml-100k/u.item') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    
    return movieNames

def parseInput(line):
    fields = line.split()
    return Row(movieID = int(fields[1]), rating = float(fields[2]))

if __name__ == "__main__":
    spark = SparkSession.builder.appName("PopularMovies").getOrcreate()

    # load movie names
    movieNames = loadMovieNames()

    # get the origin data
    lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/ml-100k/u.data")

    # convert
    movies = lines.map(parseInput)

    # convert it to dataframe
    movieDataset = spark.createDataFrame(movies)

    # calculate the average
    averageRatings = movieDataset.groupBy("movieID").avg("rating")

    # calculate the counts of every movie id
    counts = movieDataset.groupBy("movieID").count()

    # bind the count
    avgAndCounts = counts.join(averageRatings, "movieID")
    

    # filter
    tenAvgAndCounts = avgAndCounts.filter("count>10")

    # find top 10
    topTen = tenAvgAndCounts.orderBy("avg(rating)").take(10)

    for movie in topTen:
        print(movieNames[movie[0]], movie[1], movie[2])

    # stop
    spark.stop()