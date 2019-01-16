from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

def loadMovieNames():
    movieNames = {}
    with open('ml-100k/u.item') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

def parseInput(line):
    fields = line.split()
    return Row(movieID = int(fields[1]), rating = float(fields[2]))

if __name__ == "__main__":
    spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

    # load up movie id
    movieNames = loadMovieNames()

    # get the raw data
    lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/ml-100k/u.data")

    #convert it to a
    movies = lines.map(parseInput)

    # convert that to a dataframe
    movieDataset = spark.createDataFrame(movies)

    # compute the averate rating
    averageRatings = movieDataset.groupBy("movieID").avg("rating")

    # compute count of each id
    counts = movieDataset.groupBy("movieID").count()

    # join the two
    avgAndCounts = counts.join(averageRatings, "movieID")

    #filter movies rated more than 10
    tenAvgAndCounts = avgAndCounts.filter("count>10")

    #find the top 10
    topTen = tenAvgAndCounts.orderBy("avg(rating)").take(10)

    for movie in topTen:
        print(movieNames[movie[0]], movie[1], movie[2])

    spark.stop()