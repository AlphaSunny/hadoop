from pyspark import SparkConf, SparkContext


def loadMovieNames():
    movieNames = {}
    with open('ml-100k/u.item') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames


def parseInput(line):
    fields = line.split()
    return (int(fields[1]), (float(fields[2], 1.0)))



if __name__ == "__main":
    conf = SparkConf.setAppName('WorstMovies')
    sc = SparkContext(conf = conf)

    movieNames = loadMovieNames()

    lines = sc.textFile('hdfs:///user/maria_dev/ml-100k/u.data')
    
    # convert to (movieId, (ratings, 1.0))
    movieRatings = lines.map(parseInput)

    # find the totalRatings and totalCounts
    ratingTotalsAndCount = movieRatings.reduceByKey(lambda m1, m2: (m1[0] + m2[0], m1[1]+ m2[1])) 
    
    # find the average
    averageRatings = ratingTotalsAndCount.mapValues(lambda totalAndCount: totalAndCount[0] / totalAndCount[1])

    # sort movies
    sortMovies = averageRatings.sortBy(lambda x: x[1])

    # results
    results = sortMovies.take(10)

    # print
    for result in results:
        print(movieNames[result[0]], result[1])




