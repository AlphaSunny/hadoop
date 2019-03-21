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

    # 加载电影id
    movieNames = loadMovieNames()

    # 得到原始数据
    lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/ml-100k/u.data")

    # 转化
    movies = lines.map(parseInput)

    # 转化成dataframe
    movieDataset = spark.createDataFrame(movies)

    # 计算平均评分
    averageRatings = movieDataset.groupBy("movieID").avg("rating")

    # 计算每个评分的数量
    counts = movieDataset.groupBy("movieID").count()

    # 将count加入其中
    avgAndCounts = counts.join(averageRatings, "movieID")
    

    # 过滤
    tenAvgAndCounts = avgAndCounts.filter("count>10")

    # 找到top 10
    topTen = tenAvgAndCounts.orderBy("avg(rating)").take(10)

    for movie in topTen:
        print(movieNames[movie[0]], movie[1], movie[2])

    # 结束
    spark.stop()