import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
import argparse
from pyspark.sql.functions import desc

if __name__ == '__main__':
    """
    Main Function
    """

    # Initialize Spark Context
    sc = pyspark.SparkContext()
    spark = SparkSession(sc)

    # Read Title Basics from HDFS
    imdb_title_basics_dataframe = spark.read.format('csv')\
        .options(header='true', delimiter='\t', nullValue='null', inferschema='true')\
        .load(args.hdfs_source_dir + '/title_basics/' + args.year + '/' + args.month + '/' + args.day + '/*.tsv')

    # Read Title Ratings from HDFS
    imdb_title_ratings_dataframe = spark.read.format('csv')\
        .options(header='true', delimiter='\t', nullValue='null', inferschema='true')\
        .load(args.hdfs_source_dir + '/title_ratings/' + args.year + '/' + args.month + '/' + args.day + '/*.tsv')

    # Join IMDb Title Basics and ratings
    title_basics_and_ratings_df = imdb_title_basics_dataframe.join(imdb_title_ratings_dataframe, imdb_title_basics_dataframe.tconst == imdb_title_ratings_dataframe.tconst)

    # Get Top TV Series
    top_tvseries = title_basics_and_ratings_df.\
            filter(title_basics_and_ratings_df['titleType']=='tvSeries').\
            filter(title_basics_and_ratings_df['numVotes'] > 200000)\
            .orderBy(desc('averageRating'))\
            .select('originalTitle', 'startYear', 'endYear', 'averageRating', 'numVotes')

    # Write data to HDFS
    top_tvseries.write.format('csv').\
            mode('overwrite').\
            save(args.hdfs_target_dir)