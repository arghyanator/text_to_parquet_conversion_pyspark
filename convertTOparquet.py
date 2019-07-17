from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *

'''
Convert Pipe-separated Text Data file to Parquet format
Author: Arghya Banerjee
Schema is for table 
Date Modified: xxx-xx-xxxx
Reference:
    1. https://medium.com/@bufan.zeng/use-parquet-for-big-data-storage-3b6292598653
    2. https://www.analyticsvidhya.com/blog/2016/10/spark-dataframe-and-operations/
'''

if __name__ == "__main__":
    sc = SparkContext(appName="CSV2Parquet")
    sqlContext = SQLContext(sc)

    #Load the data into an RDD using delimiter '|'
    rdd = sc.textFile("/data/export_table.dat").map(lambda line: line.split('|'))
    ## Print the loaded data if you want to
    '''
    print "Printing original data\n=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-\n"
    print rdd.take(5)
    print "\nPRINTED original data...Bye\n=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-\n"
    '''
    #Create rdd into Data Frame with column names including the first and last empty columns before the first '|' and last '|'
    df = sqlContext.createDataFrame(rdd, ['EMPTYBEGIN', 'date', 'col1', 'col2', 'col3', 'EMPTYEND'])
    ## Print Data Frame if you want to with the first and last empty columns
    #df.show(10)

    ## OR Selectively pring only non-empty columns
    #df.select(['date', 'col1', 'col2', 'col3']).show(10)

    ## Write into single parquet file NOT A GOOD IDEA
    #df.coalesce(1).write.format("parquet").mode("append").save("/data/parquet/export_table.parquet")
    
    ##Write DataFrame to Parquet file WITH PARTITIONS which will create sublfoders based on values in date
    df.select(['date', 'col1', 'col2', 'col3']).write.partitionBy('date').parquet("/data/parquet/export_table.parquet")
