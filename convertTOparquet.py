from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *

'''
Convert Pipe-separated Text Data file to Parquet format
Author: Arghya Banerjee
Schema is for 15 string column table
Date Modified: Jul-8-2019
'''

if __name__ == "__main__":
    sc = SparkContext(appName="CSV2Parquet")
    sqlContext = SQLContext(sc)

    #Convert line into touple of 15 
    def lineTuple(line):
        values = line.split('|')
        return (
            values[1], values[2], values[3], values[4], values[5], values[6], values[7], values[8], values[9], values[10],
            values[11], values[12], values[13], values[14], values[15])
    
    #Load the data into an RDD of touples using map above
    rdd = sc.textFile("/data/export_partial.dat").map(lineTuple)
    ## Print the loaded data if you want to
    '''
    print "Printing original data\n=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-\n"
    print rdd.take(5)
    print "\nPRINTED original data...Bye\n=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-\n"
    '''
    #Create rdd into Data Frame with column names
    df = sqlContext.createDataFrame(rdd, ['value1', 'value2', 'value3', 'value4', 'value5', 
        'value6', 'value7', 'value8', 'value9', 'value10', 'value11', 'value12', 
        'value13', 'value14', 'value15'])
    ## Print Data Frame if you want to
    #df.show(100)
    #df.coalesce(1).write.format("parquet").mode("append").save("/data/parquet/export.parquet")
    ##Write DataFrame to Parquet file(s) under /data/parquet/export.parquet folder - creates folder
    df.write.parquet("/data/parquet/export.parquet")
