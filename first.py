import sys,os
import json,hashlib
from datetime import date, datetime
from pyspark import SparkContext, SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkContext
from pyspark.sql import SparkSession

class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, date):
            return obj.strftime('%Y-%m-%d')
        else:
            return json.JSONEncoder.default(self, obj)
			
def trans_first(new_spark_df):    
    #換column name(中換英)
    count = 0 
    for i in new_spark_df.schema.names:
        new_spark_df = new_spark_df.withColumnRenamed(i,list(new_spark_df.first())[count].replace(" ","_"))
        count += 1
    #排除row第二的資料
    df_filter = new_spark_df.filter(new_spark_df.The_villages_and_towns_urban_district\
                                    != 'The villages and towns urban district')
    #更改時間1080401 -> 2018-04-01     
    df_filter = df_filter.withColumn('transaction_year_month_and_day',\
                                     df_filter.transaction_year_month_and_day.cast(IntegerType())+19110000)
    df_filter = df_filter.withColumn('transaction_year_month_and_day',\
                                     df_filter.transaction_year_month_and_day.cast(StringType()))
    df_filter = df_filter.withColumn('transaction_year_month_and_day',\
                                     to_date(df_filter.transaction_year_month_and_day,'yyyyMMdd'))
    #按照時間最晚排列
    
    return df_filter.orderBy(desc('transaction_year_month_and_day'))			

def tran_json(data):
    j=json.dumps(data,ensure_ascii=False,cls=DateEncoder).encode('ascii', 'ignore')
    data['doc_id'] = hashlib.sha224(j).hexdigest()
    return (data['doc_id'],json.dumps(data,ensure_ascii=False,cls=DateEncoder))

def sparkToEs(data):
	data.saveAsNewAPIHadoopFile(\
	path='-',\
	outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",\
	keyClass="org.apache.hadoop.io.NullWritable",\
	valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",\
	conf=es_write_conf)

if __name__ == "__main__":

	spark = SparkSession \
		.builder \
		.appName("es_books") \
		.master("local") \
		.enableHiveSupport() \
		.getOrCreate()
	sc = spark.sparkContext
	
	es_write_conf = {
	"es.nodes" : "192.168.45.128",
	"es.port" : "9200",
	"es.resource" : "my_index/data",
	"es.input.json" : "yes",
	"es.mapping.id": "doc_id"
	}	
	
	Taipei_spark_df          = spark.read.csv("hdfs://localhost//user/cloudera/interview/A_lvr_land_A.csv", header=True)
	New_Taipei_City_spark_df = spark.read.csv("hdfs://localhost//user/cloudera/interview/F_lvr_land_A.csv", header=True)
	Taoyuan_spark_df         = spark.read.csv("hdfs://localhost//user/cloudera/interview/H_lvr_land_A.csv", header=True)
	Taichung_spark_df        = spark.read.csv("hdfs://localhost//user/cloudera/interview/B_lvr_land_A.csv", header=True)
	Kaohsiung_spark_df       = spark.read.csv("hdfs://localhost//user/cloudera/interview/E_lvr_land_A.csv", header=True)

	df_filter_Taipei = trans_first(Taipei_spark_df)
	df_filter_New_Taipei_City = trans_first(New_Taipei_City_spark_df)
	df_filter_Taoyuan = trans_first(Taoyuan_spark_df)
	df_filter_Taichung = trans_first(Taichung_spark_df)
	df_filter_Kaohsiung = trans_first(Kaohsiung_spark_df)	
	
	Taipei_rdd = df_filter_Taipei.rdd.map(lambda row: row.asDict(True))
	New_Taipei_City_rdd = df_filter_New_Taipei_City.rdd.map(lambda row: row.asDict(True))
	Taoyuan_rdd = df_filter_Taoyuan.rdd.map(lambda row: row.asDict(True))
	Taichung_rdd = df_filter_Taichung.rdd.map(lambda row: row.asDict(True))
	Kaohsiung_rdd = df_filter_Kaohsiung.rdd.map(lambda row: row.asDict(True))
	
	json_city_Taipei = Taipei_rdd.map(tran_json)
	json_city_New_Taipei_City = New_Taipei_City_rdd.map(tran_json)
	json_city_Taoyuan = Taoyuan_rdd.map(tran_json)
	json_city_Taichung = Taichung_rdd.map(tran_json)
	json_city_Kaohsiung = Kaohsiung_rdd.map(tran_json)

	sparkToEs(json_city_Taipei)
	sparkToEs(json_city_New_Taipei_City)
	sparkToEs(json_city_Taoyuan)
	sparkToEs(json_city_Taichung)
	sparkToEs(json_city_Kaohsiung)
	
	
