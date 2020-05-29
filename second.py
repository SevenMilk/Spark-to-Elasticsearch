import sys
import json
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
	
def trans_schema(data):
    return {"date": data["transaction_year_month_and_day"],\
            "event": [{"type": data["building_state"],"district": data["The_villages_and_towns_urban_district"]}]}	

if __name__ == "__main__":

	spark = SparkSession \
			.builder \
			.getOrCreate()
	sc = spark.sparkContext
	
	#read file
	Taipei_spark_df          = spark.read.csv("hdfs://localhost//user/cloudera/interview/A_lvr_land_A.csv", header=True)
	New_Taipei_City_spark_df = spark.read.csv("hdfs://localhost//user/cloudera/interview/F_lvr_land_A.csv", header=True)
	Taoyuan_spark_df         = spark.read.csv("hdfs://localhost//user/cloudera/interview/H_lvr_land_A.csv", header=True)
	Taichung_spark_df        = spark.read.csv("hdfs://localhost//user/cloudera/interview/B_lvr_land_A.csv", header=True)
	Kaohsiung_spark_df       = spark.read.csv("hdfs://localhost//user/cloudera/interview/E_lvr_land_A.csv", header=True)
	
	#ETL
	df_filter_Taipei = trans_first(Taipei_spark_df)
	df_filter_New_Taipei_City = trans_first(New_Taipei_City_spark_df)
	df_filter_Taoyuan = trans_first(Taoyuan_spark_df)
	df_filter_Taichung = trans_first(Taichung_spark_df)
	df_filter_Kaohsiung = trans_first(Kaohsiung_spark_df)
	
	city_Taipei = {"city":"台北市"}
	city_New_Taipei_City = {"city":"新北市"}
	city_Taoyuan = {"city":"桃園市"}
	city_Taichung = {"city":"台中市"}
	city_Kaohsiung = {"city":"高雄市"}

	time_slot_Taipei = {"time_slot":""}
	time_slot_New_Taipei_City = {"time_slot":""}
	time_slot_Taoyuan = {"time_slot":""}
	time_slot_Taichung = {"time_slot":""}
	time_slot_Kaohsiung = {"time_slot":""}	
	
	rdd_Taipei= df_filter_Taipei.rdd.map(trans_schema)
	rdd_New_Taipei_City= df_filter_New_Taipei_City.rdd.map(trans_schema)
	rdd_Taoyuan= df_filter_Taoyuan.rdd.map(trans_schema)
	rdd_Taichung= df_filter_Taichung.rdd.map(trans_schema)
	rdd_Kaohsiung= df_filter_Kaohsiung.rdd.map(trans_schema)

	#----------------------------------------------------------------------
	time_slot_Taipei["time_slot"] = rdd_Taipei.collect()
	time_slot_New_Taipei_City["time_slot"] = rdd_New_Taipei_City.collect()
	time_slot_Taoyuan["time_slot"] = rdd_Taoyuan.collect()
	time_slot_Taichung["time_slot"] = rdd_Taichung.collect()
	time_slot_Kaohsiung["time_slot"] = rdd_Kaohsiung.collect()

	city_Taipei.update(time_slot_Taipei)
	city_New_Taipei_City.update(time_slot_New_Taipei_City)
	city_Taoyuan.update(time_slot_Taoyuan)
	city_Taichung.update(time_slot_Taichung)
	city_Kaohsiung.update(time_slot_Kaohsiung)
	#-----------------------------------------------------------------------

	#trans json
	json_city_Taipei = json.dumps(city_Taipei,ensure_ascii=False,cls=DateEncoder)
	json_city_New_Taipei_City = json.dumps(city_New_Taipei_City,ensure_ascii=False,cls=DateEncoder)
	json_city_Taoyuan = json.dumps(city_Taoyuan,ensure_ascii=False,cls=DateEncoder)
	json_city_Taichung = json.dumps(city_Taichung,ensure_ascii=False,cls=DateEncoder)
	json_city_Kaohsiung = json.dumps(city_Kaohsiung,ensure_ascii=False,cls=DateEncoder)	
	
	#storage
	with open('./result_part1.json', 'w') as f:
		f.write(json_city_Taipei+','+json_city_New_Taipei_City)
	with open('./result_part2.json', 'w') as f:
		f.write(json_city_Taoyuan+','+json_city_Taichung+','+json_city_Kaohsiung)	
	
	