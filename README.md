# first.py
由spark讀取csv，ETL後使用ES-Hadoop將資料輸出至elasticsearch


執行指令：

     $ spark-submit --master spark://quickstart.cloudera:7077 --jars /home/cloudera/interview/elasticsearch-hadoop-6.2.4/dist/elasticsearch-spark-20_2.11-6.2.4.jar first.py


# second.py
由spark讀取csv，ETL(時間desc)後存成json file輸出


執行指令：

     $ spark-submit --master spark://quickstart.cloudera:7077 second.py
