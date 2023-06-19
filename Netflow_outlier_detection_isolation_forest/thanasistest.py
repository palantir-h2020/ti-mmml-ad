import argparse
from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np
import matplotlib.pyplot as plt
import time
from operator import truediv
from sklearn.preprocessing import StandardScaler
from pyspark.sql import functions as F
import math
from pyspark.sql.functions import monotonically_increasing_id,regexp_replace
from pyspark.ml import Pipeline,PipelineModel
from pyspark.sql.types import *
from datetime import datetime
import os.path
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics
from time import mktime
from sklearn.ensemble import IsolationForest
from pyspark_dist_explore import hist
import matplotlib.pyplot as plt
import pickle
from pyspark.sql.window import Window
from kafka import KafkaProducer
from pyspark.ml.feature import StandardScaler,VectorAssembler
import collections
from pyspark_iforest.ml.iforest import *
import warnings
from pyspark.ml.feature import StringIndexer,VectorIndexer,IndexToString,StringIndexerModel
import ipaddress
from pyspark.ml.classification import RandomForestClassifier
import json
from sklearn.preprocessing import LabelEncoder
warnings.filterwarnings("ignore")
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
prevOutliers=[]
#spark.sparkContext.parallelize((0,25), 6)

def RollingWindowMean(df,features,time_windows):
	for i in features:
		for j in time_windows:
			print('df',(df.count(), len(df.columns)))
			w = (Window.orderBy(F.col("ts_enc").cast('long')).rangeBetween(-j, 0))
			df = df.withColumn(f'{i}_{j}_mean', F.avg(i).over(w))
			print('dfavg',(df.count(), len(df.columns)))
	return df
def RollingWindowDistinctCount(df,features,time_windows):
	for i in features:
		for j in time_windows:
			print('df',(df.count(), len(df.columns)))
			w = (Window.orderBy(F.col("ts_enc").cast('long')).rangeBetween(-j, 0))
			df = df.withColumn(f'{i}_{j}_mean', F.approx_count_distinct(i).over(w))
			print('dfcnt',(df.count(), len(df.columns)))
	return df
		
def runPipeline(batch_df,N):
	print('Parsing input stream')
	df=batch_df

	if df.count()==0:
		return
	#df=df.sample(0.001)
	print('shape',df.count())

	print('Scaling')
	df=df.withColumn("ipkt",col("ipkt").cast("int"))
	df=df.withColumn("ibyt",col("ibyt").cast("int"))
	df=df.withColumn("td",col("td").cast("int"))
	df=df.withColumn('stos',col('stos').cast('float'))
	
	indexer=StringIndexerModel.load('spark_netflow_pickled_files/flg_string_indexer')
	#indexer.setHandleInvalid('skip')
	from pyspark.sql.functions import lit
	#df = indexer.transform(df) 
	df=df.withColumnRenamed('flg','flg_original')
	df=df.filter((df.pr != 'HOP'))
	#df=df.withColumn('flg',regexp_replace(col('flg_original'),"\.{2}P\.{3}","...APRS."))
	df=df.withColumn('flg',lit('...APRS.'))
	df=indexer.transform(df)
	df=df.drop('flg')
	df=df.withColumnRenamed('flg_enc','flg')

	indexer=StringIndexerModel.load('spark_netflow_pickled_files/pr_string_indexer')
	df = indexer.transform(df) 
	df=df.withColumnRenamed('pr','pr_original')
	df=df.withColumnRenamed('pr_enc','pr')

	indexer=StringIndexerModel.load('spark_netflow_pickled_files/stos_string_indexer')
	df = indexer.transform(df) 
	df=df.withColumnRenamed('stos','stos_original')
	df=df.withColumnRenamed('stos_enc','stos')

	df = df.withColumn('ts_enc', df.ts.cast('timestamp'))
	df = df.withColumn('te_enc', df.te.cast('timestamp'))
	features = [ 'sp', 'dp', 'da']
	time_windows=[10,60,600]
	df=RollingWindowDistinctCount(df, features,time_windows)
	features = [ 'td', 'pr', 'flg','stos','ipkt','ibyt']
	df=RollingWindowMean(df, features, time_windows)
	df=df.drop('ts_enc','te_enc')#,'sa','da','dp','sp')#.partitionBy("label")
	pipeline_normalize=PipelineModel.load('spark_netflow_pickled_files/normalization_pipeline')
	print(pipeline_normalize.stages)
	df_transf=pipeline_normalize.transform(df)
	print('df',(df.count(), len(df.columns)))
	print('Running Inference')
	model=IForestModel.load('spark_netflow_pickled_files/iforest')
	preds_isolation=model.transform(df_transf)
	print('preds_isolation',(preds_isolation.count(), len(preds_isolation.columns)))
	print('preds_isolation',preds_isolation.columns)

	#preds_isolation=preds_isolation.drop('flg','pr','stos')
	#preds_isolation=preds_isolation.withColumnRenamed('flg_original','flg')
	#preds_isolation=preds_isolation.withColumnRenamed('pr_original','pr')
	#preds_isolation=preds_isolation.withColumnRenamed('stos_original','stos')
	preds_isolation=preds_isolation.withColumn("dp",col("dp").cast("int"))
	preds_isolation=preds_isolation.withColumn("stos",col("stos").cast("int"))
	#outliers=preds_isolation.where(preds_isolation.anomalyScore > args.cutoff).drop("prediction",'sp_10_mean', 'sp_60_mean', 'sp_600_mean', 'dp_10_mean', 'dp_60_mean', 'dp_600_mean', 'da_10_mean', 'da_60_mean', 'da_600_mean', 'td_10_mean', 'td_60_mean', 'td_600_mean', 'pr_10_mean', 'pr_60_mean', 'pr_600_mean', 'flg_10_mean', 'flg_60_mean', 'flg_600_mean', 'stos_10_mean', 'stos_60_mean', 'stos_600_mean', 'ipkt_10_mean', 'ipkt_60_mean', 'ipkt_600_mean', 'ibyt_10_mean', 'ibyt_60_mean', 'ibyt_600_mean', 'features_interm', 'features')
	#print('outliers',(outliers.count(), len(outliers.columns),outliers.columns))
	import plotille
	fig=plotille.Figure()
	fig.histogram(np.array(preds_isolation.select('anomalyScore').collect()).reshape(-1))
	print(fig.show())
	#cutoffindexes=np.where(preds_isolation<args.cutoff)
	
	producer = KafkaProducer(bootstrap_servers=['10.101.41.255:9092'],value_serializer=str.encode)

	print('Writing outliers to kafka')
	global prevOutliers
	outliers=outliers.toPandas()
	rows = outliers.to_csv(header=False,index=False).split('\n')
	for row in rows:
		while len(prevOutliers)>N:
			del prevOutliers[0]
		if row in prevOutliers:
			continue
		else:
			prevOutliers.append(row)
			print(row)
			producer.send('netflow-ad-tba', value=row) 

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Process some integers.')
	parser.add_argument('--cutoff', type=float, help='Cutoff point.',default=0.55)
	args = parser.parse_args()
	
	offset="0"
	N=1000
	while True:
		df = spark.read.format("kafka").option("kafka.bootstrap.servers", "10.101.41.255:9092").option("subscribe", "netflow-anonymized-preprocessed-ordered").option("failOnDataLoss","false")
		if os.path.exists("kafkaOffset.txt"):
			f=open("kafkaOffset.txt")
			offset=f.readlines()[0].strip()
		else:
			offset="0"
		df=df.option("startingOffsets", """{"netflow-anonymized-preprocessed-ordered":{"0":"""+offset+"""}}""").load()
		#df=df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition", "offset", "timestamp", "timestampType")
		newoffset=df.select(col("offset")).alias("offset").select("offset.*")
		newoffset=newoffset.agg(F.max("offset")).collect()[0]['max(offset)']
		print(df.count())
		with open("kafkaOffset.txt","w") as of:
			if int(newoffset)-N<0:
				of.write("0")
			else:
				of.write(str(int(newoffset)-N))
		df=df.select(col("value").cast("string")).alias("csv").select("csv.*")
		#cols=["ts","te","td","sa","da","sp","dp","pr","flg","stos","ipkt","ibyt"]
		cols=["ts","te","td","sa","da","sp","dp","pr","flg","fwd","stos","ipkt","ibyt","opkt","obyt","in","out","sas","das","smk","dmk","dtos","dir","nh","nhb","svln","dvln","ismc","odmc","idmc","osmc","mpls1","mpls2","mpls3","mpls4","mpls5","mpls6","mpls7","mpls8","mpls9","mpls10","cl","sl","al","ra","eng","exid","tr","tpkt","tbyt","cp","prtcp","prudp","pricmp","prigmp","prother","flga","flgs","flgf","flgr","flgp","flgu"]
		colargs=[]
		for i,column in enumerate(cols):
			colargs.append("split(value,',')["+str(i)+"] as "+cols[i])
		df=df.selectExpr(*colargs)
		#df=df.select('ts', 'te', 'td', 'sa', 'da', 'sp', 'dp', 'pr', 'flg', 'stos', 'ipkt','ibyt')
		df=df.select("ts","te","td","sa","da","sp","dp","pr","flg","fwd","stos","ipkt","ibyt","opkt","obyt","in","out","sas","das","smk","dmk","dtos","dir","nh","nhb","svln","dvln","ismc","odmc","idmc","osmc","mpls1","mpls2","mpls3","mpls4","mpls5","mpls6","mpls7","mpls8","mpls9","mpls10","cl","sl","al","ra","eng","exid","tr","tpkt","tbyt","cp","prtcp","prudp","pricmp","prigmp","prother","flga","flgs","flgf","flgr","flgp","flgu")
		df.select("ts","te").show(10)
		#df=df.dropna()
		#df=df.limit(1000)
		runPipeline(df,N)


