import argparse
from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np
import matplotlib.pyplot as plt
import time
from operator import truediv
from sklearn.preprocessing import StandardScaler
from pyspark.sql import functions as F
import math
import requests
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
import os

TENANT_ID = os.environ['TENANT_ID']
PARTITION=requests.get('http://tenant-api-service.ti-dcp:6000/api/partition/'+str(TENANT_ID)).json()['partition']
KAFKA_IP=os.environ['KAFKA_BROKERS_CSV']

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
prev=[]
#spark.sparkContext.parallelize((0,25), 6)
def partitioner(key_bytes, all_partitions, available_partitions):
	global PARTITION
	return PARTITION

def RollingWindowMean(df,features,time_windows):
	start=time.time()
	w_ord=Window.orderBy(F.col("ts_enc"))
	#print('w_ord',time.time()-start)
	for i in features:
		start=time.time()
		av=F.avg(i)
		#print('avg',time.time()-start)
		for j in time_windows:
			print('df',(df.count(), len(df.columns)))
			start=time.time()
			w = (w_ord.rangeBetween(-j, 0))
			#print('mean_rangebetween',time.time()-start)
			start=time.time()
			df = df.withColumn(f'{i}_{j}_mean', av.over(w))
			#print('av.over',time.time()-start)
			print('dfavg',(df.count(), len(df.columns)))
	return df
def RollingWindowDistinctCount(df,features,time_windows):
	start=time.time()
	w_ord=Window.orderBy(F.col("ts_enc"))
	#print('w_ord',time.time()-start)
	for i in features:
		start=time.time()
		acd=F.approx_count_distinct(i)
		#print('acd',time.time()-start)
		for j in time_windows:
			print('df',(df.count(), len(df.columns)))
			start=time.time()
			w = (w_ord.rangeBetween(-j, 0))
			#print('count_rangebetween',time.time()-start)
			start=time.time()
			df = df.withColumn(f'{i}_{j}_mean', acd.over(w))
			#print('acd.over',time.time()-start)
			print('dfcnt',(df.count(), len(df.columns)))
	return df
def debug(df):
	df2=df.toPandas().to_csv(header=False,index=False).split('\n')
	for r in df2:
		if '16.0' in r:
			print(r)
def runPipeline(batch_df,N,offset):
	print('Parsing input stream')
	df=batch_df

	if df.count()==0:
		return
	#df=df.sample(0.001)
	print('shape',df.count())

	print('Scaling')
	df=df.withColumn("ipkt",col("ipkt").cast("int"))
	df=df.withColumn("ibyt",col("ibyt").cast("int"))
	df=df.withColumn("td",col("td").cast("float"))
	df=df.withColumn('stos',col('stos').cast('float'))
	#df=df.withColumn("sp",col("sp").cast("int"))
	#df=df.withColumn("dp",col("dp").cast("int"))
	#df2=df.toPandas().to_csv(header=False,index=False).split('\n')
	#for r in df2:
		#if '16.0' in r:
			#print(r)
	indexer=StringIndexerModel.load('Netflow_outlier_detection_isolation_forest/spark_netflow_pickled_files/flg_string_indexer')
	#indexer.setHandleInvalid('skip')
	from pyspark.sql.functions import lit
	#df = indexer.transform(df) 
	df=df.withColumnRenamed('flg','flg_original')
	df=df.filter((df.pr != 'HOP'))
	df=df.filter((df.pr != 'SCTP'))
	#df=df.withColumn('flg',regexp_replace(col('flg_original'),"\.{2}P\.{3}","...APRS."))
	df=df.withColumn('flg',lit('...APRS.'))
	df=indexer.transform(df)
	df=df.drop('flg')
	df=df.withColumnRenamed('flg_enc','flg')
	#debug(df)
	indexer=StringIndexerModel.load('Netflow_outlier_detection_isolation_forest/spark_netflow_pickled_files/pr_string_indexer')
	df = indexer.transform(df) 
	df=df.withColumnRenamed('pr','pr_original')
	df=df.withColumnRenamed('pr_enc','pr')
	#debug(df)
	indexer=StringIndexerModel.load('Netflow_outlier_detection_isolation_forest/spark_netflow_pickled_files/stos_string_indexer')
	df = df.filter((df.stos == 0.0) | (df.stos==1.0) | (df.stos==2.0) | (df.stos==3.0))
	df = indexer.transform(df)
	df=df.withColumnRenamed('stos','stos_original')
	df=df.withColumnRenamed('stos_enc','stos')
	#debug(df)
	df = df.withColumn('ts_enc', df.ts.cast('timestamp'))
	df = df.withColumn('ts_enc',col('ts_enc').cast('long'))
	#df = df.withColumn('te_enc', df.te.cast('timestamp'))
	features = [ 'sp', 'dp', 'da']
	time_windows=[10,60,600]
	#debug(df)
	df=RollingWindowDistinctCount(df, features,time_windows)
	#debug(df)
	features = [ 'td', 'pr', 'flg','stos','ipkt','ibyt']
	df=RollingWindowMean(df, features, time_windows)
	#debug(df)
	df=df.drop('ts_enc','te_enc')#,'sa','da','dp','sp')#.partitionBy("label")
	pipeline_normalize=PipelineModel.load('Netflow_outlier_detection_isolation_forest/spark_netflow_pickled_files/normalization_pipeline')
	print(pipeline_normalize.stages)
	#df2=df.toPandas().to_csv(header=False,index=False).split('\n')
	#for r in df2:
		#if '16.0' in r:
			#print(r)
	df_transf=pipeline_normalize.transform(df)
	print('df',(df.count(), len(df.columns)))
	print('Running Inference')
	model=IForestModel.load('Netflow_outlier_detection_isolation_forest/spark_netflow_pickled_files/iforest')
	#df2=df_transf.toPandas().to_csv(header=False,index=False).split('\n')
	#for r in df2:
		#if '16.0' in r:
			#print(r)
	preds_isolation=model.transform(df_transf)
	print('preds_isolation',(preds_isolation.count(), len(preds_isolation.columns)))
	print('preds_isolation',preds_isolation.columns)

	#preds_isolation=preds_isolation.drop('flg','pr','stos')
	#preds_isolation=preds_isolation.withColumnRenamed('flg_original','flg')
	#preds_isolation=preds_isolation.withColumnRenamed('pr_original','pr')
	#preds_isolation=preds_isolation.withColumnRenamed('stos_original','stos')
	preds_isolation=preds_isolation.withColumn("dp",col("dp").cast("int"))
	preds_isolation=preds_isolation.withColumn("stos_original",col("stos_original").cast("int"))
	preds_isolation=preds_isolation.withColumn("td",col("td").cast("float"))
	#outliers=preds_isolation.where(preds_isolation.anomalyScore > args.cutoff).drop("prediction",'sp_10_mean', 'sp_60_mean', 'sp_600_mean', 'dp_10_mean', 'dp_60_mean', 'dp_600_mean', 'da_10_mean', 'da_60_mean', 'da_600_mean', 'td_10_mean', 'td_60_mean', 'td_600_mean', 'pr_10_mean', 'pr_60_mean', 'pr_600_mean', 'flg_10_mean', 'flg_60_mean', 'flg_600_mean', 'stos_10_mean', 'stos_60_mean', 'stos_600_mean', 'ipkt_10_mean', 'ipkt_60_mean', 'ipkt_600_mean', 'ibyt_10_mean', 'ibyt_60_mean', 'ibyt_600_mean', 'features_interm', 'features')
	#print('outliers',(outliers.count(), len(outliers.columns),outliers.columns))
	import plotille
	fig=plotille.Figure()
	fig.histogram(np.array(preds_isolation.select('anomalyScore').collect()).reshape(-1))
	print(fig.show())
	preds_isolation=preds_isolation.drop("prediction","features_interm")
	print("preds_isolation.columns",preds_isolation.columns)
	producer = KafkaProducer(bootstrap_servers=[KAFKA_IP],value_serializer=str.encode,partitioner=partitioner)
	print('Writing outliers to kafka')
	global prev
	#outliers=outliers.toPandas()
	netflowInFtrs=preds_isolation.select('ts', 'te', 'td', 'sa', 'da', 'sp', 'dp', 'pr_original', 'flg_original', 'fwd', 'stos_original', 'ipkt', 'ibyt', 'opkt', 'obyt', 'in', 'out', 'sas', 'das', 'smk', 'dmk', 'dtos', 'dir', 'nh', 'nhb', 'svln', 'dvln', 'ismc', 'odmc', 'idmc', 'osmc', 'mpls1', 'mpls2', 'mpls3', 'mpls4', 'mpls5', 'mpls6', 'mpls7', 'mpls8', 'mpls9', 'mpls10', 'cl', 'sl', 'al', 'ra', 'eng', 'exid', 'tr', 'tpkt', 'tbyt', 'cp', 'prtcp', 'prudp', 'pricmp', 'prigmp', 'prother', 'flga', 'flgs', 'flgf', 'flgr', 'flgp', 'flgu').toPandas()
	rows = netflowInFtrs.to_csv(header=False,index=False).split('\n')
	print(netflowInFtrs.dtypes.tolist())
	netflowPreprocessedFeatures=preds_isolation.select('features').toPandas().to_csv(header=False,index=False).split('\n')
	netflowScores=preds_isolation.select('anomalyScore').toPandas().to_csv(header=False,index=False).split('\n')
	print("ROW SIZE=",len(rows),"N=",N,"OFFSET=",offset)
	with open("Netflow_outlier_detection_isolation_forest/kafkaOffset.txt","w") as of:
		if int(offset)-N+len(rows)<0:
			of.write("0")
		else:
			of.write(str(int(offset)+len(rows)-N))
	for idx,row in enumerate(rows):
		try:
			if float(netflowScores[idx])>float(args.cutoff):
				print("OUTLIER:",netflowScores[idx],'IDX:',idx)
		except:
			pass
		while len(prev)>N:
			del prev[0]
		#if row in prev:
			#continue
		if idx<=N:
			continue
		else:
			try:
				#if netflowScores[idx]>cutoff:
					#print(netflowScores[idx])
				prev.append(row)
				isAnomalous="1" if float(netflowScores[idx])>args.cutoff else "0"
				row=row+',IFOREST,'+str(netflowPreprocessedFeatures[idx])+','+str(netflowScores[idx])+','+isAnomalous
				#print(netflowPreprocessedFeatures[idx].split("\"[")[1].split("]\"")[0])
				if isAnomalous=="1":
					print(idx,row)
				producer.send('netflow-ad-tba', value=row)
			except:
				#print('====EXCEPTION IN KAFKA TRYCATCH====')
				pass 

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Process some integers.')
	parser.add_argument('--cutoff', type=float, help='Cutoff point.',default=0.55)
	args = parser.parse_args()
	
	offset="0"
	N=1000
	while True:
		df = spark.read.format("kafka").option("kafka.bootstrap.servers", KAFKA_IP).option("assign", """{"netflow-anonymized-preprocessed":["""+str(PARTITION)+"""]}""").option("failOnDataLoss","false")
		if os.path.exists("Netflow_outlier_detection_isolation_forest/kafkaOffset.txt"):
			f=open("Netflow_outlier_detection_isolation_forest/kafkaOffset.txt")
			offset=str(int(f.readlines()[0].strip())+1)
		else:
			offset="0"
		df=df.option("startingOffsets", """{"netflow-anonymized-preprocessed":{"""+"\""+str(PARTITION)+"\""+""":"""+offset+"""}}""").load()
		newoffset=df.select(col("offset")).alias("offset").select("offset.*")
		newoffset=newoffset.agg(F.max("offset")).collect()[0]['max(offset)']
		print(df.count())
		print(newoffset,int(offset)+999)
		if newoffset==int(offset)+999 or newoffset is None:
			continue
		with open("Netflow_outlier_detection_isolation_forest/kafkaOffset.txt","w") as of:
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
		#df.show(60)
		#df2=df.toPandas()
		#ar=df2.to_csv(header=False,index=False).split('\n')
		#for v in ar:
			#if '16.0' in v:
				#print(v)
		#for c in df.columns:
			#df.filter(df.select(c) == "16.0").show()
		#df.select(col("*")).where(col("*").eq('16.0')).show()
		#df=df.dropna()
		#df=df.limit(1000)
		runPipeline(df,N,offset)


