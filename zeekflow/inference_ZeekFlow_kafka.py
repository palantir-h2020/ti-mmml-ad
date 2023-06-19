import tensorflow as tf
tf.keras.utils.set_random_seed(1)
import pandas as pd
import numpy as np
import pickle
import shutil
from kafka import KafkaConsumer
import os
from scipy.stats import wasserstein_distance
from sklearn.metrics import average_precision_score
from sklearn.metrics import PrecisionRecallDisplay,RocCurveDisplay
from sklearn.metrics import roc_auc_score
import tensorflow.keras as keras
from time import mktime
from tensorflow.keras import layers
import tensorflow.keras.backend as K
import nltk
from tensorflow.keras.layers import Input, Dense, Reshape, Flatten, Dropout
from tensorflow.keras.optimizers import Adam
from datetime import datetime
import matplotlib.pyplot as plt
from keras.layers import TimeDistributed, LSTM, RepeatVector
import threading
import plotille
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import StringIndexer,VectorIndexer,IndexToString,StringIndexerModel
from pyspark.ml.feature import StandardScaler,VectorAssembler
from pyspark.sql.window import Window
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.sql import functions as F
import math
from pyspark.sql.functions import monotonically_increasing_id,regexp_replace
from pyspark.ml import Pipeline,PipelineModel
from pyspark.sql.types import *
import requests
import ipaddress
from tqdm import tqdm
from kafka import KafkaProducer
spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
prev=[]
TENANT_ID = os.environ['TENANT_ID']
PARTITION=requests.get('http://tenant-api-service.ti-dcp:6000/api/partition/'+str(TENANT_ID)).json()['partition']
KAFKA_IP=os.environ['KAFKA_BROKERS_CSV']
def ZeekFlowRetrain(x_train,benign_test_split,x_test,labels,latentScale=0.5,lr=0.000003,eval_steps=3000):
	full_model=tf.keras.models.load_model("NETFLOW_full_model")
	full_model.trainable=True
	###########################TRAIN#############################
	def get_data_generator(data, batch_size=32,testing=False,returnMalLabels=False,returnBenLabels=False):
		datalen = data.shape[0]#.value
		cnt = 0
		data=data.copy()
		if returnMalLabels:
			labs=labels[np.where(labels>0)].copy()
		if returnBenLabels:
			labs=labels[np.where(labels<=0)].copy()
			#print(labs.shape,datalen,np.unique(labs, return_counts=True))
		while True:
			idxes = np.arange(datalen)
			np.random.shuffle(idxes)
			cnt += 1
			for i in range(int(np.ceil(datalen/batch_size))):
				if returnMalLabels or returnBenLabels:
					l=np.take(labs, idxes[i*batch_size: (i+1) * batch_size], axis=0)
					#print('l',l)
				train_x = np.take(data, idxes[i*batch_size: (i+1) * batch_size], axis=0)
				history_x=train_x[:,-1]
				train_x=train_x[:,0:-1]
				y = np.ones(train_x.shape[0])
				if train_x.shape[0]<batch_size:
					if not testing:
						continue
					else:
						return
				if returnMalLabels or returnBenLabels:
					yield train_x.astype(np.float32),np.moveaxis(np.stack(history_x,axis=1),1,0),[y, y, y],l
				else:
					yield train_x.astype(np.float32),np.moveaxis(np.stack(history_x,axis=1),1,0),[y, y, y]

	print(x_train.shape)
	print(benign_test_split.shape)
	print(x_test.shape)
	niter = 500000
	batchsize = 32

	train_data_generator = get_data_generator(x_train, batchsize)

	losses=[]
	losses_zeek=[]
	losses_netflow=[]
	precision=[]
	recall=[]
	cutoffrange=[0.1]
	bestrecalls=0
	bestprecisions=0
	bestaucs=0
	aucs=[]
	modality_k="NETFLOW"
	
	for i in tqdm(range(niter)):
		x, x_h,y = train_data_generator.__next__()      
		#print('x',x.shape)
		#print('x_h',x_h.shape)
		full_model.trainable=True
		loss = full_model.train_on_batch([x_h,x], [x_h,x])
		#print(full_model.metrics_names)
		if i % eval_steps == 0 and i!=0:
			full_model.trainable=False
			print(f'========niter: {i}========\n loss: {loss[0]}')
			losses.append(loss[0])
			losses_zeek.append(loss[1])
			losses_netflow.append(loss[2])
			malicious_test_data_generator = get_data_generator(x_test, batchsize,True,returnMalLabels=True)
			benign_test_data_generator = get_data_generator(benign_test_split, batchsize,True,returnBenLabels=True)
			outlierpreds_netflow=np.array([])
			normalpreds_netflow=np.array([])

			outlierpreds_labels=np.array([])
			normalpreds_labels=np.array([])
			for genidx,gen in enumerate([malicious_test_data_generator,benign_test_data_generator]):
				try:
					while True:
						if genidx==0:
							x_t, hist_t,_,mal_label = gen.__next__()
							p = full_model.predict([hist_t,x_t],verbose=0,steps=1)
							outlierpreds_netflow=np.append(outlierpreds_netflow,
															np.sum(np.absolute(p[1]-x_t),
																	axis=-1))
							outlierpreds_labels=np.append(outlierpreds_labels,mal_label)
						else:
							x_t, hist_t,_,benign_label = gen.__next__()
							p = full_model.predict([hist_t,x_t],verbose=0,steps=1)
							normalpreds_netflow=np.append(normalpreds_netflow,
															np.sum(np.absolute(p[1]-x_t),
																	axis=-1))
							normalpreds_labels=np.append(normalpreds_labels,benign_label)
				except StopIteration:
					pass
			outlierpreds=outlierpreds_netflow
			normalpreds=normalpreds_netflow

			print(modality_k+' - median out: ',np.median(outlierpreds),
				'avg out: ',np.average(outlierpreds),
				'median normal: ',np.median(normalpreds),
				'avg normal: ',np.average(normalpreds))
			print(modality_k+' mean ratio:',np.average(outlierpreds)/np.average(normalpreds))
			print(modality_k+' wasserstein_distance:',wasserstein_distance(outlierpreds,normalpreds))

			sorted_normalpreds=np.sort(normalpreds)[::-1]
			cutoffidxes=[sorted_normalpreds[int(np.floor(len(sorted_normalpreds)*(r/100))-1)] for r in cutoffrange]
			benign_outputs=normalpreds
			malicious_outputs=outlierpreds
			all_outputs=np.concatenate((benign_outputs,malicious_outputs))
			all_gt=np.concatenate((np.zeros(len(benign_outputs)),np.ones(len(malicious_outputs))))
			ap=average_precision_score(all_gt, all_outputs)
			auc=roc_auc_score(all_gt, all_outputs)
			aucs.append(auc)
			print(modality_k+' average precision:',ap)
			print(modality_k+' AUC:',auc)
			if auc>bestaucs:
				full_model.save(modality_k+'_full_modelNEW')
				print('Saved best '+modality_k+' model at auc:',auc)
				bestaucs=auc
				with open('/zeekflow/cutoff.pickle', 'wb') as handle:
					pickle.dump(cutoffidxes[j], handle)
			for j in range(len(cutoffidxes)):
				truepos=0
				falsepos=0
				truenegatives=0
				falsenegatives=0
				boundary=cutoffidxes[j]
				for i in range(len(benign_outputs)):
					if benign_outputs[i]>=boundary:
						falsepos+=1
					else:
						truenegatives+=1
				for i in range(len(malicious_outputs)):
					if malicious_outputs[i]>=boundary:
						truepos+=1
					else:
						falsenegatives+=1
				if truepos/len(malicious_outputs)>bestrecalls:
					bestrecalls=truepos/len(malicious_outputs)
				if truepos/(truepos+falsepos)>bestprecisions:
					bestprecisions=truepos/(truepos+falsepos)
				print('% of normal wrong: ',cutoffrange[j],'/// boundary: ',round(boundary,4),'/// recall: ',round(truepos/len(malicious_outputs),4),'( best: ',round(bestrecalls,4),') /// precision: ',round(truepos/(truepos+falsepos),4),'( best: ',round(bestprecisions,4),') /// TP:',truepos,'FP:',falsepos,'FN:',falsenegatives,'TN:',truenegatives) # % of normal transactions wrong
				if j==0:
					precision.append(truepos/(truepos+falsepos))
					recall.append(truepos/len(malicious_outputs))
	
	shutil.move(modality_k+'_full_modelNEW',modality_k+"_full_model")
def FeatureExtractor(df,features):#,time_windows):
    dff=df.copy()
    dff['datetime']=pd.to_datetime(dff['ts'],unit = 's')
    dff.index=dff['datetime']
    dff=dff.groupby('sa')
    for i in features:
        #for j in time_windows:
        print(i)#,j)
        tmp_mean=dff[i].rolling('10min',min_periods=1).mean().reset_index()[i]
        tmp_std=dff[i].rolling('10min',min_periods=1).std().fillna(0).reset_index()[i]
        tmp_mean.index=df.index
        tmp_std.index=df.index
        df[f'{i}_mean'] = tmp_mean
        df[f'{i}_std'] = tmp_std
    return df
def FeatureExtractorTest(df,features):#,time_windows):
    dff=df.copy()
    dff['datetime']=pd.to_datetime(dff['ts'],unit = 's')
    dff.index=dff['datetime']
    dff=dff.groupby('sa')
    for i in features:
        print(i)
        count=dff[i].rolling('10min',min_periods=1).apply(lambda x: len(np.unique(x))).reset_index()[i]
        count.index=df.index
        df[f'{i}_count'] = count
    return df
def partitioner(key_bytes, all_partitions, available_partitions):
	global PARTITION
	return PARTITION
def runPipeline(df_batch,N,offset):
	global prev
	if df_batch.count()==0:
		return
	df_orig=df_batch.toPandas()
	df=df_orig[["ts", "te", "td", "sa", "da", "sp", "dp", "pr", "flg", "fwd", "stos", "ipkt", "ibyt", "opkt", "obyt","zeek"]].copy()
	rows = df_orig.to_csv(header=False,index=False).split('\n')
	del df_orig
	print('shape',df.count())
	try:
		with open('/zeekflow/cutoff.pickle', 'rb') as handle:
			cutoff = pickle.load(handle)
	except:
		cutoff=17.0033
	modelPath='/zeekflow/NETFLOW_full_model'

	print(np.count_nonzero(df.isnull()))
	#df['zeek'] = df['zeek'].replace("$",0)
	indexNoZeek=df[df['zeek']=="$"].index
	df=df.drop(indexNoZeek)
	#df=df.dropna(axis=1)
	df=df.drop(['stos'],axis=1)
	df=df.drop(['fwd'],axis=1)
	print(df.columns[df.nunique() <= 1])
	#if 'pr' in df.columns[df.nunique() <= 1]:
		#dr=list(df.columns[df.nunique() <= 1])
		#dr.remove('pr')
		#df.drop(dr,axis=1,inplace=True)
	#else:
		#df.drop(df.columns[df.nunique() <= 1],axis=1,inplace=True)
	print(df.columns)
	print(df.head())
	df.td=pd.to_numeric(df.td)
	df.sp=pd.to_numeric(df.sp)
	print(df.dtypes)
	print(df.zeek.unique())
	print(df.pr.unique())
	print(df.flg.unique())
	with open('/zeekflow/benignflgs.pk', 'rb') as fin:
		benign_flgs=pickle.load( fin)

	diff=list(set(df.flg.unique()).difference(benign_flgs))
	print('diff',diff)
	for unknown_flg in diff:
		bestED=999
		best=""
		for i in benign_flgs:
			ED=nltk.edit_distance(unknown_flg,i)
			if ED<bestED:
				bestED=ED
				best=i
		df=df.replace({unknown_flg:best})
		print(unknown_flg, 'replaced with',best)

	df=df.drop(df[df.pr=='ICMP'].index)
	df=df.drop(df[df.pr=='IGMP'].index)
	df=df.drop(df[df.pr=='ICMP6'].index)
	with open('/zeekflow/flglabelencoder.pk', 'rb') as fin:
		labelencoder=pickle.load( fin)
	df['flg'] = labelencoder.transform(df['flg'])

	with open('/zeekflow/prlabelencoder.pk', 'rb') as fin:
		labelencoder=pickle.load( fin)
	df['pr'] = labelencoder.transform(df['pr'])

	#map ip addresses to ints
	srcips=df['sa'].values
	dstips=df['da'].values
	ipv6map={}
	ipv6cnt=0
	for i in range(len(srcips)):
		try:
			srcips[i]=int(ipaddress.IPv4Address(srcips[i]))
		except Exception as e:
			if srcips[i] in ipv6map.keys():
				srcips[i]=ipv6map[srcips[i]]
			else:
				ivp6map[srcips[i]]=ipv6cnt
				srcips[i]=ipv6cnt
				ipv6cnt+=1
		try:
			dstips[i]=int(ipaddress.IPv4Address(dstips[i]))
		except Exception as e:
			if dstips[i] in ipv6map.keys():
				dstips[i]=ipv6map[dstips[i]]
			else:
				ivp6map[dstips[i]]=ipv6cnt
				dstips[i]=ipv6cnt
				ipv6cnt+=1
	df.loc[:,'sa']=srcips.astype(float)
	df.loc[:,'da']=dstips.astype(float)

	#fix hexademical ports to decimal
	sport=df['sp'].values
	dport=df['dp'].values
	for i in range(len(sport)):
		try:
			sport[i]=int(sport[i])
		except:
			sport[i]=int(sport[i],16)
		try:
			dport[i]=int(dport[i])
		except:
			dport[i]=int(dport[i],16)
	df.loc[:,'sp']=sport.astype(float)
	df.loc[:,'dp']=dport.astype(float)



	df['ts'] =df['ts'].apply(lambda x: mktime(datetime.strptime(x,'%Y-%m-%d %H:%M:%S').timetuple()))
	df['te'] =df['te'].apply(lambda x: mktime(datetime.strptime(x,'%Y-%m-%d %H:%M:%S').timetuple()))
	##################MASSIVE PROCESSING AHEAD############################
	#########SLIDING WINDOW TIME SERIES DATASET AUGMENTATION##############
	##############LOAD DATASET INSTEAD OF RUNNING THIS####################

	print('hasnan1',df.isnull().values.any())
	df=df.sort_values(by=['sa','ts']).reset_index(drop=True)
	features = [ 'sp', 'dp', 'da']
	df=FeatureExtractorTest(df, features)
	if 'pr' in df.columns:
		features = [ 'td', 'pr', 'flg','ipkt','ibyt']
	else:
		if 'flg' in df.columns:
			features = [ 'td', 'flg','ipkt','ibyt']
		else:
			features = [ 'td','ipkt','ibyt']
	df=FeatureExtractor(df, features)#, time_windows)
	print('hasnan2',df.isnull().values.any())
	print('hasnan3',df.isnull().values.any())
	df=df.drop(['ts','te','sa','da','dp','sp'],axis=1) #
	
	####history oh######
	one_hot_vocabulary = ['s', 'h', 'a', 'd', 'f', 'r', 'c', 'g', 't', 'w', 'i', 'q', 'S', 'H', 'A', 'D', 'F', 'R', 'C', 'G', 'T', 'W', 'I', 'Q', '-', '^']
	ohidxes=np.eye(len(one_hot_vocabulary))
	ohDict={one_hot_vocabulary[idx]:key for idx,key in enumerate(ohidxes)}
	history_arrays = []
	padding_length = 23
	for history in tqdm(df['zeek']):
		if type(history)==int:
			len_hist=0
			history=""
		else:
			len_hist=len(history)
		i = padding_length - len_hist
		padded_history = history + i * '-'
		padded_history_arr = [ohDict[char] for char in padded_history]
		history_arrays.append(padded_history_arr)
	history_arrays = np.array(history_arrays)
	#df['history_oh']=history_arrays.tolist()
	#df['history_oh']=df['history_oh'].apply(np.array)
	histories=history_arrays #df['history_oh']
	df=df.drop(['zeek'],axis=1)
	#df=df.drop(['history_oh','zeek'],axis=1)
	#df=df.drop(['history'],axis=1)
	#####################
	cols=df.columns
	print(cols)
	print('hasnan4',df.isnull().values.any())
	with open('/zeekflow/standardscaler.pk', 'rb') as fin:
		scaler=pickle.load( fin)
	df=scaler.transform(df)

	df = pd.DataFrame(df, columns=cols)
	#df['history_oh']=histories
	#df['history_oh'] = df['history_oh'].apply(lambda x: np.array(x), 0)
    ###############################################
    ###############################################
	netflow=df.to_numpy() #.loc[:, df.columns != 'history_oh']
	zeek=histories #df.loc[:,'history_oh']
	full_model=tf.keras.models.load_model(modelPath)
	full_model.trainable=False
	p = full_model.predict([zeek,netflow],verbose=0,steps=1)
	preds=np.sum(np.absolute(p[1]-netflow),axis=-1)
	fig=plotille.Figure()
	fig.histogram(preds)
	#print(fig.show())
	#outliers=preds[np.where(preds>=cutoff)]
	#benign=preds[np.where(preds<cutoff)]
	###
	producer = KafkaProducer(bootstrap_servers=[KAFKA_IP],value_serializer=str.encode,partitioner=partitioner)
	#with open("/zeekflow/kafkaOffset.txt","w") as of:
		#of.write(str(int(offset)+len(rows)-N))
	for idx,row in enumerate(rows):
		try:
			if float(preds[idx])>cutoff:
				print("OUTLIER:",preds[idx],'IDX:',idx)
		except:
			pass
		while len(prev)>N:
			del prev[0]
		if idx<=N:
			continue
		else:
			try:
				prev.append(row)
				isAnomalous="1" if float(preds[idx])>cutoff else "0"
				row=row+',ZEEKFLOW,"[]",'+str(preds[idx])+','+isAnomalous
				if isAnomalous=="1":
					print(idx,row)
				producer.send('netflow-ad-tba', value=row)
			except:
				pass 
	print(fig.show())
def ZeekFlow():
	global PARTITION
	offset="0"
	N=1000
	while True:
		df = spark.read.format("kafka").option("kafka.bootstrap.servers", KAFKA_IP).option("assign", """{"netflow-anonymized-preprocessed":["""+str(PARTITION)+"""]}""").option("failOnDataLoss","false")
		if os.path.exists("/zeekflow/kafkaOffset.txt"):
			f=open("/zeekflow/kafkaOffset.txt")
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
		with open("/zeekflow/kafkaOffset.txt","w") as of:
			if int(newoffset)-N<0:
				of.write("0")
			else:
				of.write(str(int(newoffset)-N))
		df=df.select(col("value").cast("string")).alias("csv").select("csv.*")
		#cols=["ts","te","td","sa","da","sp","dp","pr","flg","stos","ipkt","ibyt"]
		cols=["ts", "te", "td", "sa", "da", "sp", "dp", "pr", "flg", "fwd", "stos", "ipkt", "ibyt", "opkt", "obyt", "in", "out", "sas", "das", "smk", "dmk", "dtos", "dir", "nh", "nhb", "svln", "dvln", "ismc", "odmc", "idmc", "osmc", "mpls1", "mpls2", "mpls3", "mpls4", "mpls5", "mpls6", "mpls7", "mpls8", "mpls9", "mpls10", "cl", "sl", "al", "ra", "eng", "exid", "tr", "zeek", "pktips", "pktops", "bytips", "bytops", "bytippkt", "bytoppkt", "bytipo", "pktipo", "tpkt", "tbyt", "cp", "prtcp", "prudp", "pricmp", "prigmp", "prother", "flga", "flgs", "flgf", "flgr", "flgp", "flgu"]
		colargs=[]
		for i,column in enumerate(cols):
			colargs.append("split(value,',')["+str(i)+"] as "+cols[i])
		df=df.selectExpr(*colargs)
		#df=df.select('ts', 'te', 'td', 'sa', 'da', 'sp', 'dp', 'pr', 'flg', 'stos', 'ipkt','ibyt','zeek')
		df=df.select("ts", "te", "td", "sa", "da", "sp", "dp", "pr", "flg", "fwd", "stos", "ipkt", "ibyt", "opkt", "obyt", "in", "out", "sas", "das", "smk", "dmk", "dtos", "dir", "nh", "nhb", "svln", "dvln", "ismc", "odmc", "idmc", "osmc", "mpls1", "mpls2", "mpls3", "mpls4", "mpls5", "mpls6", "mpls7", "mpls8", "mpls9", "mpls10", "cl", "sl", "al", "ra", "eng", "exid", "tr", "zeek", "pktips", "pktops", "bytips", "bytops", "bytippkt", "bytoppkt", "bytipo", "pktipo", "tpkt", "tbyt", "cp", "prtcp", "prudp", "pricmp", "prigmp", "prother", "flga", "flgs", "flgf", "flgr", "flgp", "flgu")
		df=df.filter(df.pr!="IGMP")
		df=df.filter(df.pr!="ICMP")
		df=df.filter(df.pr!="ICMP6")
		df=df.filter(df.zeek!="$")
		runPipeline(df,N,offset)

if __name__ == "__main__":
	zf=threading.Thread(target=ZeekFlow)
	#zfr=threading.Thread(target=ZeekFlowRetrain)
	zf.start()
	#zfr.start()
