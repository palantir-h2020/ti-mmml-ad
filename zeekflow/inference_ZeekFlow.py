import tensorflow as tf
tf.keras.utils.set_random_seed(1)
import pandas as pd
import numpy as np
import pickle
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

def ZeekFlow(netflowPath,zeekPath):
	device="GPU:0"
	cutoffs={"NETFLOW":5.0033,"ZEEK":9.6715,"COMBINED":9.6251}
	modelPath='_full_model'
	flgs=pickle.load(open('benignflgs.pk', 'rb'))
	scaler=pickle.load(open('standardscaler.pk', 'rb'))
	flgencoder=pickle.load(open('flglabelencoder.pk', 'rb'))
	prencoder=pickle.load(open('prlabelencoder.pk', 'rb'))
	##################################################
	#########MERGE#############
	netflow=pd.read_csv(netflowPath)
	zeek_f=open(zeekPath,'r')
	zeek_lines = zeek_f.readlines()
	zeek=[]
	conn_order = ["ts", "uid", "id.orig_h", "id.orig_p", "id.resp_h", "id.resp_p", "proto", "service", "duration", "orig_bytes", "resp_bytes", "conn_state", "local_orig", "local_resp", "missed_bytes", "history", "orig_pkts", "orig_ip_bytes", "resp_pkts", "resp_ip_bytes",	"tunnel_parents"]
	for line in zeek_lines[8:-1]:
		details = line.split('	')
		details = [x.strip() for x in details]
		structure = {key: value for key, value in zip(conn_order, details)}
		zeek.append(structure)
	zeek = pd.DataFrame(zeek, columns=zeek[0].keys())
	zeek.duration=pd.to_numeric(zeek.duration,errors='coerce')
	zeek=zeek.dropna()
	netflow=netflow.dropna()
	netflow.dp=netflow.dp.astype(int)
	
	
	netflow.sa=netflow.sa.astype(str)
	netflow.da=netflow.da.astype(str)
	netflow.sp=netflow.sp.astype(int)
	netflow.dp=netflow.dp.astype(int)
	netflow.dp=netflow.dp.astype(int)
	netflow.ipkt=netflow.ipkt.astype(int)
	netflow.opkt=netflow.opkt.astype(int)

	zeek['id.orig_h']=zeek['id.orig_h'].astype(str)
	zeek['id.resp_h']=zeek['id.resp_h'].astype(str)
	zeek['id.orig_p']=zeek['id.orig_p'].astype(int)
	zeek['id.resp_p']=zeek['id.resp_p'].astype(int)
	zeek['orig_pkts']=zeek['orig_pkts'].astype(int)
	zeek['resp_pkts']=zeek['resp_pkts'].astype(int)
	netflow.sp=netflow.sp.astype(int)
	netflow.dp=netflow.dp.astype(int)
	netflow_to_zeek={"sa": "id.orig_h", "da": "id.resp_h","sp":"id.orig_p","dp":"id.resp_p"}
	netflow=netflow.rename(columns=netflow_to_zeek)
	common_cols = ['id.orig_h','id.resp_h','id.orig_p','id.resp_p']#,'orig_pkts','resp_pkts'] 
	zeek=zeek.drop_duplicates(subset=common_cols,keep='last')
	df12 = pd.merge(netflow, zeek, on=common_cols, how='left')     #extract common rows with merge
	df2 = zeek[~zeek['uid'].isin(df12['uid'])]
	df3 = netflow[~netflow['ts'].isin(df12['ts_x'])]
	zeek_to_netflow={v: k for k, v in netflow_to_zeek.items()}
	df12=df12.rename(columns=zeek_to_netflow)
	df12=df12.drop(['ts_y'],axis=1)
	
	df=df12.rename(columns={'ts_x':'ts'})
	########PRE#############
	df['history'] = df['history'].fillna(0)
	df=df.dropna(axis=1)
	df=df.drop(['stos'],axis=1)
	df.drop(df.columns[df.nunique() <= 1],axis=1,inplace=True)
	df.td=pd.to_numeric(df.td)
	df.sp=pd.to_numeric(df.sp)

	diff=list(set(df.flg.unique()).difference(flgs))
	for unknown_flg in diff:
		bestED=999
		best=""
		for i in flgs:
			ED=nltk.edit_distance(unknown_flg,i)
			if ED<bestED:
				bestED=ED
				best=i
		df=df.replace({unknown_flg:best})

	#df=df.drop(df[df.pr=='ICMP'].index)
	#df=df.drop(df[df.pr=='IGMP'].index)
	#df=df.replace({'ICMP': 'TCP'}, regex=True)
	#df['flg']='........'
	df['flg'] = flgencoder.transform(df['flg'])
	df['pr'] = prencoder.transform(df['pr'])

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
				ipv6map[srcips[i]]=ipv6cnt
				srcips[i]=ipv6cnt
				ipv6cnt+=1
		try:
			dstips[i]=int(ipaddress.IPv4Address(dstips[i]))
		except Exception as e:
			if dstips[i] in ipv6map.keys():
				dstips[i]=ipv6map[dstips[i]]
			else:
				ipv6map[dstips[i]]=ipv6cnt
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


	#df['ipkt']=5
	#df['ibyt']=900
	#df['opkt']=5
	#df['obyt']=2000
	df['ts'] =df['ts'].apply(lambda x: mktime(datetime.strptime(x,'%Y-%m-%d %H:%M:%S').timetuple()))
	df['te'] =df['te'].apply(lambda x: mktime(datetime.strptime(x,'%Y-%m-%d %H:%M:%S').timetuple()))
	##################MASSIVE PROCESSING AHEAD############################
	#########SLIDING WINDOW TIME SERIES DATASET AUGMENTATION##############
	##############LOAD DATASET INSTEAD OF RUNNING THIS####################

	df=df.sort_values(by=['sa','ts']).reset_index(drop=True)
	features = [ 'sp', 'dp', 'da']
	df=FeatureExtractorTest(df, features)
	features = [ 'td', 'pr', 'flg','ipkt','ibyt']
	df=FeatureExtractor(df, features)#, time_windows)
	df=df.drop(['ts','te','sa','da','dp','sp'],axis=1) #
	
	####history oh######
	one_hot_vocabulary = ['s', 'h', 'a', 'd', 'f', 'r', 'c', 'g', 't', 'w', 'i', 'q', 'S', 'H', 'A', 'D', 'F', 'R', 'C', 'G', 'T', 'W', 'I', 'Q', '-', '^']
	ohidxes=np.eye(len(one_hot_vocabulary))
	ohDict={one_hot_vocabulary[idx]:key for idx,key in enumerate(ohidxes)}
	history_arrays = []
	padding_length = 23
	for history in df['history']:
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
	df=df.drop(['history'],axis=1)
	#####################
	colsorig=df.columns
	dforig=df.copy()#.values.copy()
	df=scaler.transform(df)


	history=history_arrays
	x=df
	#####################################
	with tf.device(device):
		batchsize = 512
		for modality_k in ["NETFLOW","ZEEK","COMBINED"]:
			full_model=tf.keras.models.load_model(modality_k+modelPath)
			full_model.trainable=False
			p = full_model.predict([history,x],verbose=0,steps=1)
			if modality_k=="NETFLOW":
				preds=np.sum(np.absolute(p[1]-x),axis=-1)
			if modality_k=="ZEEK":
				preds=np.sum(np.sum(np.absolute(p[0]-history),axis=-1),axis=-1)
			if modality_k=="COMBINED":
				preds_netflow=np.sum(np.absolute(p[1]-x),axis=-1)
				preds_zeek=np.sum(np.sum(np.absolute(p[0]-history),axis=-1),axis=-1)
				avg_zeek=np.average(preds_zeek)
				avg_netflow=np.average(preds_netflow)
				zeek_netflow_ratio=avg_zeek/avg_netflow
				preds=preds_zeek+preds_netflow*zeek_netflow_ratio
			fig=plt.figure()
			ax=fig.add_axes([0,0,1,1])
			outliers=preds[np.where(preds>=cutoffs[modality_k])]
			benign=preds[np.where(preds<cutoffs[modality_k])]
			s=ax.scatter(np.arange(len(benign)), benign, color='b',alpha=0.5,label='benign')
			s=ax.scatter(np.arange(len(benign),len(outliers)+len(benign)), outliers, color='r',alpha=0.5,label='outliers')
			s=ax.set_xlabel('Sample')
			s=ax.set_ylabel('Prediction')
			s=ax.set_title(modality_k+' scatter plot')
			s=plt.yscale('log')
			s=plt.legend(loc='upper left',framealpha=0.3)
			s=plt.show()
			if False:
				for col in colsorig:
					print(col)
					currcol=dforig[col]
					outl=currcol.loc[np.where(preds>=cutoffs[modality_k])]
					normal=currcol.loc[np.where(preds<cutoffs[modality_k])]
					print('median out: ',np.median(outl),'avg out: ',np.average(outl),'median normal: ',np.median(normal),'avg normal: ',np.average(normal))
					#print('arange',currcol.min(),currcol.max())
					#if currcol.min()==currcol.max():
						#continue
					plt.hist((outl,normal),np.arange(currcol.min(),currcol.max(),(currcol.max()-currcol.min())/10),density=True,label=['outliers','normal'])
					plt.legend()
					plt.show()
					fig=plt.figure()
					ax=fig.add_axes([0,0,1,1])
					ax.scatter(np.arange(len(normal)), normal, color='b',alpha=0.5)
					ax.scatter(np.arange(len(outl)), outl, color='r',alpha=0.5)
					ax.set_xlabel('Sample')
					ax.set_ylabel('Value')
					plt.yscale('log')
					ax.set_title('scatter plot')
					plt.show()
ZeekFlow('pcaps/benign.csv','pcaps/benign/conn.log')
ZeekFlow('pcaps/nmap.csv','pcaps/nmap/conn.log')
ZeekFlow('pcaps/dirb_yt.csv','pcaps/dirb_yt/conn.log')
