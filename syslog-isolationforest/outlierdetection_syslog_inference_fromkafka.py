from pyparsing import Word, alphas, Suppress, Combine, nums, string, Regex, Optional
import pandas as pd 
import argparse
from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np
import matplotlib.pyplot as plt
import time
from sklearn.preprocessing import StandardScaler
from datetime import datetime
from time import mktime
from sklearn.ensemble import IsolationForest
import pickle
import collections
import warnings
from sklearn.ensemble import RandomForestClassifier
import json
import os.path
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sklearn.preprocessing import LabelEncoder
warnings.filterwarnings("ignore")

	
class Parser(object):
    # log lines don't include the year, but if we don't provide one, datetime.strptime will assume 1900
    ASSUMED_YEAR = '2020'

    def __init__(self):
        ints = Word(nums)

        # priority
       # priority = Suppress("<") + ints + Suppress(">")

        # timestamp
        month = Word(string.ascii_uppercase, string.ascii_lowercase, exact=3)
        day   = ints
        hour  = Combine(ints + ":" + ints + ":" + ints)

        timestamp = month + day + hour
        # a parse action will convert this timestamp to a datetime
        timestamp.setParseAction(lambda t: datetime.strptime(Parser.ASSUMED_YEAR + ' ' + ' '.join(t), '%Y %b %d %H:%M:%S'))

        # hostname
        hostname = Word(alphas + nums + "_-.")

        # appname
        appname = Word(alphas + "/-_.()")("appname") + (Suppress("[") + ints("pid") + Suppress("]")) | (Word(alphas + "/-_.")("appname"))
        appname.setName("appname")

        # message
        message = Regex(".*")

        # pattern build
        # (add results names to make it easier to access parsed fields)
        self._pattern = timestamp("timestamp") + hostname("hostname") + Optional(appname) + Suppress(':') + message("message")

    def parse(self, line):
        parsed = self._pattern.parseString(line)
        # fill in keys that might not have been found in the input string
        # (this could have been done in a parse action too, then this method would
        # have just been a two-liner)
        for key in 'appname pid'.split():
            if key not in parsed:
                parsed[key] = ''
        return parsed.asDict()


def FeatureExtractor(df,features,time_windows):
	dff=df.copy()
	dff['datetime']=pd.to_datetime(dff['timestamp'],unit = 's')
	dff.index=dff['datetime']
	for i in features:
		#print(i)
		for j in time_windows:
			tmp_mean=dff[i].rolling(j,min_periods=1).mean().reset_index()[i]
			tmp_std=dff[i].rolling(j,min_periods=1).std().fillna(0).reset_index()[i]
			tmp_mean.index=df.index
			tmp_std.index=df.index
			df[f'{i}_mean_{j}'] = tmp_mean
			df[f'{i}_std_{j}'] = tmp_std
		
if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Process some integers.')
	parser.add_argument('--outliermodel', type=str, help='Outlier Model file.',default='isolationforest.pkl')
	parser.add_argument('--vectorizerfile', type=str, help='Vectorizer file.',default='vectorizer_uncapped.pk')
	parser.add_argument('--cutoff', type=float, help='Cutoff point.',default=-0.45)
	parser.add_argument('--reload', dest='reload', action='store_true')
	parser.add_argument('--standardscalerfile', type=str, help='Standard scaler file.',default='standardscaler.pk') 
	parser.add_argument('--no-reload', dest='reload', action='store_false')
	parser.set_defaults(reload=False)
	parser.add_argument('--parse', dest='parse', action='store_true')
	parser.add_argument('--no-parse', dest='parse', action='store_false')
	parser.set_defaults(parse=True)
	#parser.add_argument('inputlabels', action='store', type=str, help='Input labels.')
	args = parser.parse_args()
	
	spark = SparkSession.builder.getOrCreate()
	spark.sparkContext.setLogLevel('ERROR')
	N=400
	while True:
		df = spark \
		.read \
		.format("kafka") \
		.option("kafka.bootstrap.servers", "10.101.10.111:9092") \
		.option("subscribe", "filebeat-linux-syslog").option("failOnDataLoss","false")
		if os.path.exists("kafkaOffset.txt"):
			f=open("kafkaOffset.txt")
			offset=f.readlines()[0].strip()
		else:
			offset="0"
		df=df.option("startingOffsets", """{"filebeat-linux-syslog":{"0":"""+offset+"""}}""").load()
		newoffset=df.select(col("offset")).alias("offset").select("offset.*")
		newoffset=newoffset.agg(F.max("offset")).collect()[0]['max(offset)']
		with open("kafkaOffset.txt","w") as of:
			of.write(str(int(newoffset)-N))
		df=df.select(col("value").cast("string")).alias("csv").select("csv.*")
		print('df size',df.count())
		df=df.toPandas()
		rows = df.to_string(header=False,index=False,index_names=False).split('\n')
		for rowidx,row in enumerate(rows):
			rows[rowidx]=json.loads(row)["message"]
		parser=Parser()
		i=0
		if not args.reload:
			if args.parse:
				df = collections.OrderedDict()
				for line in rows:#file: 
					# print(t)
					try:
						dict_new = parser.parse(line)
						df[i] = dict_new#df.append(dict_new, ignore_index=True)
						i+=1
					except:
						pass #print("Parsing error in line:", line)
				df=pd.DataFrame.from_dict(df,"index")
				with open(args.vectorizerfile,'rb') as pkl_file:
					v = pickle.load(pkl_file)
				x = v.transform(df['message'])
				#print(len(x.toarray()[0]))
				#print(len(v.get_feature_names()))
				df1 = pd.DataFrame(x.toarray(), columns=v.get_feature_names())
				df.drop('message', axis=1, inplace=True)
				res = pd.concat([df, df1], axis=1)
				res.to_csv('parsed.csv',index=False)

			df=pd.read_csv('parsed.csv')
			df=df.drop(["pid","appname","hostname"],axis=1)
			print('rows with nulls:',np.count_nonzero(df.isnull()))
			df=df.dropna()
			print('Scaling')
			df['timestamp'] =df['timestamp'].apply(lambda x: mktime(datetime.strptime(x,'%Y-%m-%d %H:%M:%S').timetuple()))
			df=df.sort_values(by=['timestamp']).reset_index(drop=True)
			features=df.columns.tolist()
			features.remove("timestamp")
			print('Feature augmentation')
			durs=['10S','60S','10min']
			FeatureExtractor(df, features,durs)
			
			cols=df.columns
			with open(args.standardscalerfile,'rb') as pkl_file:
				scaler=pickle.load(pkl_file)
				df=scaler.transform(df)
			df = pd.DataFrame(df, columns=cols)
			df.to_csv('df.csv',index=False)
		else:
			df=pd.read_csv('df.csv')
		dataset=df.drop(['timestamp'],axis=1)
		
		print('Running Inference')
		with open(args.outliermodel,'rb') as pkl_file:
			isf = pickle.load(pkl_file)
		preds_isolation=isf.score_samples(dataset)
		cutoffindexes=np.where(preds_isolation<args.cutoff)
		outliers=dataset.iloc[cutoffindexes]
		print(outliers.columns)
		################TODO WRITE TO KAFKA STREAM################
