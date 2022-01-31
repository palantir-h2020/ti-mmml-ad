This is a repository containing the code for Netflow traffic outlier detection using the isolation forest machine learning algorithm. This code was written as part of the Palantir European Project (H2020).

Requires spark 3.0
Requires spark-iforest implementation in this repo:
```
cd spark-iforest/

mvn clean package -DskipTests

cp target/spark-iforest-<version>.jar $SPARK_HOME/jars/

cd spark-iforest/python

python setup.py sdist

pip install dist/pyspark-iforest-<version>.tar.gz
```
Requires kafka stream with csv formatted samples in column order:
```
["ts","te","td","sa","da","sp","dp","pr","flg","stos","ipkt","ibyt","label"]
```

Run script with
```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 --driver-memory 15g "distributed_netflow_inference (outlier detection).py"
```
