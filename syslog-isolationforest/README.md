This is a repository containing the code for Syslog outlier detection using the isolation forest machine learning algorithm. This code was written as part of the Palantir European Project (H2020).

Requires spark 3.0+
Requires kafka stream with csv formatted samples in column order:
```
["message","label"]
```
Run script with
```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 --driver-memory 15g outlierdetection_syslog_inference_fromkafka.py
```
