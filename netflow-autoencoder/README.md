This is a repository containing the code for Netflow traffic outlier detection using the autoencoder deep learning architecture. This code was written as part of the Palantir European Project (H2020).

Requires spark 3.0
Requires elephas implementation in this repo (install with setup.py)
Requires kafka stream with csv formatted samples in column order:
```
["ts","te","td","sa","da","sp","dp","pr","flg","stos","ipkt","ibyt","label"]
```

Run script with
```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 --driver-memory 15g "autoencoder_distributed_netflow_inference(Outlier).py"
```
