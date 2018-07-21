TPC-H Benchmark on Beam SQL

```bash
flink run beam-flink.jar --runner=FlinkRunner --inputFile=gs://beam-tpc-gsoc1/data/ --output=gs://beam-tpc-gsoc1/tpch3query --query=3 --table=DS
```
