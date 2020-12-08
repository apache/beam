
go run group_by_key.go --runner=flink --endpoint=localhost:8099 --parallelism 5 --iterations 4 --input_options='{"num_records": 20000000, "key_size": 10, "value_size": 90, "num_hot_keys": 200, "hot_key_fraction": 1}'
