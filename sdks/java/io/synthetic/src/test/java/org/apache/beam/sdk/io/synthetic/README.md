# Transform integration tests

## GroupByKeyLoadIT

### Direct

```
./gradlew clean integrationTest -p sdks/java/io/synthetic/ -DintegrationTestRunner=direct -DintegrationTestPipelineOptions='["--inputOptions={\"numRecords\" : 1000, \"splitPointFrequencyRecords\" : 1, \"keySizeBytes\" : 10, \"valueSizeBytes\" : 20, \"numHotKeys\" : 3, \"hotKeyFraction\" : 0.3, \"seed\" : 123456, \"bundleSizeDistribution\" : {\"type\":\"const\", \"const\" : 42}, \"forceNumInitialBundles\" : 100, \"progressShape\" : \"LINEAR\", \"initializeDelayDistribution\" : {\"type\":\"const\", \"const\" : 42}}"]' --tests org.apache.beam.sdk.io.synthetic.GroupByKeyLoadIT
```


### Dataflow

```
./gradlew clean integrationTest -p sdks/java/io/synthetic/ -DintegrationTestRunner=dataflow -DintegrationTestPipelineOptions='["--project=apache-beam-io-testing", "--tempRoot=gs://apache-beam-io-testing", "--runner=TestDataflowRunner", "--inputOptions={\"numRecords\" : 1000, \"splitPointFrequencyRecords\" : 1, \"keySizeBytes\" : 10, \"valueSizeBytes\" : 20, \"numHotKeys\" : 3, \"hotKeyFraction\" : 0.3, \"seed\" : 123456, \"bundleSizeDistribution\" : {\"type\":\"const\", \"const\" : 42}, \"forceNumInitialBundles\" : 100, \"progressShape\" : \"LINEAR\", \"initializeDelayDistribution\" : {\"type\":\"const\", \"const\" : 42}}"]' --tests org.apache.beam.sdk.io.synthetic.GroupByKeyLoadIT
```


## CombineLoadIT

### Direct

```
./gradlew clean integrationTest -p sdks/java/io/synthetic/ -DintegrationTestRunner=direct -DintegrationTestPipelineOptions='["--inputOptions={\"numRecords\" : 1000, \"splitPointFrequencyRecords\" : 1, \"keySizeBytes\" : 10, \"valueSizeBytes\" : 20, \"numHotKeys\" : 3, \"hotKeyFraction\" : 0.3, \"seed\" : 123456, \"bundleSizeDistribution\" : {\"type\":\"const\", \"const\" : 42}, \"forceNumInitialBundles\" : 100, \"progressShape\" : \"LINEAR\", \"initializeDelayDistribution\" : {\"type\":\"const\", \"const\" : 42}}"]' --tests org.apache.beam.sdk.io.synthetic.CombineLoadIT
```


### Dataflow

```
./gradlew clean integrationTest -p sdks/java/io/synthetic/ -DintegrationTestRunner=dataflow -DintegrationTestPipelineOptions='["--project=apache-beam-io-testing", "--tempRoot=gs://apache-beam-io-testing", "--runner=TestDataflowRunner", "--inputOptions={\"numRecords\" : 1000, \"splitPointFrequencyRecords\" : 1, \"keySizeBytes\" : 10, \"valueSizeBytes\" : 20, \"numHotKeys\" : 3, \"hotKeyFraction\" : 0.3, \"seed\" : 123456, \"bundleSizeDistribution\" : {\"type\":\"const\", \"const\" : 42}, \"forceNumInitialBundles\" : 100, \"progressShape\" : \"LINEAR\", \"initializeDelayDistribution\" : {\"type\":\"const\", \"const\" : 42}}"]' --tests org.apache.beam.sdk.io.synthetic.CombineLoadIT
```


## CoGroupByKeyLoadIT

### Direct

```
./gradlew clean integrationTest -p sdks/java/io/synthetic/ -DintegrationTestRunner=direct -DintegrationTestPipelineOptions='["--inputOptions={\"numRecords\" : 1000, \"splitPointFrequencyRecords\" : 1, \"keySizeBytes\" : 10, \"valueSizeBytes\" : 20, \"numHotKeys\" : 3, \"hotKeyFraction\" : 0.3, \"seed\" : 123456, \"bundleSizeDistribution\" : {\"type\":\"const\", \"const\" : 42}, \"forceNumInitialBundles\" : 100, \"progressShape\" : \"LINEAR\", \"initializeDelayDistribution\" : {\"type\":\"const\", \"const\" : 42}}", "--coInputOptions={\"numRecords\" : 1000, \"splitPointFrequencyRecords\" : 1, \"keySizeBytes\" : 10, \"valueSizeBytes\" : 20, \"numHotKeys\" : 3, \"hotKeyFraction\" : 0.3, \"seed\" : 123456, \"bundleSizeDistribution\" : {\"type\":\"const\", \"const\" : 42}, \"forceNumInitialBundles\" : 100, \"progressShape\" : \"LINEAR\", \"initializeDelayDistribution\" : {\"type\":\"const\", \"const\" : 42}}"]' --tests org.apache.beam.sdk.io.synthetic.CoGroupByKeyLoadIT
```


### Dataflow

```
./gradlew clean integrationTest -p sdks/java/io/synthetic/ -DintegrationTestRunner=dataflow -DintegrationTestPipelineOptions='["--project=apache-beam-io-testing", "--tempRoot=gs://apache-beam-io-testing", "--runner=TestDataflowRunner", "--inputOptions={\"numRecords\" : 1000, \"splitPointFrequencyRecords\" : 1, \"keySizeBytes\" : 10, \"valueSizeBytes\" : 20, \"numHotKeys\" : 3, \"hotKeyFraction\" : 0.3, \"seed\" : 123456, \"bundleSizeDistribution\" : {\"type\":\"const\", \"const\" : 42}, \"forceNumInitialBundles\" : 100, \"progressShape\" : \"LINEAR\", \"initializeDelayDistribution\" : {\"type\":\"const\", \"const\" : 42}}", "--coInputOptions={\"numRecords\" : 1000, \"splitPointFrequencyRecords\" : 1, \"keySizeBytes\" : 10, \"valueSizeBytes\" : 20, \"numHotKeys\" : 3, \"hotKeyFraction\" : 0.3, \"seed\" : 123456, \"bundleSizeDistribution\" : {\"type\":\"const\", \"const\" : 42}, \"forceNumInitialBundles\" : 100, \"progressShape\" : \"LINEAR\", \"initializeDelayDistribution\" : {\"type\":\"const\", \"const\" : 42}}"]' --tests org.apache.beam.sdk.io.synthetic.CoGroupByKeyLoadIT
```


## SideInputLoadIT

### Direct

```
./gradlew clean integrationTest -p sdks/java/io/synthetic/ -DintegrationTestRunner=direct -DintegrationTestPipelineOptions='["--inputOptions={\"numRecords\" : 1000, \"splitPointFrequencyRecords\" : 1, \"keySizeBytes\" : 10, \"valueSizeBytes\" : 20, \"numHotKeys\" : 3, \"hotKeyFraction\" : 0.3, \"seed\" : 123456, \"bundleSizeDistribution\" : {\"type\":\"const\", \"const\" : 42}, \"forceNumInitialBundles\" : 100, \"progressShape\" : \"LINEAR\", \"initializeDelayDistribution\" : {\"type\":\"const\", \"const\" : 42}}", "--sideInputOptions={\"numRecords\" : 1000, \"splitPointFrequencyRecords\" : 1, \"keySizeBytes\" : 10, \"valueSizeBytes\" : 20, \"numHotKeys\" : 3, \"hotKeyFraction\" : 0.3, \"seed\" : 123456, \"bundleSizeDistribution\" : {\"type\":\"const\", \"const\" : 42}, \"forceNumInitialBundles\" : 100, \"progressShape\" : \"LINEAR\", \"initializeDelayDistribution\" : {\"type\":\"const\", \"const\" : 42}}", "--sideInputType=MAP"]' --tests org.apache.beam.sdk.io.synthetic.SideInputLoadIT
```


### Dataflow

```
./gradlew clean integrationTest -p sdks/java/io/synthetic/ -DintegrationTestRunner=dataflow -DintegrationTestPipelineOptions='["--project=apache-beam-io-testing", "--tempRoot=gs://apache-beam-io-testing", "--runner=TestDataflowRunner", "--inputOptions={\"numRecords\" : 1000, \"splitPointFrequencyRecords\" : 1, \"keySizeBytes\" : 10, \"valueSizeBytes\" : 20, \"numHotKeys\" : 3, \"hotKeyFraction\" : 0.3, \"seed\" : 123456, \"bundleSizeDistribution\" : {\"type\":\"const\", \"const\" : 42}, \"forceNumInitialBundles\" : 100, \"progressShape\" : \"LINEAR\", \"initializeDelayDistribution\" : {\"type\":\"const\", \"const\" : 42}}", "--sideInputOptions={\"numRecords\" : 1000, \"splitPointFrequencyRecords\" : 1, \"keySizeBytes\" : 10, \"valueSizeBytes\" : 20, \"numHotKeys\" : 3, \"hotKeyFraction\" : 0.3, \"seed\" : 123456, \"bundleSizeDistribution\" : {\"type\":\"const\", \"const\" : 42}, \"forceNumInitialBundles\" : 100, \"progressShape\" : \"LINEAR\", \"initializeDelayDistribution\" : {\"type\":\"const\", \"const\" : 42}}", "--sideInputType=MAP"]' --tests org.apache.beam.sdk.io.synthetic.SideInputLoadIT
```
