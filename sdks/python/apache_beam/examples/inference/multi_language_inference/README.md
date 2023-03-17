<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->
For a detailed explanation of this inference example, visit the [documentation](https://beam.apache.org/documentation/ml/multi-language-inference/).
## Set up Python virtual environment
Make sure to set up a virtual environment for Python with all the required dependencies.
More details on how to do this can be found [here](https://beam.apache.org/get-started/quickstart-py/#set-up-your-environment).
## Running the Java pipeline
Make sure you have Maven installed and added to PATH. Also make sure that JAVA_HOME
points to the correct Java version.

First we need to download the Maven archetype for Beam. Run the following command:

```bash
export BEAM_VERSION=<Beam version>

mvn archetype:generate \
    -DarchetypeGroupId=org.apache.beam \
    -DarchetypeArtifactId=beam-sdks-java-maven-archetypes-examples \
    -DarchetypeVersion=$BEAM_VERSION \
    -DgroupId=org.example \
    -DartifactId=multi-language-beam \
    -Dversion="0.1" \
    -Dpackage=org.apache.beam.examples \
    -DinteractiveMode=false
```
This will set up all the required dependencies for the Java pipeline. Next the pipeline needs to be
implemented. The logic of this pipeline is written in the `MultiLangRunInference.java` file. After that,
run the following command to start the Java pipeline:

```bash
export GCP_PROJECT=<your gcp project>
export GCP_BUCKET=<your gcp bucker>
export GCP_REGION=<region of bucket>
export MODEL_NAME=bert-base-uncased
export LOCAL_PACKAGE=<path to tarball>

cd last_word_prediction
mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.MultiLangRunInference \
    -Dexec.args="--runner=DataflowRunner \
                 --project=$GCP_PROJECT\
                 --region=$GCP_REGION \
                 --gcpTempLocation=gs://$GCP_BUCKET/temp/ \
                 --inputFile=gs://$GCP_BUCKET/input/imdb_reviews.csv \
                 --outputFile=gs://$GCP_BUCKET/output/ouput.txt \
                 --modelPath=gs://$GCP_BUCKET/input/bert-model/bert-base-uncased.pth \
                 --modelName=$MODEL_NAME \
                 --localPackage=$LOCAL_PACKAGE" \
    -Pdataflow-runner
```

The `localPackage` argument is the path to a locally available package compiled as a tarball. This package must be created by the user and contain the python transforms used in the pipeline.
Make sure to run this in the [`last_word_prediction`](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/inference/multi_language_inference/last_word_prediction) directory. This will start the Java pipeline.

