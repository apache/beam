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
## Setting up the Expansion service
*Note: skip this step for Beam 2.44 and later.*

Because we can not add local packages in Beam 2.43 we must create our own expansion service.
Start up the expansion service with this command:

```bash
export PORT = <port to host expansion service>
export IMAGE = <custom docker image>

python -m multi_language_custom_transform.start_expansion_service  \
    --port=$PORT \
    --fully_qualified_name_glob="*" \
    --environment_config=$IMAGE \
    --environment_type=DOCKER
```
## Running the Java pipeline
Make sure you have Maven installed and added to PATH. Also make sure that JAVA_HOME
points to the correct Java version.

First we need to download the Maven arche-type for Beam. Run the following command:

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
implemented. The logic of the pipeline is the file `MultiLangRunInference.java`. After this is done,
run the following command to start the Java pipeline:

```bash
export GCP_PROJECT= <your gcp project>
export GCP_REGION= <region of bucket>
export GCP_BUCKET= <your gcp bucker>
export MODEL_NAME=bert-base-uncased
export PORT= <port to host expansion service>

mvn compile exec:java -Dexec.mainClass=org.MultiLangRunInference \
    -Dexec.args="--runner=DataflowRunner --project=$GCP_PROJECT\
                 --region=$GCP_REGION \
                 --gcpTempLocation=gs://$GCP_BUCKET/temp/ \
                 --inputFile=gs://$GCP_BUCKET/input/imdb_reviews.csv \
                 --outputFile=gs://$GCP_BUCKET/output/ouput.txt \
                 --modelPath=gs://$GCP_BUCKET/input/bert-model/bert-base-uncased.pth \
                 --modelName=$MODEL_NAME \
                 --port=$PORT" \
    -Pdataflow-runner \
    -e
```
Make sure to run this in the `last_word_prediction` directory. This will start the Java pipeline.

