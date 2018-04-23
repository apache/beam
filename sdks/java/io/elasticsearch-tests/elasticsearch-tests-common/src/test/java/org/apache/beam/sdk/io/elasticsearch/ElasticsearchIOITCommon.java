/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.elasticsearch;

import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.ConnectionConfiguration;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestCommon.ES_INDEX;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestCommon.ES_TYPE;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestCommon.NUM_DOCS_ITESTS;

import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.elasticsearch.client.RestClient;

/**
 * Manipulates test data used by the {@link ElasticsearchIO}
 * integration tests.
 *
 * <p>This is independent from the tests so that for read tests it can be run separately after data
 * store creation rather than every time (which can be more fragile.)
 */
public class ElasticsearchIOITCommon {

  private static final String writeIndex = ES_INDEX + System.currentTimeMillis();

  /**
   * Use this to create the index for reading before IT read tests.
   *
   * <p>To invoke this class, you can use this command line from elasticsearch-tests-common module
   * directory after setting the correct server IP:
   *
   * <pre>
   * mvn test-compile exec:java \
   * -Dexec.mainClass=org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOITCommon \
   * -Dexec.args="--elasticsearchServer=127.0.0.1 \
   * --elasticsearchHttpPort=9200" \
   * -Dexec.classpathScope=test
   *   </pre>
   *
   * @param args Please pass options from ElasticsearchTestOptions used for connection to
   *     Elasticsearch as shown above.
   */
  public static void main(String[] args) throws Exception {
    PipelineOptionsFactory.register(IOTestPipelineOptions.class);
    IOTestPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).as(IOTestPipelineOptions.class);
    createAndPopulateReadIndex(options);
  }

  private static void createAndPopulateReadIndex(IOTestPipelineOptions options) throws Exception {
    // automatically creates the index and insert docs
    ConnectionConfiguration connectionConfiguration =
        getConnectionConfiguration(options, ReadOrWrite.READ);
    try (RestClient restClient = connectionConfiguration.createClient()) {
      ElasticSearchIOTestUtils
          .insertTestDocuments(connectionConfiguration, NUM_DOCS_ITESTS, restClient);
    }
  }

  static ConnectionConfiguration getConnectionConfiguration(IOTestPipelineOptions options,
      ReadOrWrite rOw) {
    ConnectionConfiguration connectionConfiguration = ConnectionConfiguration.create(
            new String[] {
              "http://"
                  + options.getElasticsearchServer()
                  + ":"
                  + options.getElasticsearchHttpPort()
            },
            (rOw == ReadOrWrite.READ) ? ES_INDEX : writeIndex,
            ES_TYPE);
    return connectionConfiguration;
  }

  /** Enum that tells whether we use the index for reading or for writing. */
  enum ReadOrWrite {
    READ,
    WRITE
  }
}
