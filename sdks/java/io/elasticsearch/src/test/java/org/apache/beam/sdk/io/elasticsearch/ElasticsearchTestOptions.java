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

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.testing.TestPipelineOptions;

/**
 * These options can be used by a test connecting to an Elasticsearch instance to configure their
 * connection.
 */

public interface ElasticsearchTestOptions extends TestPipelineOptions {
    @Description("Server name for Elasticsearch server (host name/ip address)")
    @Default.String("elasticsearch-server-name")
    String getElasticsearchServer();
    void setElasticsearchServer(String value);


    @Description("Http port for elasticsearch server")
    @Default.Integer(9200)
    Integer getElasticsearchHttpPort();
    void setElasticsearchHttpPort(Integer value);

    @Description("Tcp port for elasticsearch server")
    @Default.Integer(9300)
    Integer getElasticsearchTcpPort();
    void setElasticsearchTcpPort(Integer value);

}
