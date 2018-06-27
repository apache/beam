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
package org.apache.beam.sdk.io.common;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.testing.TestPipelineOptions;

/**
 * This shared set of options is used so that the full suite of IO tests can be run in one pass - if
 * a test tries to read TestPipelineOptions, it must be able to understand all the options that were
 * passed on the command line.
 */
public interface IOTestPipelineOptions extends TestPipelineOptions {
  /* Elasticsearch */
  @Description("Server name for Elasticsearch server (host name/ip address)")
  @Default.String("elasticsearch-server-name")
  String getElasticsearchServer();

  void setElasticsearchServer(String value);

  @Description("Http port for elasticsearch server")
  @Default.Integer(9200)
  Integer getElasticsearchHttpPort();

  void setElasticsearchHttpPort(Integer value);

  /* Solr */
  @Description("Address of Zookeeper server for Solr")
  @Default.String("zookeeper-server")
  String getSolrZookeeperServer();

  void setSolrZookeeperServer(String value);

  /* Used by most IOIT */
  @Description("Number records that will be written and read by the test")
  @Default.Integer(100000)
  Integer getNumberOfRecords();

  void setNumberOfRecords(Integer count);
}
