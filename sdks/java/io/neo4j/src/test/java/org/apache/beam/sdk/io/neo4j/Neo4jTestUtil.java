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
package org.apache.beam.sdk.io.neo4j;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

public class Neo4jTestUtil {

  public static final String NEO4J_VERSION = "latest";
  public static final String NEO4J_NETWORK_ALIAS = "neo4jcontainer";
  public static final String NEO4J_USERNAME = "neo4j";
  public static final String NEO4J_PASSWORD = "abcd";
  public static final String NEO4J_DATABASE = "neo4j";

  public static final String getUrl(String hostname, int port) {
    return "neo4j://" + hostname + ":" + port;
  }

  public static Driver getDriver(String hostname, int port) throws URISyntaxException {
    return GraphDatabase.routingDriver(
        Arrays.asList(new URI(getUrl(hostname, port))),
        AuthTokens.basic(NEO4J_USERNAME, NEO4J_PASSWORD),
        Config.builder().build());
  }

  public static Session getSession(Driver driver, boolean withDatabase) {
    SessionConfig.Builder builder = SessionConfig.builder();
    if (withDatabase) {
      builder = builder.withDatabase(NEO4J_DATABASE);
    }
    return driver.session(builder.build());
  }

  public static Neo4jIO.DriverConfiguration getDriverConfiguration(String hostname, int port) {
    return Neo4jIO.DriverConfiguration.create(
        getUrl(hostname, port), NEO4J_USERNAME, NEO4J_PASSWORD);
  }

  public static void executeOnNeo4j(String hostname, int port, String cypher, boolean useDatabase)
      throws Exception {
    try (Driver driver = Neo4jTestUtil.getDriver(hostname, port)) {
      try (Session session = Neo4jTestUtil.getSession(driver, useDatabase)) {
        session.run(cypher);
      }
    }
  }
}
