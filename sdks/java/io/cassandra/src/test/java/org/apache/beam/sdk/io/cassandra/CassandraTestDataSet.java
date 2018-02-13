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
package org.apache.beam.sdk.io.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manipulates test data used by the {@link CassandraIO} tests.
 *
 * <p>This is independent from the tests so that for read tests it can be run separately after
 * data store creation rather than every time (which can be more fragile).
 */
public class CassandraTestDataSet {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraTestDataSet.class);

  /**
   * Use this to create the read tables before IT read tests.
   *
   * <p>To invoke this class, you can use this command line:
   * (run from the cassandra root directory)
   * mvn test-compile exec:java -Dexec.mainClass=org.apache.beam.sdk.io.cassandra
   * .CassandraTestDataSet \
   *   -Dexec.args="--cassandraHost=localhost --cassandraPort=9042 \
   *   -Dexec.classpathScope=test
   * @param args Please pass options from IOTestPipelineOptions used for connection to Cassandra as
   * shown above.
   */
  public static void main(String[] args) {
    PipelineOptionsFactory.register(IOTestPipelineOptions.class);
    IOTestPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).as(IOTestPipelineOptions.class);

    createDataTable(options);
  }

  public static final String KEYSPACE = "BEAM";
  public static final String TABLE_READ_NAME = "BEAM_READ_TEST";
  public static final String TABLE_WRITE_NAME = "BEAM_WRITE_TEST";

  public static void createDataTable(IOTestPipelineOptions options) {
    createTable(options, TABLE_READ_NAME);
    insertTestData(options, TABLE_READ_NAME);
    createTable(options, TABLE_WRITE_NAME);
  }

  public static Cluster getCluster(IOTestPipelineOptions options) {
    return Cluster.builder()
        .addContactPoint(options.getCassandraHost())
        .withPort(options.getCassandraPort())
        .build();
  }

  private static void createTable(IOTestPipelineOptions options, String tableName) {
    Cluster cluster = null;
    Session session = null;
    try {
      cluster = getCluster(options);
      session = cluster.connect();

      LOG.info("Create {} keyspace if not exists", KEYSPACE);
      session.execute("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH REPLICATION = "
          + "{'class':'SimpleStrategy', 'replication_factor':3};");

      session.execute("USE " + KEYSPACE);

      LOG.info("Create {} table if not exists", tableName);
      session.execute("CREATE TABLE IF NOT EXISTS " + tableName + "(id int, name text, PRIMARY "
          + "KEY(id))");
    } finally {
      if (session != null) {
        session.close();
      }
      if (cluster != null) {
        cluster.close();
      }
    }
  }

  private static void insertTestData(IOTestPipelineOptions options, String tableName) {
    Cluster cluster = null;
    Session session = null;
    try {
      cluster = getCluster(options);
      session = cluster.connect();

      LOG.info("Insert test dataset");
      String[] scientists = {
          "Lovelace",
          "Franklin",
          "Meitner",
          "Hopper",
          "Curie",
          "Faraday",
          "Newton",
          "Bohr",
          "Galilei",
          "Maxwell"
      };
      for (int i = 0; i < 1000; i++) {
        int index = i % scientists.length;
        session.execute("INSERT INTO " + KEYSPACE + "." + tableName + "(id, name) values("
            + i + ",'" + scientists[index] + "');");
      }
    } finally {
      if (session != null) {
        session.close();
      }
      if (cluster != null) {
        cluster.close();
      }
    }
  }

  public static void cleanUpDataTable(IOTestPipelineOptions options) {
      Cluster cluster = null;
      Session session = null;
      try {
        cluster = getCluster(options);
        session = cluster.connect();
        session.execute("TRUNCATE TABLE " + KEYSPACE + "." + TABLE_WRITE_NAME);
      } finally {
        if (session != null) {
          session.close();
        }
        if (cluster != null) {
          cluster.close();
        }
    }
  }

}
