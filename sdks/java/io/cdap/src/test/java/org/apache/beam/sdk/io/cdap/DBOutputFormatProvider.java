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
package org.apache.beam.sdk.io.cdap;

import static org.apache.hadoop.mapreduce.lib.db.DBConfiguration.DRIVER_CLASS_PROPERTY;
import static org.apache.hadoop.mapreduce.lib.db.DBConfiguration.OUTPUT_FIELD_COUNT_PROPERTY;
import static org.apache.hadoop.mapreduce.lib.db.DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY;
import static org.apache.hadoop.mapreduce.lib.db.DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY;
import static org.apache.hadoop.mapreduce.lib.db.DBConfiguration.PASSWORD_PROPERTY;
import static org.apache.hadoop.mapreduce.lib.db.DBConfiguration.URL_PROPERTY;
import static org.apache.hadoop.mapreduce.lib.db.DBConfiguration.USERNAME_PROPERTY;

import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.hadoop.format.HadoopFormatIO;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;

/**
 * {@link OutputFormatProvider} for {@link DBBatchSink} CDAP plugin. Used for integration test
 * {@link CdapIO#write()}.
 */
public class DBOutputFormatProvider implements OutputFormatProvider {

  private static final String POSTGRESQL_DRIVER = "org.postgresql.Driver";

  private final Map<String, String> conf;

  DBOutputFormatProvider(DBConfig config) {
    this.conf = new HashMap<>();

    conf.put(DRIVER_CLASS_PROPERTY, POSTGRESQL_DRIVER);
    conf.put(URL_PROPERTY, config.dbUrl);
    conf.put(USERNAME_PROPERTY, config.pgUsername);
    conf.put(PASSWORD_PROPERTY, config.pgPassword);

    conf.put(OUTPUT_TABLE_NAME_PROPERTY, config.tableName);
    conf.put(OUTPUT_FIELD_COUNT_PROPERTY, config.fieldCount);
    conf.put(OUTPUT_FIELD_NAMES_PROPERTY, config.fieldNames);

    conf.put(HadoopFormatIO.JOB_ID, String.valueOf(1));
  }

  @Override
  public String getOutputFormatClassName() {
    return DBOutputFormat.class.getName();
  }

  @Override
  public Map<String, String> getOutputFormatConfiguration() {
    return conf;
  }
}
