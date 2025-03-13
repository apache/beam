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
import static org.apache.hadoop.mapreduce.lib.db.DBConfiguration.INPUT_CLASS_PROPERTY;
import static org.apache.hadoop.mapreduce.lib.db.DBConfiguration.INPUT_FIELD_NAMES_PROPERTY;
import static org.apache.hadoop.mapreduce.lib.db.DBConfiguration.INPUT_ORDER_BY_PROPERTY;
import static org.apache.hadoop.mapreduce.lib.db.DBConfiguration.INPUT_TABLE_NAME_PROPERTY;
import static org.apache.hadoop.mapreduce.lib.db.DBConfiguration.PASSWORD_PROPERTY;
import static org.apache.hadoop.mapreduce.lib.db.DBConfiguration.URL_PROPERTY;
import static org.apache.hadoop.mapreduce.lib.db.DBConfiguration.USERNAME_PROPERTY;

import io.cdap.cdap.api.data.batch.InputFormatProvider;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.hadoop.format.HadoopFormatIO;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;

/**
 * {@link InputFormatProvider} for {@link DBBatchSource} CDAP plugin. Used for integration test
 * {@link CdapIO#read()}.
 */
public class DBInputFormatProvider implements InputFormatProvider {

  private static final String POSTGRESQL_DRIVER = "org.postgresql.Driver";

  private final Map<String, String> conf;

  DBInputFormatProvider(DBConfig config) {
    this.conf = new HashMap<>();

    conf.put(DRIVER_CLASS_PROPERTY, POSTGRESQL_DRIVER);
    conf.put(URL_PROPERTY, config.dbUrl);
    conf.put(USERNAME_PROPERTY, config.pgUsername);
    conf.put(PASSWORD_PROPERTY, config.pgPassword);

    conf.put(INPUT_TABLE_NAME_PROPERTY, config.tableName);
    conf.put(INPUT_FIELD_NAMES_PROPERTY, config.fieldNames);
    conf.put(INPUT_ORDER_BY_PROPERTY, config.orderBy);
    conf.put(INPUT_CLASS_PROPERTY, config.valueClassName);

    conf.put(HadoopFormatIO.JOB_ID, String.valueOf(1));
  }

  @Override
  public String getInputFormatClassName() {
    return DBInputFormat.class.getName();
  }

  @Override
  public Map<String, String> getInputFormatConfiguration() {
    return conf;
  }
}
