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
package org.apache.beam.sdk.io.hcatalog;

import java.io.File;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of a light-weight embedded metastore.
 * This class is a trimmed-down version of <a href="https://github.com/apache/hive/blob/master
/hcatalog/core/src/test/java/org/apache/hive/hcatalog/mapreduce/HCatBaseTest.java">
 * https://github.com/apache/hive/blob/master/hcatalog/core/src/test/java/org/apache/hive/hcatalog/mapreduce
 * /HCatBaseTest.java </a>
 */
public class EmbeddedMetastoreService {

  private static final Logger LOG = LoggerFactory.getLogger(EmbeddedMetastoreService.class);
  private static final String BASE_DIR = System.getProperty("user.dir");
  private static final String HIVE_DIR = "/target/hive";
  private static final String TEST_DATA_DIR = HCatUtil.makePathASafeFileName(BASE_DIR + HIVE_DIR
      + "/data/" + EmbeddedMetastoreService.class.getCanonicalName() + "-"
      + System.currentTimeMillis());
  private static final String TEST_WAREHOUSE_DIR = TEST_DATA_DIR + "/warehouse";
  private static final EmbeddedMetastoreService SVC_INSTANCE = new EmbeddedMetastoreService();

  static {
    try {
      initiateService();
    } catch (MetaException e) {
      LOG.error("Exception while initiating service", e);
    }
  }

  private Driver driver;
  private HiveConf hiveConf;

  private EmbeddedMetastoreService() {
  }

  private static void initiateService() throws MetaException {

    //set-up the directories for test datastore
    setUpFSDirectory(HIVE_DIR);
    setUpFSDirectory(TEST_DATA_DIR);
    setUpFSDirectory(TEST_WAREHOUSE_DIR);

    //below properties are used by theembedded metastore to store config
    //and data files during execution
    System.setProperty("test.tmp.dir", HCatUtil.makePathASafeFileName(
        BASE_DIR + HIVE_DIR));
    System.setProperty("derby.stream.error.file",
        HCatUtil.makePathASafeFileName(BASE_DIR + HIVE_DIR + "/derby.log"));

    if (SVC_INSTANCE.driver == null) {
      SVC_INSTANCE.setUpHiveConf();
      SVC_INSTANCE.driver = new Driver(SVC_INSTANCE.hiveConf);
      SessionState.start(new CliSessionState(SVC_INSTANCE.hiveConf));
    }
  }

  private static void setUpFSDirectory(String dirPath) {
    File fsDir = new File(dirPath);
    fsDir.mkdir();
    fsDir.deleteOnExit();
  }

  /**
   * This method executes the passed query on the embedded metastore service.
   * @throws CommandNeedRetryException
   */
  static void executeQuery (String query) throws CommandNeedRetryException {
    LOG.debug("Executing query -" + query);
    SVC_INSTANCE.driver.run(query);
  }

  /**
   * This method returns the HiveConf object for the embedded metastore.
   */
  static HiveConf getHiveConf ()  {
    return SVC_INSTANCE.hiveConf;
  }

  void setUpHiveConf() {
    hiveConf = new HiveConf(getClass());
    hiveConf.setVar(HiveConf.ConfVars.PREEXECHOOKS, "");
    hiveConf.setVar(HiveConf.ConfVars.POSTEXECHOOKS, "");
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    hiveConf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, TEST_WAREHOUSE_DIR);
    hiveConf.setVar(HiveConf.ConfVars.HIVEMAPREDMODE, "nonstrict");
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVEOPTIMIZEMETADATAQUERIES, true);
    hiveConf
      .setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
          "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd."
          + "SQLStdHiveAuthorizerFactory");
  }
}
