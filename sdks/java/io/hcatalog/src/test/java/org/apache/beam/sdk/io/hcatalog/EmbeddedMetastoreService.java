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

import static org.apache.hive.hcatalog.common.HCatUtil.makePathASafeFileName;
import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * Implementation of a light-weight embedded metastore.
 * This class is a trimmed-down version of <a href="https://github.com/apache/hive/blob/master
/hcatalog/core/src/test/java/org/apache/hive/hcatalog/mapreduce/HCatBaseTest.java">
 * https://github.com/apache/hive/blob/master/hcatalog/core/src/test/java/org/apache/hive/hcatalog/mapreduce
 * /HCatBaseTest.java </a>
 */
public final class EmbeddedMetastoreService {

  private final Driver driver;
  private final HiveConf hiveConf;
  private final SessionState sessionState;
  private String baseDirPath;

  EmbeddedMetastoreService(String baseDirPath) throws IOException {

    String hiveDirPath, testDataDirPath, testWarehouseDirPath;
    this.baseDirPath = baseDirPath;
    hiveDirPath = makePathASafeFileName(baseDirPath + "/hive");
    testDataDirPath = makePathASafeFileName(hiveDirPath + "/data/"
    + EmbeddedMetastoreService.class.getCanonicalName()
        + System.currentTimeMillis());
    testWarehouseDirPath = makePathASafeFileName(testDataDirPath + "/warehouse");

    setupWorkspace(hiveDirPath, testDataDirPath, testWarehouseDirPath);

    System.setProperty("test.tmp.dir", hiveDirPath);
    System.setProperty("derby.stream.error.file", makePathASafeFileName(hiveDirPath
        + "/derby.log"));
    hiveConf = constructHiveConf(testWarehouseDirPath);
    driver = new Driver(getHiveConf());
    sessionState = SessionState.start(new CliSessionState(getHiveConf()));
  }

  private void setupWorkspace(String... dirPaths) throws IOException {
    for (String eachDirPath : dirPaths) {
      File fsDir = new File(eachDirPath);
      fsDir.mkdir();
    }
  }

  /**
   * This method executes the passed query on the embedded metastore service.
   * @throws CommandNeedRetryException
   */
  void executeQuery (String query) throws CommandNeedRetryException {
    driver.run(query);
  }

  /**
   * This method returns the HiveConf object for the embedded metastore.
   */
  HiveConf getHiveConf ()  {
    return hiveConf;
  }

  private HiveConf constructHiveConf(String testWarehouseDirPath) {
    HiveConf hiveConf = new HiveConf(getClass());
    hiveConf.setVar(HiveConf.ConfVars.PREEXECHOOKS, "");
    hiveConf.setVar(HiveConf.ConfVars.POSTEXECHOOKS, "");
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    hiveConf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, testWarehouseDirPath);
    hiveConf.setVar(HiveConf.ConfVars.HIVEMAPREDMODE, "nonstrict");
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVEOPTIMIZEMETADATAQUERIES, true);
    hiveConf
      .setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
          "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd."
          + "SQLStdHiveAuthorizerFactory");
    return hiveConf;
  }

  /**
   * Shuts down the service, quietly.
   */
  public void shutDownServiceQuietly() {
    try {
      driver.close();
      sessionState.close();
      FileUtils.forceDeleteOnExit(new File(baseDirPath));
    } catch (Exception e) {
      //do nothing, return quietly
    }
  }
}
