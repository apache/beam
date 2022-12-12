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
package org.apache.beam.sdk.io.hcatalog.test;

import static org.apache.beam.sdk.io.hcatalog.test.HCatalogIOTestUtils.getConfigPropertiesAsMap;
import static org.apache.hive.hcatalog.common.HCatUtil.makePathASafeFileName;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * Implementation of a light-weight embedded metastore. This class is a trimmed-down version of <a
 * href="https://github.com/apache/hive/blob/master/hcatalog/core/src/test/java/org/apache/hive/hcatalog/mapreduce/HCatBaseTest.java">
 * https://github.com/apache/hive/blob/master/hcatalog/core/src/test/java/org/apache/hive/hcatalog/mapreduce/HCatBaseTest.java
 * </a>
 *
 * <p>Used only for testing.
 */
@Internal
public final class EmbeddedMetastoreService implements AutoCloseable {
  private final IDriver driver;
  private final HiveConf hiveConf;
  private final SessionState sessionState;

  public EmbeddedMetastoreService(String baseDirPath) throws IOException {
    FileUtils.forceDeleteOnExit(new File(baseDirPath));

    String hiveDirPath = makePathASafeFileName(baseDirPath + "/hive");
    String testDataDirPath =
        makePathASafeFileName(
            hiveDirPath
                + "/data/"
                + EmbeddedMetastoreService.class.getCanonicalName()
                + System.currentTimeMillis());
    String testWarehouseDirPath = makePathASafeFileName(testDataDirPath + "/warehouse");

    hiveConf = new HiveConf(getClass());
    hiveConf.setVar(HiveConf.ConfVars.PREEXECHOOKS, "");
    hiveConf.setVar(HiveConf.ConfVars.POSTEXECHOOKS, "");
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    hiveConf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, testWarehouseDirPath);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVEOPTIMIZEMETADATAQUERIES, true);
    hiveConf.setVar(
        HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider");
    hiveConf.set("test.tmp.dir", hiveDirPath);

    System.setProperty("derby.stream.error.file", "/dev/null");
    driver = DriverFactory.newDriver(hiveConf);
    sessionState = SessionState.start(new SessionState(hiveConf));
  }

  /** Executes the passed query on the embedded metastore service. */
  public void executeQuery(String query) {
    CommandProcessorResponse response = driver.run(query);
    if (response.failed()) {
      throw new RuntimeException(response.getException());
    }
  }

  /** Returns the HiveConf object for the embedded metastore. */
  public HiveConf getHiveConf() {
    return hiveConf;
  }

  public Map<String, String> getHiveConfAsMap() {
    return getConfigPropertiesAsMap(hiveConf);
  }

  @Override
  public void close() throws Exception {
    driver.close();
    sessionState.close();
  }
}
