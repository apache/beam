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

package org.apache.beam.sdk.io.hdfs;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import java.io.File;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to work with Apache Hadoop.
 */
public class HadoopUtils {
  private static final Logger LOG = LoggerFactory.getLogger(SerializableConfiguration.class);

  /**
   * Loading hadoop configuration from HADOOP_CONF_DIR and YARN_CONF_DIR environment variable.
   */
  public static void readConfigurationFromHadoopYarnConfigDirs(
          @Nullable SerializableConfiguration conf) {

    if (conf != null) {
      List<String> hadoopEnvList = Lists.newArrayList("HADOOP_CONF_DIR", "YARN_CONF_DIR");
      for (String env : hadoopEnvList) {
        String hadoopConfPath = System.getenv(env);
        if (!Strings.isNullOrEmpty(hadoopConfPath)
                && new File(hadoopConfPath).exists()) {

          // We just need to load both core-site.xml and hdfs-site.xml to determine the
          // default fs path and the hdfs configuration
          if (new File(hadoopConfPath + "/core-site.xml").exists()) {
            conf.get().addResource(new Path(hadoopConfPath + "/core-site.xml"));

            if (LOG.isDebugEnabled()) {
              LOG.debug("Adding " + hadoopConfPath + "/core-site.xml to hadoop configuration");
            }
          }

          if (new File(hadoopConfPath + "/hdfs-site.xml").exists()) {
            conf.get().addResource(new Path(hadoopConfPath + "/hdfs-site.xml"));

            if (LOG.isDebugEnabled()) {
              LOG.debug("Adding " + hadoopConfPath + "/hdfs-site.xml to hadoop configuration");
            }
          }
        }
      }
    }
  }
}
