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

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link PipelineOptions} which encapsulate {@link Configuration Hadoop Configuration} for the
 * {@link HadoopFileSystem}.
 */
@SuppressWarnings("WeakerAccess")
@Experimental(Kind.FILESYSTEM)
public interface HadoopFileSystemOptions extends PipelineOptions {
  @Description(
      "A list of Hadoop configurations used to configure zero or more Hadoop filesystems. "
          + "By default, Hadoop configuration is loaded from 'core-site.xml' and 'hdfs-site.xml' "
          + "based upon the HADOOP_CONF_DIR and YARN_CONF_DIR environment variables. "
          + "To specify configuration on the command-line, represent the value as a JSON list of JSON "
          + "maps, where each map represents the entire configuration for a single Hadoop filesystem. "
          + "For example --hdfsConfiguration='[{\"fs.default.name\": \"hdfs://localhost:9998\", ...},"
          + "{\"fs.default.name\": \"s3a://\", ...},...]'")
  @Default.InstanceFactory(ConfigurationLocator.class)
  List<Configuration> getHdfsConfiguration();

  void setHdfsConfiguration(List<Configuration> value);

  /** A {@link DefaultValueFactory} which locates a Hadoop {@link Configuration}. */
  class ConfigurationLocator implements DefaultValueFactory<List<Configuration>> {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigurationLocator.class);

    @Override
    public List<Configuration> create(PipelineOptions options) {
      // Find default configuration when HADOOP_CONF_DIR or YARN_CONF_DIR is set.
      List<Configuration> configurationList = readConfigurationFromHadoopYarnConfigDirs();
      return configurationList.size() > 0 ? configurationList : null;
    }

    private List<Configuration> readConfigurationFromHadoopYarnConfigDirs() {
      List<Configuration> configurationList = Lists.newArrayList();

      /*
       * If we find a configuration in HADOOP_CONF_DIR and YARN_CONF_DIR,
       * we should be returning them both separately.
       *
       * Also, ensure that we only load one configuration if both
       * HADOOP_CONF_DIR and YARN_CONF_DIR point to the same location.
       */
      Set<String> confDirs = Sets.newHashSet();
      for (String confDir : Lists.newArrayList("HADOOP_CONF_DIR", "YARN_CONF_DIR")) {
        if (getEnvironment().containsKey(confDir)) {
          String hadoopConfDir = getEnvironment().get(confDir);
          if (!Strings.isNullOrEmpty(hadoopConfDir)) {
            confDirs.add(hadoopConfDir);
          }
        }
      }

      /*
       * Explode the paths by ":" to handle the case in which the environment variables
       * contains multiple paths.
       *
       * This happens on Cloudera 6.x, in which the spark-env.sh script sets
       * HADOOP_CONF_DIR by appending also the Hive configuration folder:
       *
       * if [ -d "$HIVE_CONF_DIR" ]; then
       *   HADOOP_CONF_DIR="$HADOOP_CONF_DIR:$HIVE_CONF_DIR"
       * fi
       * export HADOOP_CONF_DIR
       *
       */
      Set<String> explodedConfDirs = Sets.newHashSet();
      for (String confDir : confDirs) {
        Iterable<String> paths = Splitter.on(':').split(confDir);
        for (String p : paths) {
          explodedConfDirs.add(p);
        }
      }

      // Load the configuration from paths found (if exists)
      for (String confDir : explodedConfDirs) {
        if (new File(confDir).exists()) {
          Configuration conf = new Configuration(false);
          boolean confLoaded = false;
          for (String confName : Lists.newArrayList("core-site.xml", "hdfs-site.xml")) {
            File confFile = new File(confDir, confName);
            if (confFile.exists()) {
              LOG.debug("Adding {} to hadoop configuration", confFile.getAbsolutePath());
              conf.addResource(new Path(confFile.getAbsolutePath()));
              confLoaded = true;
            }
          }
          if (confLoaded) {
            configurationList.add(conf);
          }
        }
      }
      return configurationList;
    }

    @VisibleForTesting
    Map<String, String> getEnvironment() {
      return System.getenv();
    }
  }
}
