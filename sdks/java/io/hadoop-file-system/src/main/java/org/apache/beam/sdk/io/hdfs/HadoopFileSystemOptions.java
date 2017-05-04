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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link PipelineOptions} which encapsulate {@link Configuration Hadoop Configuration}
 * for the {@link HadoopFileSystem}.
 */
public interface HadoopFileSystemOptions extends PipelineOptions {
  @Description("A list of Hadoop configurations used to configure zero or more Hadoop filesystems. "
      + "To specify on the command-line, represent the value as a JSON list of JSON maps, where "
      + "each map represents the entire configuration for a single Hadoop filesystem. For example "
      + "--hdfsConfiguration='[{\"fs.default.name\": \"hdfs://localhost:9998\", ...},"
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

      for (String confDir : confDirs) {
        if (new File(confDir).exists()) {
          Configuration conf = new Configuration(false);
          if (new File(confDir, "core-site.xml").exists()) {
            conf.addResource(new Path(confDir, "core-site.xml"));

            if (LOG.isDebugEnabled()) {
              LOG.debug("Adding {}/core-site.xml to hadoop configuration", confDir);
            }
          }

          if (new File(confDir, "hdfs-site.xml").exists()) {
            conf.addResource(new Path(confDir, "hdfs-site.xml"));

            if (LOG.isDebugEnabled()) {
              LOG.debug("Adding {}/hdfs-site.xml to hadoop configuration", confDir);
            }
          }

          if (conf.size() != 0) {
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
