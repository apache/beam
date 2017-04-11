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
package org.apache.beam.sdk.io.hadoop;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import java.io.Externalizable;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper to allow Hadoop {@link Configuration}s to be serialized using Java's standard
 * serialization mechanisms.
 */
public class SerializableConfiguration implements Externalizable {
  private static final long serialVersionUID = 0L;
  private static final Logger LOG = LoggerFactory.getLogger(SerializableConfiguration.class);

  private transient Configuration conf;

  public SerializableConfiguration() {
  }

  public SerializableConfiguration(Configuration conf) {
    if (conf == null) {
      throw new NullPointerException("Configuration must not be null.");
    }
    this.conf = conf;
  }

  public Configuration get() {
    return conf;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeInt(conf.size());
    for (Map.Entry<String, String> entry : conf) {
      out.writeUTF(entry.getKey());
      out.writeUTF(entry.getValue());
    }
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    conf = new Configuration(false);
    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      conf.set(in.readUTF(), in.readUTF());
    }
  }

  /**
   * Returns new configured {@link Job} object.
   */
  public static Job newJob(@Nullable SerializableConfiguration conf) throws IOException {
    if (conf == null) {
      return Job.getInstance();
    } else {
      // Don't reading configuration from slave thread, but only from master thread.
      Job job = Job.getInstance(new Configuration(false));
      for (Map.Entry<String, String> entry : conf.get()) {
        job.getConfiguration().set(entry.getKey(), entry.getValue());
      }
      return job;
    }
  }

  /**
   * Returns new populated {@link Configuration} object.
   */
  public static Configuration newConfiguration(@Nullable SerializableConfiguration conf) {
    if (conf == null) {
      return new Configuration();
    } else {
      return conf.get();
    }
  }

  /**
   * Loading hadoop configuration from HADOOP_CONF_DIR and YARN_CONF_DIR environment variable
   *
   * <p>Currently this makes a shallow copy of the conf directory. If there are cases where a
   * Hadoop config directory contains subdirectories, this code will have to be fixed.
   */
  public static void readConfigurationFromFile(@Nullable SerializableConfiguration conf){
    if (conf != null) {
      List<String> hadoopEnvList = Lists.newArrayList("HADOOP_CONF_DIR", "YARN_CONF_DIR");
      for (String env : hadoopEnvList){
        String path = System.getenv(env);
        if (!Strings.isNullOrEmpty(path)){
          File dir = new File(path);
          if (dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files == null) {
              LOG.warn("Failed to list files under directory " + dir);
            } else {
              for (File file : files) {
                if (file.isFile() && file.getName().endsWith(".xml")) {
                  conf.get().addResource(new Path(file.getAbsolutePath()));
                }
              }
            }
          }
        }
      }
    }
  }

}
