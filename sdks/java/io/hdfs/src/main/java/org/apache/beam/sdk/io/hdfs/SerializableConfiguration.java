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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

/**
 * A wrapper to allow Hadoop {@link Configuration}s to be serialized using Java's standard
 * serialization mechanisms.
 */
public class SerializableConfiguration implements Externalizable {
  private static final long serialVersionUID = 0L;

  private Configuration conf;

  public SerializableConfiguration() {
  }

  public SerializableConfiguration(Configuration conf) {
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
      Job job = Job.getInstance();
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

}
