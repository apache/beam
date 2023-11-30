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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A wrapper to allow Hadoop {@link Configuration}s to be serialized using Java's standard
 * serialization mechanisms.
 *
 * <p>SerializableConfiguration is not thread safe. For example, serialized configuration could be
 * corrupted if {@link SerializableConfiguration#writeExternal} and {@link
 * SerializableConfiguration#readExternal} are run at the same time, or the {@link Configuration} is
 * changed outside during writeExternal.
 */
@NotThreadSafe
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class SerializableConfiguration implements Externalizable {
  private static final long serialVersionUID = 0L;

  private transient Configuration conf;

  // a flag marking conf might be changed so pending invalidate cache used by writeExternal
  private transient boolean confMutated;

  private transient byte[] serializationCache;

  public SerializableConfiguration() {}

  public SerializableConfiguration(Configuration conf) {
    if (conf == null) {
      throw new NullPointerException("Configuration must not be null.");
    }
    this.conf = conf;
  }

  public Configuration get() {
    // get() call returns the original conf, which is mutable by caller
    confMutated = true;
    return conf;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    if (confMutated || serializationCache == null) {
      confMutated = false;
      ByteArrayOutputStream baos = new ByteArrayOutputStream(512);
      try (DataOutputStream dos = new DataOutputStream(baos)) {
        // this call is slow.
        conf.write(dos);
        serializationCache = baos.toByteArray();
      }
    }
    out.writeUTF(conf.getClass().getCanonicalName());
    out.write(serializationCache);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    confMutated = true;
    String className = in.readUTF();
    try {
      conf =
          Class.forName(className)
              .asSubclass(Configuration.class)
              .getDeclaredConstructor()
              .newInstance();
      conf.readFields(in);
    } catch (InstantiationException
        | IllegalAccessException
        | NoSuchMethodException
        | InvocationTargetException e) {
      throw new IOException("Unable to create configuration: " + e);
    }
  }

  /** Returns new configured {@link Job} object. */
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

  /** Returns a new configuration instance using provided flags. */
  public static SerializableConfiguration fromMap(Map<String, String> entries) {
    Configuration hadoopConfiguration = new Configuration();

    for (Map.Entry<String, String> entry : entries.entrySet()) {
      hadoopConfiguration.set(entry.getKey(), entry.getValue());
    }

    return new SerializableConfiguration(hadoopConfiguration);
  }

  /** Returns new populated {@link Configuration} object. */
  public static Configuration newConfiguration(@Nullable SerializableConfiguration conf) {
    if (conf == null) {
      return new Configuration();
    } else {
      return conf.get();
    }
  }
}
