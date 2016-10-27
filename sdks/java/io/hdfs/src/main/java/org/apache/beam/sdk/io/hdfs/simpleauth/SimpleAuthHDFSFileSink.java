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
package org.apache.beam.sdk.io.hdfs.simpleauth;

import java.security.PrivilegedExceptionAction;
import org.apache.beam.sdk.io.Sink;
import org.apache.beam.sdk.io.hdfs.HDFSFileSink;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * A {@code Sink} for writing records to a Hadoop filesystem using a Hadoop file-based output
 * format with Simple Authentication.
 *
 * <p>Allows arbitrary username as HDFS user, which is used for writing to HDFS.
 *
 * @param <K> The type of keys to be written to the sink.
 * @param <V> The type of values to be written to the sink.
 */
public class SimpleAuthHDFSFileSink<K, V> extends HDFSFileSink<K, V> {
  private final String username;

  public SimpleAuthHDFSFileSink(String path,
                                Class<? extends FileOutputFormat<K, V>> formatClass,
                                Configuration conf,
                                String username) {
    super(path, formatClass, conf);
    this.username = username;
  }

  @Override
  public WriteOperation<KV<K, V>, ?> createWriteOperation(PipelineOptions options) {
    return new SimpleAuthHDFSWriteOperation<>(this, path, formatClass, username);
  }

  /** {{@link WriteOperation}} for HDFS with Simple Authentication. */
  public static class SimpleAuthHDFSWriteOperation<K, V> extends HDFSWriteOperation<K, V> {
    private final String username;

    SimpleAuthHDFSWriteOperation(Sink<KV<K, V>> sink,
                                 String path,
                                 Class<? extends FileOutputFormat<K, V>> formatClass,
                                 String username) {
      super(sink, path, formatClass);
      this.username = username;
    }

    @Override
    public void finalize(final Iterable<String> writerResults, final PipelineOptions options)
        throws Exception {
      UserGroupInformation.createRemoteUser(username).doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          superFinalize(writerResults, options);
          return null;
        }
      });
    }

    private void superFinalize(Iterable<String> writerResults, PipelineOptions options)
        throws Exception {
      super.finalize(writerResults, options);
    }

    @Override
    public Writer<KV<K, V>, String> createWriter(PipelineOptions options) throws Exception {
      return new SimpleAuthHDFSWriter<>(this, path, formatClass, username);
    }
  }

  /** {{@link Writer}} for HDFS files with Simple Authentication. */
  public static class SimpleAuthHDFSWriter<K, V> extends HDFSWriter<K, V> {
    private final UserGroupInformation ugi;

    public SimpleAuthHDFSWriter(SimpleAuthHDFSWriteOperation<K, V> writeOperation,
                                String path,
                                Class<? extends FileOutputFormat<K, V>> formatClass,
                                String username) {
      super(writeOperation, path, formatClass);
      ugi = UserGroupInformation.createRemoteUser(username);
    }

    @Override
    public void open(final String uId) throws Exception {
      ugi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          superOpen(uId);
          return null;
        }
      });
    }

    private void superOpen(String uId) throws Exception {
      super.open(uId);
    }

    @Override
    public String close() throws Exception {
      return ugi.doAs(new PrivilegedExceptionAction<String>() {
        @Override
        public String run() throws Exception {
          return superClose();
        }
      });
    }

    private String superClose() throws Exception {
      return super.close();
    }
  }

}
