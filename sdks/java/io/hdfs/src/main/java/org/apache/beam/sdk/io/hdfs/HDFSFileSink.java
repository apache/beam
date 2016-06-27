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

import static com.google.common.base.Preconditions.checkState;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Sink;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.KV;

import com.google.api.client.util.Maps;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * A {@code Sink} for writing records to a Hadoop filesystem using a Hadoop file-based output
 * format.
 *
 * @param <K> The type of keys to be written to the sink.
 * @param <V> The type of values to be written to the sink.
 */
public class HDFSFileSink<K, V> extends Sink<KV<K, V>> {

  private static final JobID jobId = new JobID(
      Long.toString(System.currentTimeMillis()),
      new Random().nextInt(Integer.MAX_VALUE));

  protected final String path;
  protected final Class<? extends FileOutputFormat<K, V>> formatClass;

  // workaround to make Configuration serializable
  private final Map<String, String> map;

  public HDFSFileSink(String path, Class<? extends FileOutputFormat<K, V>> formatClass) {
    this.path = path;
    this.formatClass = formatClass;
    this.map = Maps.newHashMap();
  }

  public HDFSFileSink(String path, Class<? extends FileOutputFormat<K, V>> formatClass,
                      Configuration conf) {
    this(path, formatClass);
    // serialize conf to map
    for (Map.Entry<String, String> entry : conf) {
      map.put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void validate(PipelineOptions options) {
    try {
      Job job = jobInstance();
      FileSystem fs = FileSystem.get(job.getConfiguration());
      checkState(!fs.exists(new Path(path)), "Output path " + path + " already exists");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Sink.WriteOperation<KV<K, V>, ?> createWriteOperation(PipelineOptions options) {
    return new HDFSWriteOperation<>(this, path, formatClass);
  }

  private Job jobInstance() throws IOException {
    Job job = Job.getInstance();
    // deserialize map to conf
    Configuration conf = job.getConfiguration();
    for (Map.Entry<String, String> entry : map.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
    job.setJobID(jobId);
    return job;
  }

  // =======================================================================
  // WriteOperation
  // =======================================================================

  /** {{@link WriteOperation}} for HDFS. */
  public static class HDFSWriteOperation<K, V> extends WriteOperation<KV<K, V>, String> {

    private final Sink<KV<K, V>> sink;
    protected final String path;
    protected final Class<? extends FileOutputFormat<K, V>> formatClass;

    public HDFSWriteOperation(Sink<KV<K, V>> sink,
                              String path,
                              Class<? extends FileOutputFormat<K, V>> formatClass) {
      this.sink = sink;
      this.path = path;
      this.formatClass = formatClass;
    }

    @Override
    public void initialize(PipelineOptions options) throws Exception {
      Job job = ((HDFSFileSink<K, V>) getSink()).jobInstance();
      FileOutputFormat.setOutputPath(job, new Path(path));
    }

    @Override
    public void finalize(Iterable<String> writerResults, PipelineOptions options) throws Exception {
      Job job = ((HDFSFileSink<K, V>) getSink()).jobInstance();
      FileSystem fs = FileSystem.get(job.getConfiguration());

      // If there are 0 output shards, just create output folder.
      if (!writerResults.iterator().hasNext()) {
        fs.mkdirs(new Path(path));
        return;
      }

      // job successful
      JobContext context = new JobContextImpl(job.getConfiguration(), job.getJobID());
      FileOutputCommitter outputCommitter = new FileOutputCommitter(new Path(path), context);
      outputCommitter.commitJob(context);

      // get actual output shards
      Set<String> actual = Sets.newHashSet();
      FileStatus[] statuses = fs.listStatus(new Path(path), new PathFilter() {
        @Override
        public boolean accept(Path path) {
          String name = path.getName();
          return !name.startsWith("_") && !name.startsWith(".");
        }
      });

      // get expected output shards
      Set<String> expected = Sets.newHashSet(writerResults);
      checkState(
          expected.size() == Lists.newArrayList(writerResults).size(),
          "Data loss due to writer results hash collision");
      for (FileStatus s : statuses) {
        String name = s.getPath().getName();
        int pos = name.indexOf('.');
        actual.add(pos > 0 ? name.substring(0, pos) : name);
      }

      checkState(actual.equals(expected), "Writer results and output files do not match");

      // rename output shards to Hadoop style, i.e. part-r-00000.txt
      int i = 0;
      for (FileStatus s : statuses) {
        String name = s.getPath().getName();
        int pos = name.indexOf('.');
        String ext = pos > 0 ? name.substring(pos) : "";
        fs.rename(
            s.getPath(),
            new Path(s.getPath().getParent(), String.format("part-r-%05d%s", i, ext)));
        i++;
      }
    }

    @Override
    public Writer<KV<K, V>, String> createWriter(PipelineOptions options) throws Exception {
      return new HDFSWriter<>(this, path, formatClass);
    }

    @Override
    public Sink<KV<K, V>> getSink() {
      return sink;
    }

    @Override
    public Coder<String> getWriterResultCoder() {
      return StringUtf8Coder.of();
    }

  }

  // =======================================================================
  // Writer
  // =======================================================================

  /** {{@link Writer}} for HDFS files. */
  public static class HDFSWriter<K, V> extends Writer<KV<K, V>, String> {

    private final HDFSWriteOperation<K, V> writeOperation;
    private final String path;
    private final Class<? extends FileOutputFormat<K, V>> formatClass;

    // unique hash for each task
    private int hash;

    private TaskAttemptContext context;
    private RecordWriter<K, V> recordWriter;
    private FileOutputCommitter outputCommitter;

    public HDFSWriter(HDFSWriteOperation<K, V> writeOperation,
                      String path,
                      Class<? extends FileOutputFormat<K, V>> formatClass) {
      this.writeOperation = writeOperation;
      this.path = path;
      this.formatClass = formatClass;
    }

    @Override
    public void open(String uId) throws Exception {
      this.hash = uId.hashCode();

      Job job = ((HDFSFileSink<K, V>) getWriteOperation().getSink()).jobInstance();
      FileOutputFormat.setOutputPath(job, new Path(path));

      // Each Writer is responsible for writing one bundle of elements and is represented by one
      // unique Hadoop task based on uId/hash. All tasks share the same job ID. Since Dataflow
      // handles retrying of failed bundles, each task has one attempt only.
      JobID jobId = job.getJobID();
      TaskID taskId = new TaskID(jobId, TaskType.REDUCE, hash);
      context = new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID(taskId, 0));

      FileOutputFormat<K, V> outputFormat = formatClass.newInstance();
      recordWriter = outputFormat.getRecordWriter(context);
      outputCommitter = (FileOutputCommitter) outputFormat.getOutputCommitter(context);
    }

    @Override
    public void write(KV<K, V> value) throws Exception {
      recordWriter.write(value.getKey(), value.getValue());
    }

    @Override
    public String close() throws Exception {
      // task/attempt successful
      recordWriter.close(context);
      outputCommitter.commitTask(context);

      // result is prefix of the output file name
      return String.format("part-r-%d", hash);
    }

    @Override
    public WriteOperation<KV<K, V>, String> getWriteOperation() {
      return writeOperation;
    }

  }

}
