/**
 * Copyright 2016-2017 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.hadoop.output;

import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.Writer;
import cz.seznam.euphoria.hadoop.HadoopUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Verify the bridge between hadoop {@code OutputFormat} and {@code DataSink}.
 */
public class DataSinkOutputFormatTest {

  @SuppressWarnings("unchecked")
  public static class DummySink implements DataSink {

    static Map<Integer, List<Object>> outputs = new HashMap<>();
    static Map<Integer, List<Object>> committed = new HashMap<>();
    static boolean isCommitted = false;
    static boolean isInitialized = false;

    @Override
    public void initialize() {
      if (isInitialized) {
        throw new IllegalArgumentException("Already initialized.");
      }
      isInitialized = true;
    }

    @Override
    public Writer openWriter(int partitionId) {
      List<Object> output = new ArrayList<>();
      outputs.put(partitionId, output);
      return new Writer() {
        List<Object> listOutput = output;

        @Override
        public void write(Object elem) throws IOException {
          listOutput.add(elem);
        }

        @Override
        public void commit() throws IOException {
          committed.put(partitionId, listOutput);
          outputs.remove(partitionId);
        }

        @Override
        public void close() throws IOException {
          // nop
        }
      };
    }

    @Override
    public void commit() throws IOException {
      isCommitted = true;
    }

    @Override
    public void rollback() throws IOException {
      // nop
    }

  }

  /**
   * Test that {@code ListDataSink} can be used in place of hadoop {@code OutputFormat}.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testDataSink() throws Exception {
    final DummySink sink = new DummySink();
    final Configuration conf = new Configuration();

    DataSinkOutputFormat.configure(conf, sink);

    // mock the instances we will need
    final TaskAttemptContext setupContext =
        HadoopUtils.createSetupTaskContext(conf, HadoopUtils.getJobID());
    final TaskAttemptContext firstContext =
        HadoopUtils.createTaskContext(conf, HadoopUtils.getJobID(), 0);
    final TaskAttemptContext secondContext =
        HadoopUtils.createTaskContext(conf, HadoopUtils.getJobID(), 1);
    final TaskAttemptContext cleanupContext =
        HadoopUtils.createCleanupTaskContext(conf, HadoopUtils.getJobID());

    final DataSinkOutputFormat<Long> setupFormat = DataSinkOutputFormat.class.newInstance();
    setupFormat.checkOutputSpecs(setupContext);

    // instantiate the output format
    final DataSinkOutputFormat<Long> firstFormat = DataSinkOutputFormat.class.newInstance();

    // create record writer for the first partition
    final RecordWriter<NullWritable, Long> firstWriter = firstFormat.getRecordWriter(firstContext);
    final OutputCommitter firstCommitter = firstFormat.getOutputCommitter(firstContext);
    firstCommitter.setupTask(firstContext);
    firstWriter.write(NullWritable.get(), 2L);
    firstWriter.close(firstContext);
    firstCommitter.commitTask(firstContext);

    // now the second partition, we need to create new instance of output format
    final DataSinkOutputFormat<Long> secondFormat = DataSinkOutputFormat.class.newInstance();

    // create record writer for the second partition
    final RecordWriter<NullWritable, Long> secondWriter = secondFormat.getRecordWriter(secondContext);
    final OutputCommitter secondCommitter = secondFormat.getOutputCommitter(secondContext);
    secondCommitter.setupTask(secondContext);
    secondWriter.write(NullWritable.get(), 4L);
    secondWriter.close(secondContext);
    secondCommitter.commitTask(secondContext);

    // and now validate what was written
    assertFalse(DummySink.isCommitted);

    final DataSinkOutputFormat<Long> cleanupFormat = DataSinkOutputFormat.class.newInstance();
    cleanupFormat.checkOutputSpecs(cleanupContext);
    cleanupFormat.getOutputCommitter(cleanupContext).commitJob(cleanupContext);

    assertTrue(DummySink.isCommitted);

    assertTrue(DummySink.outputs.isEmpty());
    assertEquals(2, DummySink.committed.size());

    assertEquals(Collections.singletonList(2L), DummySink.committed.get(0));
    assertEquals(Collections.singletonList(4L), DummySink.committed.get(1));
  }
}
