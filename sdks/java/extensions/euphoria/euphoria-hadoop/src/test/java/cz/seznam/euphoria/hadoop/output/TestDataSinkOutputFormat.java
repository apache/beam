
package cz.seznam.euphoria.hadoop.output;

import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.Writer;
import cz.seznam.euphoria.core.client.util.Pair;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;

import static org.junit.Assert.*;
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verify the bridge between hadoop {@code OutputFormat} and {@code DataSink}.
 */
public class TestDataSinkOutputFormat {

  @SuppressWarnings("unchecked")
  public static class DummySink implements DataSink {

    static Map<Integer, List<Object>> outputs = new HashMap<>();
    static Map<Integer, List<Object>> committed = new HashMap<>();
    static boolean isCommitted = false;

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

  @Test
  @SuppressWarnings("unchecked")
  /**
   * Test that {@code ListDataSink} can be used in place of hadoop {@code OutputFormat}.
   **/
  public void testDataSink() throws Exception {
    DummySink sink = new DummySink();
    Configuration conf = new Configuration();
    DataSinkOutputFormat.configure(conf, sink);

    // mock the instances we will need
    TaskAttemptContext first = mockContext(conf, 0);
    TaskAttemptContext second = mockContext(conf, 1);

    // instantiate the output format
    DataSinkOutputFormat format = DataSinkOutputFormat.class.newInstance();

    // validate
    format.checkOutputSpecs(first);

    // create record writer for the first partition
    RecordWriter writer = format.getRecordWriter(first);
    writer.write(1L, 2L);
    writer.close(first);
    format.getOutputCommitter(first).commitTask(first);

    // now the second partition, we need to create new instnace of outputformat
    format = DataSinkOutputFormat.class.newInstance();
    // validate
    format.checkOutputSpecs(second);

    // create record writer for the second partition
    writer = format.getRecordWriter(second);
    writer.write(2L, 4L);
    writer.close(second);
    OutputCommitter committer = format.getOutputCommitter(second);
    committer.commitTask(second);

    // and now vlidate what was written
    assertFalse(DummySink.isCommitted);

    committer.commitJob(second);
    assertTrue(DummySink.isCommitted);

    assertTrue(DummySink.outputs.isEmpty());
    assertEquals(2, DummySink.committed.size());

    assertEquals(Arrays.asList(Pair.of(1L, 2L)), DummySink.committed.get(0));
    assertEquals(Arrays.asList(Pair.of(2L, 4L)), DummySink.committed.get(1));
  }

  private TaskAttemptContext mockContext(Configuration conf, int taskId) {
    TaskAttemptContext ret = mock(TaskAttemptContext.class);
    TaskAttemptID mockAttemptId = mock(TaskAttemptID.class);
    TaskID mockTaskId = mock(TaskID.class);
    when(ret.getConfiguration()).thenReturn(conf);
    when(ret.getTaskAttemptID()).thenReturn(mockAttemptId);
    when(mockAttemptId.getTaskID()).thenReturn(mockTaskId);
    when(mockTaskId.getId()).thenReturn(taskId);
    return ret;
  }

}
