/**
 * Copyright 2016 Seznam.cz, a.s.
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
package cz.seznam.euphoria.hadoop.input;

import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.Partition;
import cz.seznam.euphoria.core.client.io.Reader;
import cz.seznam.euphoria.hadoop.utils.Serializer;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * {@code InputFormat} based on {@code DataSource}.
 *
 * @param <V> the type of elements read from this input format
 */
public class DataSourceInputFormat<V> extends InputFormat<NullWritable, V> {

  private static final String DATA_SOURCE = "cz.seznam.euphoria.hadoop.data-source-serialized";

  /**
   * Sets/Serializes given {@link DataSource} into Hadoop configuration. Note that
   * original configuration is modified.
   *
   * @param conf Instance of Hadoop configuration
   * @param source Euphoria source
   *
   * @return Modified configuration
   *
   * @throws IOException if serializing the given data source fails for some reason
   */
  public static Configuration configure(Configuration conf, DataSource<?> source)
      throws IOException {

    conf.set(DATA_SOURCE, toBase64(source));
    return conf;
  }

  private static <V> String toBase64(
      DataSource<V> source) throws IOException {
    
    return Base64.getEncoder().encodeToString(Serializer.toBytes(source));
  }
  private static <V> DataSource<V> fromBase64(String base64bytes)
      throws IOException, ClassNotFoundException {

    byte[] serialized = Base64.getDecoder().decode(base64bytes);
    return Serializer.fromBytes(serialized);
  }


  private static class SourceSplit<V> extends InputSplit implements Writable {

    private Partition<V> partition;

    // Writable, DO NOT USE
    public SourceSplit() {

    }

    SourceSplit(Partition partition) {
      this.partition = partition;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
      // don't know
      return 0;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
      Set<String> locations = partition.getLocations();
      return locations.toArray(new String[locations.size()]);
    }

    @Override
    public void write(DataOutput d) throws IOException {
      // use java serialization
      byte[] serialized = Serializer.toBytes(partition);
      WritableUtils.writeVInt(d, serialized.length);
      d.write(serialized);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
      try {
        byte[] serialized = new byte[WritableUtils.readVInt(di)];
        di.readFully(serialized);
        this.partition = Serializer.fromBytes(serialized);
      } catch (ClassNotFoundException ex) {
        throw new IOException(ex);
      }
    }

  }

  DataSource<V> source;

  @Override
  public List<InputSplit> getSplits(JobContext jc)
      throws IOException, InterruptedException {

    initialize(jc.getConfiguration());
    return source.getPartitions().stream().map(SourceSplit::new)
        .collect(Collectors.toList());
  }

  @Override
  public RecordReader<NullWritable, V> createRecordReader(
      InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
    
    initialize(tac.getConfiguration());
    SourceSplit<V> split = (SourceSplit<V>) is;
    Reader<V> reader = split.partition.openReader();
    return new RecordReader<NullWritable, V>() {

      V v;

      @Override
      public void initialize(InputSplit is, TaskAttemptContext tac)
          throws IOException, InterruptedException {
        // nop
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        if (reader.hasNext()) {
          v = reader.next();
          return true;
        }
        return false;
      }

      @Override
      public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
      }

      @Override
      public V getCurrentValue() throws IOException, InterruptedException {
        return v;
      }

      @Override
      public float getProgress() throws IOException, InterruptedException {
        return 0.0f;
      }

      @Override
      public void close() throws IOException {
        reader.close();
      }

    };
  }

  private void initialize(Configuration conf) throws IOException {
    if (source == null) {
      String serialized = conf.get(DATA_SOURCE);
      try {
        source = fromBase64(serialized);
      } catch (ClassNotFoundException ex) {
        throw new IOException(ex);
      }
    }
  }


}
