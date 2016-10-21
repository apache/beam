
package cz.seznam.euphoria.hadoop.input;

import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.Partition;
import cz.seznam.euphoria.core.client.io.Reader;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.hadoop.utils.Serializer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * {@code InputFormat} based on {@code DataSource}.
 */
public class DataSourceInputFormat<K, V> extends InputFormat<K, V> {

  private static final String DATA_SOURCE = "cz.seznam.euphoria.hadoop.data-source-serialized";

  public static <K, V> void configure(
      Configuration conf, DataSource<Pair<K, V>> source) throws IOException {

    conf.set(DATA_SOURCE, toBase64(source));
  }

  private static <K, V> String toBase64(
      DataSource<Pair<K, V>> source) throws IOException {
    
    return Base64.encodeBase64String(Serializer.toBytes(source));
  }
  private static <K, V> DataSource<Pair<K, V>> fromBase64(String base64bytes)
      throws IOException, ClassNotFoundException {
    
    return Serializer.fromBytes(Base64.decodeBase64(base64bytes));
  }


  private static class SourceSplit<K, V> extends InputSplit implements Writable {

    private Partition<Pair<K, V>> partition;

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
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
        oos.writeObject(partition);
      }
      byte[] serialized = baos.toByteArray();
      WritableUtils.writeVInt(d, serialized.length);
      d.write(serialized);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
      byte[] serialized = new byte[WritableUtils.readVInt(di)];
      di.readFully(serialized);
      ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
      try (ObjectInputStream ois = new ObjectInputStream(bais)) {
        this.partition = (Partition<Pair<K, V>>) ois.readObject();
      } catch (ClassNotFoundException ex) {
        throw new IOException(ex);
      }
    }

  }

  DataSource<Pair<K, V>> source;

  @Override
  public List<InputSplit> getSplits(JobContext jc)
      throws IOException, InterruptedException {

    initialize(jc.getConfiguration());
    return source.getPartitions().stream().map(SourceSplit::new)
        .collect(Collectors.toList());
  }

  @Override
  public RecordReader<K, V> createRecordReader(
      InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
    
    initialize(tac.getConfiguration());
    SourceSplit<K, V> split = (SourceSplit<K, V>) is;
    Reader<Pair<K, V>> reader = split.partition.openReader();
    return new RecordReader<K, V>() {

      K k;
      V v;

      @Override
      public void initialize(InputSplit is, TaskAttemptContext tac)
          throws IOException, InterruptedException {
        // nop
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        if (reader.hasNext()) {
          Pair<K, V> next = reader.next();
          k = next.getFirst();
          v = next.getSecond();
          return true;
        }
        return false;
      }

      @Override
      public K getCurrentKey() throws IOException, InterruptedException {
        return k;
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
