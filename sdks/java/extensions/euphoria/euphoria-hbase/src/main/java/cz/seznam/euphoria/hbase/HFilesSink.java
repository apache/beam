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

package cz.seznam.euphoria.hbase;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.io.VoidSink;
import cz.seznam.euphoria.core.client.io.Writer;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.hadoop.output.HadoopSink;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Sink to HBase using {@code HFileOutputFormat2}.
 */
public class HFilesSink extends HadoopSink<ImmutableBytesWritable, Cell> {

  private static final Logger LOG = LoggerFactory.getLogger(HFilesSink.class);

  /**
   * Create builder for the sink.
   * @return the sink builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    Configuration conf = HBaseConfiguration.create();
    String table = null;
    Path output = null;
    List<Update<Job>> updaters = new ArrayList<>();

    /**
     * Specify configuration to use.
     * @param conf the configuration
     * @return this
     */
    public Builder withConfiguration(Configuration conf) {
      this.conf = conf;
      return this;
    }

    /**
     * Specify target table.
     * @param table the table to read
     * @return this
     */
    public Builder withTable(String table) {
      this.table = table;
      return this;
    }


    /**
     * Specify output path.
     * @param path the output path
     * @return this
     */
    public Builder withOutputPath(Path path) {
      this.output = path;
      return this;
    }

    /**
     * Whether or not to compress output HFiles.
     * @param compress compression flag
     * @return this
     */
    public Builder setCompression(boolean compress) {
      updaters.add(j -> HFileOutputFormat2.setCompressOutput(j, compress));
      return this;
    }

    public HFilesSink build() {
      Preconditions.checkArgument(table != null, "Specify table by call to `withTable`");
      Preconditions.checkArgument(
          output != null,
          "Specify output path by call to `withOutputPath`");

      updaters.add(j -> FileOutputFormat.setOutputPath(j, output));
      return new HFilesSink(table, conf, updaters);
    }

  }

  private final byte[][] endKeys;

  HFilesSink(String table, Configuration conf, List<Update<Job>> updaters) {
    super(HFileOutputFormat2.class, toConf(updaters, conf));
    this.endKeys = getEndKeys(table, getConfiguration());
  }

  private static Configuration toConf(List<Update<Job>> updaters, Configuration conf) {
    try {
      Configuration ret = HBaseConfiguration.create(conf);
      Job job = Job.getInstance(ret);
      for (Update<Job> u : updaters) {
        try {
          u.update(job);
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      }
      return job.getConfiguration();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public boolean prepareDataset(
      Dataset<Pair<ImmutableBytesWritable, Cell>> output) {

    // initialize this inside the `keyBy` or `reduceBy` function
    final AtomicReference<ByteBuffer[]> bufferedKeys = new AtomicReference<>();

    CellComparator comparator = new CellComparator();

    ReduceByKey.of(output)
        .keyBy(p -> toRegionIdUninitialized(bufferedKeys, endKeys, p.getFirst()))
        // FIXME: use raw byte arrays here and rawcomparators to sort it
        // reconstruct the cell afterwards
        .valueBy(Pair::getSecond)
        .reduceBy((s, ctx) -> {
          // this is ugly and we should make it more clear with access to key
          // via #131
          Iterator<Cell> iterator = s.iterator();
          Cell first = iterator.next();
          int id = toRegionIdUninitialized(bufferedKeys, endKeys, ByteBuffer.wrap(
              first.getRowArray(), first.getRowOffset(), first.getRowLength()));
          Writer<Pair<ImmutableBytesWritable, Cell>> writer = HFilesSink.this.openWriter(id);
          ImmutableBytesWritable w = new ImmutableBytesWritable();
          try {
            Cell c = first;
            for (;;) {
              w.set(c.getRowArray(), c.getRowOffset(), c.getRowLength());
              writer.write(Pair.of(w, c));
              if (!iterator.hasNext()) {
                break;
              }
              c = iterator.next();
            }
            writer.close();
            writer.commit();
          } catch (Exception ex) {
            try {
              writer.rollback();
            } catch (IOException ex1) {
              LOG.error("Failed to rollback writer {}", id, ex1);
            }
            throw new RuntimeException(ex);
          }
        })
        .withSortedValues(comparator::compare)
        .output()
        .persist(new VoidSink<Pair<Integer, Object>>() {

          @Override
          public void rollback() throws IOException {
            HFilesSink.this.rollback();
          }

          @Override
          public void commit() throws IOException {
            HFilesSink.this.commit();
          }

        });

    return true;
  }

  private static int toRegionIdUninitialized(
      AtomicReference<ByteBuffer[]> endKeys,
      byte[][] bytesEndKeys,
      ImmutableBytesWritable row) {

    if (endKeys.get() == null) {
      endKeys.set(initialize(bytesEndKeys));
    }
    return toRegionId(endKeys.get(), ByteBuffer.wrap(
        row.get(), row.getOffset(), row.getLength()));
  }

  private static int toRegionIdUninitialized(
      AtomicReference<ByteBuffer[]> endKeys,
      byte[][] bytesEndKeys,
      ByteBuffer row) {

    if (endKeys.get() == null) {
      endKeys.set(initialize(bytesEndKeys));
    }
    return toRegionId(endKeys.get(), row);
  }

  @VisibleForTesting
  static int toRegionId(ByteBuffer[] endKeys, ImmutableBytesWritable row) {
    return toRegionId(endKeys, ByteBuffer.wrap(
        row.get(), row.getOffset(), row.getLength()));
  }

  private static int toRegionId(ByteBuffer[] endKeys, ByteBuffer row) {
    int search = Arrays.binarySearch(endKeys, row);
    return search >= 0 ? search : - (search + 1);
  }

   private static ByteBuffer[] initialize(byte[][] bytesEndKeys) {
    return Arrays.stream(bytesEndKeys)
        .map(ByteBuffer::wrap)
        .toArray(l -> new ByteBuffer[bytesEndKeys.length]);
  }

  private byte[][] getEndKeys(String table, Configuration configuration) {
    try (Connection conn = ConnectionFactory.createConnection(configuration)) {
      RegionLocator locator = conn.getRegionLocator(TableName.valueOf(table));
      byte[][] ends = locator.getEndKeys();
      return Arrays.stream(ends).map(ByteBuffer::wrap)
          .filter(b -> b.remaining() > 0)
          .sorted()
          .map(b -> b.slice().array())
          .toArray(byte[][]::new);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

}
