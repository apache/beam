/*
 * Copyright 2016-2018 Seznam.cz, a.s.
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

import cz.seznam.euphoria.core.client.functional.VoidFunction;
import cz.seznam.euphoria.hadoop.input.HadoopSource;
import cz.seznam.euphoria.shadow.com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;

/**
 * A {@code BoundedDataSource} for HBase.
 */
public class HBaseSource extends HadoopSource<ImmutableBytesWritable, Result> {

  static final Charset DEFAULT = Charset.forName("UTF-8");

  /**
   * Function that can be configured to be applied on scanner before running the
   * job.
   */
  @FunctionalInterface
  public interface ScanUpdate extends Update<Scan> {
  }

  /**
   * Create builder for the sink.
   * @return new builder for {@link HBaseSource}
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder for specification of the source.
   */
  public static class Builder {

    private Configuration conf = HBaseConfiguration.create();
    private final List<ScanUpdate> scanUpdaters = new ArrayList<>();
    private String table = null;
    private String quorum = null;
    private VoidFunction<Scan> scanFactory = Scan::new;
    private String znodeParent = null;

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
     * Specify source table.
     * @param table the table to read
     * @return this
     */
    public Builder withTable(String table) {
      this.table = table;
      return this;
    }

    /**
     * Specify zookeeper quorum.
     * @param quorum the quorum to use
     * @return this
     */
    public Builder withZookeeperQuorum(String quorum) {
      this.quorum = quorum;
      return this;
    }

    /**
     * Set parent znode for HBase zookeeper configuration.
     * @param parent the parent znode
     * @return this
     */
    public Builder withZnodeParent(String parent) {
      this.znodeParent = parent;
      return this;
    }

    /**
     * Add single whole family to the input.
     * @param family family to add
     * @return this
     */
    public Builder addFamily(byte[] family) {
      scanUpdaters.add(s -> s.addFamily(family));
      return this;
    }

    /**
     * Add single whole family to the input.
     * @param family family to add
     * @return this
     */
    public Builder addFamily(String family) {
      return addFamily(family.getBytes(DEFAULT));
    }

    /**
     * Add specific families to the input.
     * @param families whole families to add to the input
     * @return this
     */
    @SafeVarargs
    public final Builder addFamilies(final byte[]... families) {
      for (byte[] family : families) {
        addFamily(family);
      }
      return this;
    }

    /**
     * Add specific families to the input.
     * @param families whole families to add to the input
     * @return this
     */
    public Builder addFamilies(String... families) {
      for (String f : families) {
        addFamilies(f.getBytes(DEFAULT));
      }
      return this;
    }

    /**
     * Add family with specified qualifiers.
     * @param family family of the columns
     * @param qualifiers columns to add from the family
     * @return this
     */
    public Builder addColumns(byte[] family, byte[]... qualifiers) {
      for (byte[] q : qualifiers) {
        addColumn(family, q);
      }
      return this;
    }

    /**
     * Add family with specified qualifiers.
     * @param family family of the columns
     * @param qualifiers columns to add from the family
     * @return this
     */
    public Builder addColumns(String family, String... qualifiers) {
      for (String q : qualifiers) {
        addColumn(family, q);
      }
      return this;
    }

    /**
     * Add specific column to the input.
     * @param family
     * @param qualifier
     * @return this
     */
    private Builder addColumn(byte[] family, byte[] qualifier) {
      scanUpdaters.add(s -> s.addColumn(family, qualifier));
      return this;
    }

    /**
     * Add specific column to the input.
     * @param family family to read
     * @param qualifier qualifier to read
     * @return this
     */
    public Builder addColumn(String family, String qualifier) {
      return addColumn(family.getBytes(DEFAULT), qualifier.getBytes(DEFAULT));
    }


    /**
     * Specify scanner caching
     * @param caching scan caching
     * @return this
     */
    public Builder withScanCaching(int caching) {
      scanUpdaters.add(s -> s.setCaching(caching));
      return this;
    }

    /**
     * Specify batch size.
     * @param batchSize scan batch size
     * @return this
     */
    public Builder withBatchSize(int batchSize) {
      scanUpdaters.add(s -> s.setBatch(batchSize));
      return this;
    }

    /**
     * Specify a time range of the scanning.
     * @param range the range
     * @return this
     */
    public Builder withTimeRange(TimeRange range) {
      return withTimeRange(range.getMin(), range.getMax());
    }

    /**
     * Specify a time range of the scanning.
     * @param minStamp minimal timestamp
     * @param maxStamp maximal timestamp
     * @return this
     */
    public Builder withTimeRange(long minStamp, long maxStamp) {
      scanUpdaters.add(s -> s.setTimeRange(minStamp, maxStamp));
      return this;
    }

    /**
     * Specify startRow.
     * @param startRow scan start row
     * @return  this
     */
    public Builder withStartRow(byte[] startRow) {
      scanFactory = () -> new Scan(startRow);
      return this;
    }

    /**
     * Specify start and stop row
     * @param startRow the start row
     * @param stopRow the stop row
     * @return this
     */
    public Builder withStartStopRow(byte[] startRow, byte[] stopRow) {
      scanFactory = () -> new Scan(startRow, stopRow);
      return this;
    }

    /**
     * Add a generic function to be applied to the scan after being created.
     * @param update the function to apply to scan before being submitted
     * @return this
     */
    public Builder withScanUpdator(ScanUpdate update) {
      scanUpdaters.add(update);
      return this;
    }


    /**
     * Build the source.
     * @return the source
     */
    public HBaseSource build() {
      Preconditions.checkArgument(
          table != null, "Please specify source table by call to `usingTable`");
      conf = new Configuration(conf);
      if (quorum != null) {
        conf.set(HConstants.ZOOKEEPER_QUORUM, quorum);
      }
      if (znodeParent != null) {
        conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, znodeParent);
      }
      return new HBaseSource(conf, table, scanFactory, scanUpdaters);
    }

  }

  /**
   * Create {@code HBaseSource} with specified configuration.
   * @param conf the configuration to use
   * @param table the table to scan
   * @param scanUpdaters functions to be applied on created scanner
   */
  private HBaseSource(Configuration conf,
      String table,
      VoidFunction<Scan> scanFactory,
      List<ScanUpdate> scanUpdaters) {

    super(ImmutableBytesWritable.class, Result.class,
        TableInputFormat.class, setupConf(table, scanFactory, scanUpdaters, conf));
  }

  private static Configuration setupConf(
      String table,
      VoidFunction<Scan> scanFactory,
      List<ScanUpdate> scanUpdaters,
      Configuration conf) {

    Configuration ret = HBaseConfiguration.create(conf);
    // create scan
    Scan scan = scanFactory.apply();
    // modify it according to the specification
    scanUpdaters.forEach(u -> {
      try {
        u.update(scan);
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    });

    try {
      // create fake job, that will be thrown away
      Job job = Job.getInstance(ret);
      // setup the configuration
      TableMapReduceUtil.initTableMapperJob(
          table, scan, TableMapper.class, NullWritable.class, NullWritable.class, job);

      return job.getConfiguration();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }

  }

}
