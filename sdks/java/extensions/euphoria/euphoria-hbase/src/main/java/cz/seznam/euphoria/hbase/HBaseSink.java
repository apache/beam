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

import com.google.common.base.Preconditions;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.Writer;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.hadoop.output.HadoopSink;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;

import java.io.IOException;
import org.apache.hadoop.hbase.HConstants;

/**
 * Direct sink to HBase using RPCs.
 */
public class HBaseSink<T extends Mutation> implements DataSink<T> {

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private Configuration conf = null;
    private String table = null;
    private String quorum = null;
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
     * Specify target table.
     * @param table the table to read
     * @return this
     */
    public Builder withTable(String table) {
      this.table = table;
      return this;
    }

    /**
     * Specify zookeeper quorum.
     * @param quorum the zookeeper quorum
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
     * Build the output sink.
     * @return the built output sink
     */
    public <T extends Mutation> HBaseSink<T> build() {
      Preconditions.checkArgument(table != null, "Specify table by call to `withTable`");
      conf = conf == null
          ? HBaseConfiguration.create()
          : HBaseConfiguration.create(conf);
      if (quorum != null) {
        conf.set(TableOutputFormat.QUORUM_ADDRESS, quorum);
      }
      if (znodeParent != null) {
        conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, znodeParent);
      }
      conf.set(TableOutputFormat.OUTPUT_TABLE, table);
      return new HBaseSink<>(new WrapperSink<>(conf));
    }

  }

  private static class WrapperSink<T extends Mutation> extends HadoopSink<Object, T> {

    @SuppressWarnings("unchecked")
    WrapperSink(Configuration conf) {
      super((Class) TableOutputFormat.class, conf);
    }

  }

  private static final Object EMPTY = new Object();

  private final WrapperSink<T> wrapped;

  HBaseSink(WrapperSink<T> wrapped) {
    this.wrapped = wrapped;
  }

  @Override
  public Writer<T> openWriter(int partitionId) {
    throw new UnsupportedOperationException("Unsupported");
  }

  @Override
  public boolean prepareDataset(Dataset<T> input) {
    MapElements.of(input)
        .using(i -> Pair.of(EMPTY, i))
        .output()
        .persist(wrapped);
    return true;
  }

  @Override
  public void commit() throws IOException {
    wrapped.commit();
  }

  @Override
  public void rollback() throws IOException {
    wrapped.rollback();
  }

}
