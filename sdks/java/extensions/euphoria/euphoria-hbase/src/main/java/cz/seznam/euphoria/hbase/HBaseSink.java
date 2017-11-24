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
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.hadoop.output.HadoopSink;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;

/**
 * Direct sink to HBase using RPCs.
 */
public class HBaseSink extends HadoopSink<Object, Mutation> {

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    Configuration conf = HBaseConfiguration.create();
    String table = null;
    List<Update<Configuration>> updaters = new ArrayList<>();

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
     * @param quorum
     * @return this
     */
    public Builder withQuorum(String quorum) {
      updaters.add(c -> c.set(TableOutputFormat.QUORUM_ADDRESS, quorum));
      return this;
    }

    /**
     * Build the output sink.
     * @return  the built output sink
     */
    public HBaseSink build() {
      Preconditions.checkArgument(table != null, "Specify table by call to `withTable`");
      updaters.add(c -> c.set(TableOutputFormat.OUTPUT_TABLE, table));
      return new HBaseSink(conf, updaters);
    }

    /**
     * Build sink for raw mutations without any key.
     */
    @SuppressWarnings("unchecked")
    public DataSink<Mutation> buildRaw() {
      return build().setRaw();
    }

  }

  private boolean raw;

  @SuppressWarnings("unchecked")
  HBaseSink(Configuration conf, List<Update<Configuration>> updaters) {

    super((Class) TableOutputFormat.class, toConf(conf, updaters));
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean prepareDataset(Dataset output) {
    if (!raw) {
      return false;
    }
    MapElements.of(output)
        .using(i -> Pair.of((Object) "", i))
        .output()
        .persist(this);
    return true;
  }


  @SuppressWarnings("unchecked")
  private DataSink<Mutation> setRaw() {
    this.raw = true;
    return (DataSink) this;
  }

  private static Configuration toConf(
      Configuration conf, List<Update<Configuration>> updaters) {

    Configuration ret = HBaseConfiguration.create(conf);
    for (Update<Configuration> u : updaters) {
      try {
        u.update(ret);
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
    return ret;
  }

}
