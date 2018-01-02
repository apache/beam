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

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.executor.local.LocalExecutor;
import cz.seznam.euphoria.hbase.util.ResultUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

/**
 * Integration utility for {@code HBaseSource}.
 */
public class HBaseSourceTest extends HBaseTestCase {

  private HBaseSource source;
  private Flow flow;

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    source = HBaseSource.newBuilder().addFamily("t")
        .withConfiguration(cluster.getConfiguration())
        .withTable("test")
        .build();
    flow = Flow.create();
  }

  @Test
  public void testReading() throws IOException {
    Put p = new Put(b("test"));
    KeyValue kv = new KeyValue(
        b("test"), b("t"), b("col"), System.currentTimeMillis(), b("value"));
    p.add(kv);
    client.put(p);

    ListDataSink<KeyValue> sink = ListDataSink.get();
    Dataset<Pair<ImmutableBytesWritable, Result>> input = flow.createInput(source);
    Dataset<Cell> cells = FlatMap.of(input)
        .using(ResultUtil.toCells())
        .output();
    MapElements.of(cells)
        .using(c -> (KeyValue) c)
        .output()
        .persist(sink);

    new LocalExecutor().submit(flow).join();

    assertEquals(Collections.singletonList(kv), sink.getOutputs());
  }

}
