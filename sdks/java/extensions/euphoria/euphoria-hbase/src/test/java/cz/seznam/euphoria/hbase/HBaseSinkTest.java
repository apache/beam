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
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.executor.local.LocalExecutor;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

/**
 * Test suite for {@code HBaseSink}.
 */
public class HBaseSinkTest extends HBaseTestCase {

  private DataSink<Put> sink;
  private Flow flow;

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    sink = HBaseSink.newBuilder()
        .withConfiguration(cluster.getConfiguration())
        .withTable("test")
        .build();
    flow = Flow.create();
  }

  @Test
  public void testTestSimpleOutput() throws IOException {
    List<String> data = Arrays.asList("a", "b", "c");
    ListDataSource<String> source = ListDataSource.unbounded(data);
    Dataset<String> input = flow.createInput(source);
    MapElements.of(input)
        .using(HBaseTestCase::put)
        .output()
        .persist(sink);

    new LocalExecutor().submit(flow).join();

    for (String v : data) {
      assertArrayEquals(b(v), get(v));
    }
  }

  private byte[] get(String key) throws IOException {
    Get get = new Get(b(key));
    get.addColumn(b("t"), b(key));
    Result res = client.get(get);
    return res.getColumnLatestCell(b("t"), b(key)).getValue();
  }

}
