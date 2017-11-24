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
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.executor.local.LocalExecutor;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test {@code HFilesSink}.
 */
public class HFilesSinkTest extends HBaseTestCase {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  final TableName table = TableName.valueOf("test");

  Flow flow;

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    flow = Flow.create();
  }

  @After
  @Override
  public void tearDown() throws IOException {
    super.tearDown();
  }

  @Override
  TableName createTable() throws IOException {
    Admin admin = conn.getAdmin();
    HTableDescriptor desc = new HTableDescriptor(table)
        .addFamily(new HColumnDescriptor("t"));
    admin.createTable(desc, new byte[][] { b("bbb"), b("d") });
    return table;
  }


  @Test
  public void testRegionAssignment() {

    ByteBuffer[] endKeys = Stream.of("b", "dd", "fff")
        .map(c -> ByteBuffer.wrap(b(c)))
        .toArray(l -> new ByteBuffer[l]);

    assertEquals(0, HFilesSink.toRegionId(endKeys, ibw("a")));
    assertEquals(0, HFilesSink.toRegionId(endKeys, ibw("b")));
    assertEquals(1, HFilesSink.toRegionId(endKeys, ibw("c")));
    assertEquals(1, HFilesSink.toRegionId(endKeys, ibw("dd")));
    assertEquals(2, HFilesSink.toRegionId(endKeys, ibw("ddd")));
    assertEquals(2, HFilesSink.toRegionId(endKeys, ibw("e")));
    assertEquals(2, HFilesSink.toRegionId(endKeys, ibw("fff")));
    assertEquals(3, HFilesSink.toRegionId(endKeys, ibw("ffff")));
    assertEquals(3, HFilesSink.toRegionId(endKeys, ibw("gg")));
  }

  @Test
  public void testWrite() throws IOException {
    List<String> inputs = Stream.of("a", "b", "bbb", "bbbb", "c", "xy")
        .sorted(Comparator.reverseOrder())
        .collect(Collectors.toList());

    File tmp = folder.newFolder();
    tmp.deleteOnExit();
    ListDataSource<String> source = ListDataSource.unbounded(inputs);
    Dataset<String> input = flow.createInput(source);
    MapElements.of(input)
        .using(s -> kv(s))
        .output()
        .persist(HFilesSink.newBuilder()
            .withTable(table.getNameAsString())
            .withConfiguration(cluster.getConfiguration())
            .withOutputPath(new Path("file://" + tmp.getPath()))
            .build());

    new LocalExecutor().submit(flow).join();

    // we should have three output files in "t" column family
    assertEquals(3, new File(tmp, "t").listFiles().length);
    // we should have success marker
    assertTrue(new File(tmp, "_SUCCESS").exists());

  }

}
