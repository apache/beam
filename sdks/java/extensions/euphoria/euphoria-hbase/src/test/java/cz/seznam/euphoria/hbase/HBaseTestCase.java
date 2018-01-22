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

import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.security.User;
import org.junit.After;
import org.junit.Before;

/**
 * Class encapsulating creation of hbase cluster.
 */
public class HBaseTestCase {

  HBaseTestingUtility utility;
  MiniHBaseCluster cluster;
  Connection conn;
  Table client;

  @Before
  public void setUp() throws Exception {
    utility = new HBaseTestingUtility();
    cluster = utility.startMiniCluster();
    additionalConf();
    conn = ConnectionFactory.createConnection(cluster.getConfiguration());
    TableName table = createTable();
    client = conn.getTable(table);
  }

  protected void additionalConf() throws Exception {
    // by default empty
  }

  @After
  public void tearDown() throws IOException {
    client.close();
    conn.close();
    cluster.shutdown();
  }

  /**
   * Create table and return its name.
   * @return name of the table created
   * @throws IOException
   */
  TableName createTable() throws IOException {
    Admin admin = conn.getAdmin();
    TableName table = TableName.valueOf("test");
    admin.createTable(new HTableDescriptor(table)
        .addFamily(new HColumnDescriptor("t")));
    return table;
  }

  static byte[] b(String str) {
    return str.getBytes(HBaseSource.DEFAULT);
  }

  static ImmutableBytesWritable ibw(String s) {
    return new ImmutableBytesWritable(b(s));
  }

  static Cell kv(String s) {
    byte[] bytes = b(s);
    long stamp = System.currentTimeMillis();
    byte[] family = b("t");
    return new KeyValue(bytes, family, bytes, stamp, bytes);
  }

  static Put put(String s) {
    byte[] bytes = b(s);
    long stamp = System.currentTimeMillis();
    byte[] family = b("t");
    Put ret = new Put(bytes);
    ret.addColumn(family, bytes, stamp, bytes);
    return ret;
  }

  byte[] get(String key) {
    try {
      Get get = new Get(b(key));
      get.addColumn(b("t"), b(key));
      Result res = client.get(get);
      return res.getColumnLatestCell(b("t"), b(key)).getValue();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

}
