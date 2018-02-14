/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.cassandra;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests on {@link CassandraServiceImplTest}.
 */
public class CassandraServiceImplTest {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraServiceImplTest.class);

  private static final String MURMUR3_PARTITIONER = "org.apache.cassandra.dht.Murmur3Partitioner";

  private Cluster createClusterMock() {
    Metadata metadata = Mockito.mock(Metadata.class);
    Mockito.when(metadata.getPartitioner()).thenReturn(MURMUR3_PARTITIONER);
    Cluster cluster = Mockito.mock(Cluster.class);
    Mockito.when(cluster.getMetadata()).thenReturn(metadata);
    return cluster;
  }

  @Test
  public void testValidPartitioner() throws Exception {
    assertTrue(CassandraServiceImpl.isMurmur3Partitioner(createClusterMock()));
  }

  @Test
  public void testDistance() throws Exception {
    BigInteger distance = CassandraServiceImpl.distance(10L, 100L);
    assertEquals(BigInteger.valueOf(90), distance);

    distance = CassandraServiceImpl.distance(100L, 10L);
    assertEquals(new BigInteger("18446744073709551525"), distance);
  }

  @Test
  public void testRingFraction() throws Exception {
    // simulate a first range taking "half" of the available tokens
    List<CassandraServiceImpl.TokenRange> tokenRanges = new ArrayList<>();
    tokenRanges.add(new CassandraServiceImpl.TokenRange(1, 1, Long.MIN_VALUE, 0));
    assertEquals(0.5, CassandraServiceImpl.getRingFraction(tokenRanges), 0);

    // add a second range to cover all tokens available
    tokenRanges.add(new CassandraServiceImpl.TokenRange(1, 1, 0, Long.MAX_VALUE));
    assertEquals(1.0, CassandraServiceImpl.getRingFraction(tokenRanges), 0);
  }

  @Test
  public void testEstimatedSizeBytes() throws Exception {
    List<CassandraServiceImpl.TokenRange> tokenRanges = new ArrayList<>();
    // one partition containing all tokens, the size is actually the size of the partition
    tokenRanges.add(new CassandraServiceImpl.TokenRange(1, 1000, Long.MIN_VALUE, Long.MAX_VALUE));
    assertEquals(1000, CassandraServiceImpl.getEstimatedSizeBytes(tokenRanges));

    // one partition with half of the tokens, we estimate the size to the double of this partition
    tokenRanges = new ArrayList<>();
    tokenRanges.add(new CassandraServiceImpl.TokenRange(1, 1000, Long.MIN_VALUE, 0));
    assertEquals(2000, CassandraServiceImpl.getEstimatedSizeBytes(tokenRanges));

    // we have three partitions covering all tokens, the size is the sum of partition size *
    // partition count
    tokenRanges = new ArrayList<>();
    tokenRanges.add(new CassandraServiceImpl.TokenRange(1, 1000, Long.MIN_VALUE, -3));
    tokenRanges.add(new CassandraServiceImpl.TokenRange(1, 1000, -2, 10000));
    tokenRanges.add(new CassandraServiceImpl.TokenRange(2, 3000, 10001, Long.MAX_VALUE));
    assertEquals(8000, CassandraServiceImpl.getEstimatedSizeBytes(tokenRanges));
  }

  @Test
  public void testThreeSplits() throws Exception {
    CassandraServiceImpl service = new CassandraServiceImpl();
    CassandraIO.Read spec = CassandraIO.read().withKeyspace("beam").withTable("test");
    List<CassandraIO.CassandraSource> sources = service.split(spec, 50, 150);
    assertEquals(3, sources.size());
    assertTrue(sources.get(0).splitQuery.matches("SELECT \\* FROM beam.test WHERE token\\"
        + "(\\$pk\\)<(.*)"));
    assertTrue(sources.get(1).splitQuery.matches("SELECT \\* FROM beam.test WHERE token\\"
        + "(\\$pk\\)>=(.*) AND token\\(\\$pk\\)<(.*)"));
    assertTrue(sources.get(2).splitQuery.matches("SELECT \\* FROM beam.test WHERE token\\"
        + "(\\$pk\\)>=(.*)"));
  }

  @Test
  public void testTwoSplits() throws Exception {
    CassandraServiceImpl service = new CassandraServiceImpl();
    CassandraIO.Read spec = CassandraIO.read().withKeyspace("beam").withTable("test");
    List<CassandraIO.CassandraSource> sources = service.split(spec, 50, 100);
    assertEquals(2, sources.size());
    LOG.info("TOKEN: " + ((double) Long.MAX_VALUE / 2));
    LOG.info(sources.get(0).splitQuery);
    LOG.info(sources.get(1).splitQuery);
    assertEquals("SELECT * FROM beam.test WHERE token($pk)<" + ((double) Long.MAX_VALUE / 2) + ";",
        sources.get(0).splitQuery);
    assertEquals("SELECT * FROM beam.test WHERE token($pk)>=" + ((double) Long.MAX_VALUE / 2)
            + ";",
        sources.get(1).splitQuery);
  }

  @Test
  public void testUniqueSplit() throws Exception {
    CassandraServiceImpl service = new CassandraServiceImpl();
    CassandraIO.Read spec = CassandraIO.read().withKeyspace("beam").withTable("test");
    List<CassandraIO.CassandraSource> sources = service.split(spec, 100, 100);
    assertEquals(1, sources.size());
    assertEquals("SELECT * FROM beam.test;", sources.get(0).splitQuery);
  }

}
