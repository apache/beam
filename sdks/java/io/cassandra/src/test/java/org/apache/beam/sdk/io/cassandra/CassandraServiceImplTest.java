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
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Test;
import org.mockito.Mockito;

/** Tests on {@link CassandraServiceImplTest}. */
public class CassandraServiceImplTest {
  private static final String MURMUR3_PARTITIONER = "org.apache.cassandra.dht.Murmur3Partitioner";

  private Cluster createClusterMock() {
    Metadata metadata = Mockito.mock(Metadata.class);
    KeyspaceMetadata keyspaceMetadata = Mockito.mock(KeyspaceMetadata.class);
    TableMetadata tableMetadata = Mockito.mock(TableMetadata.class);
    ColumnMetadata columnMetadata = Mockito.mock(ColumnMetadata.class);

    Mockito.when(metadata.getPartitioner()).thenReturn(MURMUR3_PARTITIONER);
    Mockito.when(metadata.getKeyspace(Mockito.anyString())).thenReturn(keyspaceMetadata);
    Mockito.when(keyspaceMetadata.getTable(Mockito.anyString())).thenReturn(tableMetadata);
    Mockito.when(tableMetadata.getPartitionKey())
        .thenReturn(Collections.singletonList(columnMetadata));
    Mockito.when(columnMetadata.getName()).thenReturn("$pk");
    Cluster cluster = Mockito.mock(Cluster.class);
    Mockito.when(cluster.getMetadata()).thenReturn(metadata);
    return cluster;
  }

  @Test
  public void testValidPartitioner() {
    assertTrue(CassandraServiceImpl.isMurmur3Partitioner(createClusterMock()));
  }

  @Test
  public void testDistance() {
    BigInteger distance = CassandraServiceImpl.distance(new BigInteger("10"),
        new BigInteger("100"));
    assertEquals(BigInteger.valueOf(90), distance);

    distance = CassandraServiceImpl.distance(new BigInteger("100"), new BigInteger("10"));
    assertEquals(new BigInteger("18446744073709551526"), distance);
  }

  @Test
  public void testRingFraction() {
    // simulate a first range taking "half" of the available tokens
    List<CassandraServiceImpl.TokenRange> tokenRanges = new ArrayList<>();
    tokenRanges.add(new CassandraServiceImpl.TokenRange(1, 1,
        BigInteger.valueOf(Long.MIN_VALUE), new BigInteger("0")));
    assertEquals(0.5, CassandraServiceImpl.getRingFraction(tokenRanges), 0);

    // add a second range to cover all tokens available
    tokenRanges.add(new CassandraServiceImpl.TokenRange(1, 1,
        new BigInteger("0"), BigInteger.valueOf(Long.MAX_VALUE)));
    assertEquals(1.0, CassandraServiceImpl.getRingFraction(tokenRanges), 0);
  }

  @Test
  public void testEstimatedSizeBytes() {
    List<CassandraServiceImpl.TokenRange> tokenRanges = new ArrayList<>();
    // one partition containing all tokens, the size is actually the size of the partition
    tokenRanges.add(new CassandraServiceImpl.TokenRange(1, 1000,
        BigInteger.valueOf(Long.MIN_VALUE), BigInteger.valueOf(Long.MAX_VALUE)));
    assertEquals(1000, CassandraServiceImpl.getEstimatedSizeBytes(tokenRanges));

    // one partition with half of the tokens, we estimate the size to the double of this partition
    tokenRanges = new ArrayList<>();
    tokenRanges.add(new CassandraServiceImpl.TokenRange(1, 1000,
        BigInteger.valueOf(Long.MIN_VALUE), new BigInteger("0")));
    assertEquals(2000, CassandraServiceImpl.getEstimatedSizeBytes(tokenRanges));

    // we have three partitions covering all tokens, the size is the sum of partition size *
    // partition count
    tokenRanges = new ArrayList<>();
    tokenRanges.add(new CassandraServiceImpl.TokenRange(1, 1000,
        BigInteger.valueOf(Long.MIN_VALUE), new BigInteger("-3")));
    tokenRanges.add(new CassandraServiceImpl.TokenRange(1, 1000,
        new BigInteger("-2"), new BigInteger("10000")));
    tokenRanges.add(new CassandraServiceImpl.TokenRange(2, 3000,
        new BigInteger("10001"), BigInteger.valueOf(Long.MAX_VALUE)));
    assertEquals(8000, CassandraServiceImpl.getEstimatedSizeBytes(tokenRanges));
  }
}
