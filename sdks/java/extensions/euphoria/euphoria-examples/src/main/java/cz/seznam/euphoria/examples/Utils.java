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
package cz.seznam.euphoria.examples;

import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.DataSinks;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.hadoop.output.SequenceFileSink;
import cz.seznam.euphoria.hbase.HBaseSource;
import cz.seznam.euphoria.kafka.KafkaSink;
import cz.seznam.euphoria.shadow.com.google.common.base.Preconditions;
import cz.seznam.euphoria.shadow.com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.KeyValueSerialization.KeyValueDeserializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Various utilities for examples.
 */
class Utils {

  static AtomicReference<KeyValueDeserializer> cache = new AtomicReference<>();

  static String getPath(URI uri) {
    String path = uri.getPath();
    Preconditions.checkArgument(path.length() > 1, "Path must not be empty.");
    return path.substring(1);
  }

  static Map<String, List<String>> splitQuery(String query) {
    if (Strings.isNullOrEmpty(query)) {
      return Collections.emptyMap();
    }
    return Arrays.stream(query.split("&"))
        .map(Utils::splitQueryParameter)
        .collect(Collectors.groupingBy(Pair::getFirst, HashMap::new,
            Collectors.mapping(Pair::getSecond, Collectors.toList())));
  }

  static Pair<String, String> splitQueryParameter(String it) {
    final int idx = it.indexOf("=");
    final String key = idx > 0 ? it.substring(0, idx) : it;
    final String value = idx > 0 && it.length() > idx + 1 ? it.substring(idx + 1) : null;
    try {
      return Pair.of(
          URLDecoder.decode(key, "UTF-8"),
          value == null ? null : URLDecoder.decode(value, "UTF-8"));
    } catch (UnsupportedEncodingException ex) {
      throw new RuntimeException(ex);
    }
  }

  static String getZnodeParent(URI uri) {
    Map<String, List<String>> query = splitQuery(uri.getQuery());
    return Optional.ofNullable(query.get("znode"))
        .map(l -> l.get(0))
        .orElse(null);
  }

  static Cell toCell(byte[] input) {
    try {
      if (cache.get() == null) {
        cache.set(new KeyValueDeserializer());
      }
      KeyValueDeserializer d = cache.get();
      ByteArrayInputStream bais = new ByteArrayInputStream(input);
      d.open(bais);
      KeyValue ret = d.deserialize(new KeyValue());
      d.close();
      return ret;
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }

  }

  static HBaseSource getHBaseSource(URI input, Configuration conf) {

    HBaseSource.Builder builder = HBaseSource
        .newBuilder()
        .withConfiguration(conf)
        .withZookeeperQuorum(input.getAuthority())
        .withZnodeParent(getZnodeParent(input));

    String path = getPath(input);
    if (!path.isEmpty()) {
      String[] split = path.split("/", 2);
      if (split.length == 2) {
        builder.withTable(split[0]);
        String[] parts = split[1].split(",");
        for (String part : parts) {
          if (!part.isEmpty()) {
            String[] cf = part.split(":");
            if (cf.length == 1) {
              builder.addFamily(cf[0]);
            } else {
              builder.addColumn(cf[0], cf[1]);
            }
          }
        }
        return builder.build();
      }
    }

    throw new IllegalArgumentException("Invalid input URI, expected "
        + "hbase://<zookeeper_quorum>/table/[<family>[:<qualifier>],]+");
  }

  static DataSink<byte[]> getSink(URI output, Configuration conf) {
    switch (output.getScheme()) {
      case "hdfs":
      case "file":
        return DataSinks.mapping(
            SequenceFileSink
                .of(ImmutableBytesWritable.class, ImmutableBytesWritable.class)
                .outputPath(output.toString())
                .withConfiguration(conf)
                .build(),
            b -> Pair.of(new ImmutableBytesWritable(), new ImmutableBytesWritable(b)));
      case "kafka":
        return DataSinks.mapping(
            new KafkaSink(output.getAuthority(), getPath(output), toSettings(conf)),
            b -> Pair.of(new byte[0], b));
    }
    throw new IllegalArgumentException("Unknown scheme in " + output);
  }

  private static Settings toSettings(Configuration conf) {
    Settings ret = new Settings();
    for (Map.Entry<String, String> e : conf) {
      ret.setString(e.getKey(), e.getValue());
    }
    return ret;
  }

  private Utils() { }
}
