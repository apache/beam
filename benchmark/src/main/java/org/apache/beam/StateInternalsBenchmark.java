package org.apache.beam;

import avro.shaded.com.google.common.collect.Maps;
import com.alibaba.jstorm.beam.translation.runtime.TimerServiceImpl;
import com.alibaba.jstorm.beam.translation.runtime.state.JStormStateInternals;
import com.alibaba.jstorm.cache.IKvStoreManager;
import com.alibaba.jstorm.cache.rocksdb.RocksDbKvStoreManagerFactory;
import com.alibaba.jstorm.utils.KryoSerializer;
import java.io.Serializable;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.MapState;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Created by peihe on 25/05/2017.
 */
public class StateInternalsBenchmark {

  @State(Scope.Benchmark)
  public static class Config implements Serializable {

    @Setup(Level.Trial)
    public void doSetup() throws Exception {
      // Setup for JStormStateInternals.
      String property = "java.io.tmpdir";
      String tempDir = System.getProperty(property);
      IKvStoreManager kvStoreManager = RocksDbKvStoreManagerFactory.getManager(
          Maps.newHashMap(),
          "test",
          tempDir,
          new KryoSerializer());
      stateInternals =
          new JStormStateInternals("key-1", kvStoreManager, new TimerServiceImpl());
    }

    @TearDown(Level.Trial)
    public void doTearDown() {
      System.out.println("Do TearDown");
    }

    public StateInternals stateInternals;

    @Param({"1000000"})
    public int totalValues;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @Fork(1)
  @Warmup(iterations = 5)
  @Measurement(iterations = 10)
  public void testBagState(Config config) throws Exception {
    BagState<Integer> bagState = config.stateInternals.state(
        StateNamespaces.global(), StateTags.bag("state-id-bag", BigEndianIntegerCoder.of()));

    for (int i = 0; i < config.totalValues; ++i) {
      bagState.add(i);
      bagState.readLater();
    }
    bagState.read();
    bagState.clear();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @Fork(1)
  @Warmup(iterations = 5)
  @Measurement(iterations = 10)
  public void testMapState(Config config) throws Exception {
    MapState<String, Integer> mapState = config.stateInternals.state(
        StateNamespaces.global(),
        StateTags.map("state-id-map", StringUtf8Coder.of(), BigEndianIntegerCoder.of()));

    for (int i = 0; i < config.totalValues; ++i) {
      String key = "key-:" + i;
      mapState.put(key, i);
      mapState.get(key).readLater();
    }

    for (int i = 0; i < config.totalValues; ++i) {
      String key = "key-:" + i;
      mapState.get(key).read();
    }
    mapState.clear();
  }
}
