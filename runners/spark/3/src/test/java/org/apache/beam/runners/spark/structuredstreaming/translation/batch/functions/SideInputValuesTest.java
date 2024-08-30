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
package org.apache.beam.runners.spark.structuredstreaming.translation.batch.functions;

import static org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers.encoderOf;
import static org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers.windowedValueEncoder;
import static org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.seqOf;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.sdk.util.WindowedValue.getFullCoder;
import static org.apache.beam.sdk.util.WindowedValue.valueInGlobalWindow;
import static org.assertj.core.api.Assertions.assertThat;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import javax.annotation.Nullable;
import org.apache.beam.runners.spark.structuredstreaming.SparkSessionRule;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.assertj.core.util.Lists;
import org.joda.time.Instant;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

public class SideInputValuesTest {

  @ClassRule public static final SparkSessionRule SESSION = new SparkSessionRule();

  @ClassRule public static final SparkKryo KRYO = new SparkKryo();

  @Test
  public void globalSideInputValues() {
    Encoder<WindowedValue<String>> enc =
        windowedValueEncoder(encoderOf(String.class), encoderOf(GlobalWindow.class));
    Dataset<WindowedValue<String>> ds =
        dataset(enc, valueInGlobalWindow("a"), valueInGlobalWindow("b"));

    SideInputValues<String> values = new SideInputValues.Global<>("test", StringUtf8Coder.of(), ds);
    assertThat(values.get(GlobalWindow.INSTANCE)).isEqualTo(ImmutableList.of("a", "b"));

    SideInputValues<String> deserialized = KRYO.serde(values);
    assertThat(deserialized).isEqualToIgnoringGivenFields(values, "binaryValues");
    assertThat(deserialized.get(GlobalWindow.INSTANCE)).isEqualTo(ImmutableList.of("a", "b"));
  }

  @Test
  public void windowedSideInputValues() {
    Encoder<WindowedValue<String>> encoder =
        windowedValueEncoder(encoderOf(String.class), encoderOf(IntervalWindow.class));
    Coder<WindowedValue<String>> coder =
        getFullCoder(StringUtf8Coder.of(), IntervalWindow.getCoder());

    Dataset<WindowedValue<String>> ds =
        dataset(
            encoder,
            valueInWindows("a", intervalWindow(0, 1), intervalWindow(1, 2)),
            valueInWindows("b", intervalWindow(1, 2), intervalWindow(2, 3)));

    SideInputValues<String> values = new SideInputValues.ByWindow<>("test", coder, ds);
    assertThat(values.get(intervalWindow(0, 1))).isEqualTo(ImmutableList.of("a"));
    assertThat(values.get(intervalWindow(1, 2))).isEqualTo(ImmutableList.of("a", "b"));
    assertThat(values.get(intervalWindow(2, 3))).isEqualTo(ImmutableList.of("b"));

    SideInputValues<String> deserialized = KRYO.serde(values);
    assertThat(deserialized).isEqualToIgnoringGivenFields(values, "binaryValues");
    assertThat(deserialized.get(intervalWindow(0, 1))).isEqualTo(ImmutableList.of("a"));
    assertThat(deserialized.get(intervalWindow(1, 2))).isEqualTo(ImmutableList.of("a", "b"));
    assertThat(deserialized.get(intervalWindow(2, 3))).isEqualTo(ImmutableList.of("b"));
  }

  private static <T> Dataset<T> dataset(Encoder<T> enc, T... data) {
    return SESSION.getSession().createDataset(seqOf(data), enc);
  }

  private static IntervalWindow intervalWindow(int start, int end) {
    return new IntervalWindow(Instant.ofEpochMilli(start), Instant.ofEpochMilli(end));
  }

  private static <T> WindowedValue<T> valueInWindows(T value, BoundedWindow... windows) {
    return WindowedValue.of(value, Instant.EPOCH, Lists.list(windows), PaneInfo.NO_FIRING);
  }

  public static class SparkKryo extends ExternalResource {
    private @Nullable Kryo kryo = null;

    @Override
    protected void after() {
      kryo = null;
    }

    <T> T serde(T obj) {
      Output out = new Output(128);
      kryo().writeClassAndObject(out, obj);
      return (T) kryo().readClassAndObject(new Input(out.getBuffer(), 0, out.position()));
    }

    Kryo kryo() {
      if (kryo == null) {
        kryo = new KryoSerializer(SESSION.getSession().sparkContext().conf()).newKryo();
      }
      return checkStateNotNull(kryo);
    }
  }
}
