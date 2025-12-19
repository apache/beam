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
package org.apache.beam.runners.dataflow.worker.windmill.state;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateNamespaces.GlobalNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.dataflow.worker.WindmillNamespacePrefix;
import org.apache.beam.runners.dataflow.worker.WindmillTimeUtils;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.Timer;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Enclosed.class)
public class WindmillTagEncodingV2Test {

  private static final IntervalWindow INTERVAL_WINDOW =
      new IntervalWindow(new Instant(10), new Instant(20));

  private static final CustomWindow CUSTOM_WINDOW = new CustomWindow(INTERVAL_WINDOW);

  private static final int TRIGGER_INDEX = 5;

  private static final StateNamespace GLOBAL_NAMESPACE = new GlobalNamespace();

  private static final StateNamespace INTERVAL_WINDOW_NAMESPACE =
      StateNamespaces.window(IntervalWindow.getCoder(), INTERVAL_WINDOW);
  private static final StateNamespace INTERVAL_WINDOW_AND_TRIGGER_NAMESPACE =
      StateNamespaces.windowAndTrigger(IntervalWindow.getCoder(), INTERVAL_WINDOW, TRIGGER_INDEX);

  private static final StateNamespace OTHER_WINDOW_NAMESPACE =
      StateNamespaces.window(new CustomWindow.CustomWindowCoder(), CUSTOM_WINDOW);
  private static final StateNamespace OTHER_WINDOW_AND_TRIGGER_NAMESPACE =
      StateNamespaces.windowAndTrigger(
          new CustomWindow.CustomWindowCoder(), CUSTOM_WINDOW, TRIGGER_INDEX);

  // Generate a tag with length > 256, so length is encoded in two bytes.
  private static final String TAG =
      IntStream.of(300).mapToObj(i -> "a").collect(Collectors.joining());

  private static final StateTag<ValueState<Integer>> USER_STATE_TAG =
      StateTags.value(TAG, VarIntCoder.of());
  private static final StateTag<ValueState<Integer>> SYSTEM_STATE_TAG =
      StateTags.makeSystemTagInternal(StateTags.value(TAG, VarIntCoder.of()));

  private static final ByteString TAG_BYTES = encode(StringUtf8Coder.of(), TAG);

  private static final ByteString SYSTEM_STATE_TAG_BYTES =
      ByteString.copyFrom(new byte[] {1}) // system tag
          .concat(TAG_BYTES);
  private static final ByteString USER_STATE_TAG_BYTES =
      ByteString.copyFrom(new byte[] {2}) // user tag
          .concat(TAG_BYTES);

  private static final ByteString GLOBAL_NAMESPACE_BYTES =
      ByteString.copyFrom(new byte[] {0x1}); // global namespace

  private static final ByteString INTERVAL_WINDOW_BYTES =
      ByteString.EMPTY
          .concat(encode(InstantCoder.of(), INTERVAL_WINDOW.end()))
          .concat(encode(InstantCoder.of(), INTERVAL_WINDOW.start()));

  private static final ByteString INTERVAL_WINDOW_NAMESPACE_BYTES =
      ByteString.copyFrom(new byte[] {0x10}) // non global namespace
          .concat(ByteString.copyFrom(new byte[] {0x64})) // interval window
          .concat(INTERVAL_WINDOW_BYTES)
          .concat(ByteString.copyFrom(new byte[] {0x01})); // window namespace

  private static final ByteString INTERVAL_WINDOW_AND_TRIGGER_NAMESPACE_BYTES =
      ByteString.copyFrom(new byte[] {0x10}) // non global namespace
          .concat(ByteString.copyFrom(new byte[] {0x64})) // interval window
          .concat(INTERVAL_WINDOW_BYTES)
          .concat(ByteString.copyFrom(new byte[] {0x02})) // window and trigger namespace
          .concat(
              ByteString.copyFrom(new byte[] {0x00, 0x00, 0x00, 0x05})); // big endian trigger index

  private static final ByteString OTHER_WINDOW_NAMESPACE_BYTES =
      ByteString.copyFrom(new byte[] {0x10}) // non global namespace
          .concat(ByteString.copyFrom(new byte[] {0x02})) // non interval window
          .concat(encode(new CustomWindow.CustomWindowCoder(), new CustomWindow(INTERVAL_WINDOW)))
          .concat(ByteString.copyFrom(new byte[] {0x01})); // window namespace

  private static final ByteString OTHER_WINDOW_AND_TRIGGER_NAMESPACE_BYTES =
      ByteString.copyFrom(new byte[] {0x10}) // non global namespace
          .concat(ByteString.copyFrom(new byte[] {0x02})) // non interval window
          .concat(encode(new CustomWindow.CustomWindowCoder(), new CustomWindow(INTERVAL_WINDOW)))
          .concat(ByteString.copyFrom(new byte[] {0x02})) // window and trigger namespace
          .concat(
              ByteString.copyFrom(new byte[] {0x00, 0x00, 0x00, 0x05})); // big endian trigger index

  private static final String TIMER_FAMILY_ID = "timerFamily";
  private static final ByteString TIMER_FAMILY_ID_BYTES =
      encode(StringUtf8Coder.of(), TIMER_FAMILY_ID);

  private static final String TIMER_ID = "timerId";
  private static final ByteString TIMER_ID_BYTES = encode(StringUtf8Coder.of(), TIMER_ID);

  private static final ByteString SYSTEM_TIMER_BYTES =
      ByteString.copyFrom(new byte[] {0x3}) // system timer
          .concat(TIMER_FAMILY_ID_BYTES)
          .concat(TIMER_ID_BYTES);

  private static final ByteString USER_TIMER_BYTES =
      ByteString.copyFrom(new byte[] {0x4}) // user timer
          .concat(TIMER_FAMILY_ID_BYTES)
          .concat(TIMER_ID_BYTES);

  private static final ByteString SYSTEM_TIMER_BYTES_NO_FAMILY_ID =
      ByteString.copyFrom(new byte[] {0x3}) // system timer
          .concat(encode(StringUtf8Coder.of(), ""))
          .concat(TIMER_ID_BYTES);

  private static final ByteString USER_TIMER_BYTES_NO_FAMILY_ID =
      ByteString.copyFrom(new byte[] {0x4}) // user timer
          .concat(encode(StringUtf8Coder.of(), ""))
          .concat(TIMER_ID_BYTES);

  @RunWith(Parameterized.class)
  public static class EncodeStateTagTest {
    @Parameters(name = "{index}: namespace={0} stateTag={1} expectedBytes={2}")
    public static Collection<Object[]> data() {
      return ImmutableList.of(
          new Object[] {
            GLOBAL_NAMESPACE, USER_STATE_TAG, GLOBAL_NAMESPACE_BYTES.concat(USER_STATE_TAG_BYTES)
          },
          new Object[] {
            GLOBAL_NAMESPACE,
            SYSTEM_STATE_TAG,
            GLOBAL_NAMESPACE_BYTES.concat(SYSTEM_STATE_TAG_BYTES)
          },
          new Object[] {
            INTERVAL_WINDOW_NAMESPACE,
            USER_STATE_TAG,
            INTERVAL_WINDOW_NAMESPACE_BYTES.concat(USER_STATE_TAG_BYTES)
          },
          new Object[] {
            INTERVAL_WINDOW_AND_TRIGGER_NAMESPACE,
            USER_STATE_TAG,
            INTERVAL_WINDOW_AND_TRIGGER_NAMESPACE_BYTES.concat(USER_STATE_TAG_BYTES)
          },
          new Object[] {
            OTHER_WINDOW_NAMESPACE,
            USER_STATE_TAG,
            OTHER_WINDOW_NAMESPACE_BYTES.concat(USER_STATE_TAG_BYTES)
          },
          new Object[] {
            OTHER_WINDOW_AND_TRIGGER_NAMESPACE,
            USER_STATE_TAG,
            OTHER_WINDOW_AND_TRIGGER_NAMESPACE_BYTES.concat(USER_STATE_TAG_BYTES)
          });
    }

    @Parameter(0)
    public StateNamespace namespace;

    @Parameter(1)
    public StateTag<?> stateTag;

    @Parameter(2)
    public ByteString expectedBytes;

    @Test
    public void testStateTag() {
      assertEquals(
          expectedBytes,
          WindmillTagEncodingV2.instance().stateTag(namespace, stateTag).byteString());
    }
  }

  @RunWith(Parameterized.class)
  public static class TimerTagTest {
    @Parameters(
        name =
            "{index}: namespace={0} prefix={1} expectedBytes={2} includeTimerId={3}"
                + " includeTimerFamilyId={4} timeDomain={4}")
    public static Collection<Object[]> data() {
      List<Object[]> data = new ArrayList<>();
      for (boolean includeTimerFamilyId : ImmutableList.of(true, false)) {
        ByteString expectedSystemTimerBytes =
            includeTimerFamilyId ? SYSTEM_TIMER_BYTES : SYSTEM_TIMER_BYTES_NO_FAMILY_ID;
        ByteString expectedUserTimerBytes =
            includeTimerFamilyId ? USER_TIMER_BYTES : USER_TIMER_BYTES_NO_FAMILY_ID;
        List<Object[]> tests =
            ImmutableList.of(
                new Object[] {
                  GLOBAL_NAMESPACE,
                  WindmillNamespacePrefix.USER_NAMESPACE_PREFIX,
                  GLOBAL_NAMESPACE_BYTES.concat(expectedUserTimerBytes)
                },
                new Object[] {
                  GLOBAL_NAMESPACE,
                  WindmillNamespacePrefix.SYSTEM_NAMESPACE_PREFIX,
                  GLOBAL_NAMESPACE_BYTES.concat(expectedSystemTimerBytes)
                },
                new Object[] {
                  INTERVAL_WINDOW_NAMESPACE,
                  WindmillNamespacePrefix.USER_NAMESPACE_PREFIX,
                  INTERVAL_WINDOW_NAMESPACE_BYTES.concat(expectedUserTimerBytes)
                },
                new Object[] {
                  INTERVAL_WINDOW_NAMESPACE,
                  WindmillNamespacePrefix.SYSTEM_NAMESPACE_PREFIX,
                  INTERVAL_WINDOW_NAMESPACE_BYTES.concat(expectedSystemTimerBytes)
                },
                new Object[] {
                  OTHER_WINDOW_NAMESPACE,
                  WindmillNamespacePrefix.USER_NAMESPACE_PREFIX,
                  OTHER_WINDOW_NAMESPACE_BYTES.concat(expectedUserTimerBytes)
                },
                new Object[] {
                  OTHER_WINDOW_NAMESPACE,
                  WindmillNamespacePrefix.SYSTEM_NAMESPACE_PREFIX,
                  OTHER_WINDOW_NAMESPACE_BYTES.concat(expectedSystemTimerBytes)
                },
                new Object[] {
                  INTERVAL_WINDOW_AND_TRIGGER_NAMESPACE,
                  WindmillNamespacePrefix.USER_NAMESPACE_PREFIX,
                  INTERVAL_WINDOW_AND_TRIGGER_NAMESPACE_BYTES.concat(expectedUserTimerBytes)
                },
                new Object[] {
                  INTERVAL_WINDOW_AND_TRIGGER_NAMESPACE,
                  WindmillNamespacePrefix.SYSTEM_NAMESPACE_PREFIX,
                  INTERVAL_WINDOW_AND_TRIGGER_NAMESPACE_BYTES.concat(expectedSystemTimerBytes)
                },
                new Object[] {
                  OTHER_WINDOW_AND_TRIGGER_NAMESPACE,
                  WindmillNamespacePrefix.USER_NAMESPACE_PREFIX,
                  OTHER_WINDOW_AND_TRIGGER_NAMESPACE_BYTES.concat(expectedUserTimerBytes)
                },
                new Object[] {
                  OTHER_WINDOW_AND_TRIGGER_NAMESPACE,
                  WindmillNamespacePrefix.SYSTEM_NAMESPACE_PREFIX,
                  OTHER_WINDOW_AND_TRIGGER_NAMESPACE_BYTES.concat(expectedSystemTimerBytes)
                });

        for (Object[] params : tests) {
          for (TimeDomain timeDomain : TimeDomain.values()) {
            data.add(
                new Object[] {params[0], params[1], params[2], includeTimerFamilyId, timeDomain});
          }
        }
      }
      return data;
    }

    @Parameter(0)
    public StateNamespace namespace;

    @Parameter(1)
    public WindmillNamespacePrefix prefix;

    @Parameter(2)
    public ByteString expectedBytes;

    @Parameter(3)
    public boolean includeTimerFamilyId;

    @Parameter(4)
    public TimeDomain timeDomain;

    @Test
    public void testTimerTag() {
      TimerData timerData =
          includeTimerFamilyId
              ? TimerData.of(
                  TIMER_ID,
                  TIMER_FAMILY_ID,
                  namespace,
                  new Instant(123),
                  new Instant(456),
                  timeDomain)
              : TimerData.of(TIMER_ID, namespace, new Instant(123), new Instant(456), timeDomain);
      assertEquals(expectedBytes, WindmillTagEncodingV2.instance().timerTag(prefix, timerData));
    }
  }

  @RunWith(Parameterized.class)
  public static class TimerDataFromTimerTest {
    @Parameters(name = "{index}: namespace={0} prefix={1} draining={4} timeDomain={5}")
    public static Collection<Object[]> data() {
      List<Object[]> tests =
          ImmutableList.of(
              new Object[] {
                GLOBAL_NAMESPACE,
                WindmillNamespacePrefix.USER_NAMESPACE_PREFIX,
                GLOBAL_NAMESPACE_BYTES.concat(USER_TIMER_BYTES),
                GlobalWindow.Coder.INSTANCE
              },
              new Object[] {
                GLOBAL_NAMESPACE,
                WindmillNamespacePrefix.SYSTEM_NAMESPACE_PREFIX,
                GLOBAL_NAMESPACE_BYTES.concat(SYSTEM_TIMER_BYTES),
                GlobalWindow.Coder.INSTANCE
              },
              new Object[] {
                INTERVAL_WINDOW_NAMESPACE,
                WindmillNamespacePrefix.USER_NAMESPACE_PREFIX,
                INTERVAL_WINDOW_NAMESPACE_BYTES.concat(USER_TIMER_BYTES),
                IntervalWindow.getCoder()
              },
              new Object[] {
                INTERVAL_WINDOW_NAMESPACE,
                WindmillNamespacePrefix.SYSTEM_NAMESPACE_PREFIX,
                INTERVAL_WINDOW_NAMESPACE_BYTES.concat(SYSTEM_TIMER_BYTES),
                IntervalWindow.getCoder()
              },
              new Object[] {
                OTHER_WINDOW_NAMESPACE,
                WindmillNamespacePrefix.USER_NAMESPACE_PREFIX,
                OTHER_WINDOW_NAMESPACE_BYTES.concat(USER_TIMER_BYTES),
                new CustomWindow.CustomWindowCoder()
              },
              new Object[] {
                OTHER_WINDOW_NAMESPACE,
                WindmillNamespacePrefix.SYSTEM_NAMESPACE_PREFIX,
                OTHER_WINDOW_NAMESPACE_BYTES.concat(SYSTEM_TIMER_BYTES),
                new CustomWindow.CustomWindowCoder()
              },
              new Object[] {
                INTERVAL_WINDOW_AND_TRIGGER_NAMESPACE,
                WindmillNamespacePrefix.USER_NAMESPACE_PREFIX,
                INTERVAL_WINDOW_AND_TRIGGER_NAMESPACE_BYTES.concat(USER_TIMER_BYTES),
                IntervalWindow.getCoder()
              },
              new Object[] {
                INTERVAL_WINDOW_AND_TRIGGER_NAMESPACE,
                WindmillNamespacePrefix.SYSTEM_NAMESPACE_PREFIX,
                INTERVAL_WINDOW_AND_TRIGGER_NAMESPACE_BYTES.concat(SYSTEM_TIMER_BYTES),
                IntervalWindow.getCoder()
              },
              new Object[] {
                OTHER_WINDOW_AND_TRIGGER_NAMESPACE,
                WindmillNamespacePrefix.USER_NAMESPACE_PREFIX,
                OTHER_WINDOW_AND_TRIGGER_NAMESPACE_BYTES.concat(USER_TIMER_BYTES),
                new CustomWindow.CustomWindowCoder()
              },
              new Object[] {
                OTHER_WINDOW_AND_TRIGGER_NAMESPACE,
                WindmillNamespacePrefix.SYSTEM_NAMESPACE_PREFIX,
                OTHER_WINDOW_AND_TRIGGER_NAMESPACE_BYTES.concat(SYSTEM_TIMER_BYTES),
                new CustomWindow.CustomWindowCoder()
              });

      List<Object[]> data = new ArrayList<>();
      for (Object[] params : tests) {
        for (boolean draining : ImmutableList.of(true, false)) {
          for (TimeDomain timeDomain : TimeDomain.values()) {
            data.add(
                new Object[] {params[0], params[1], params[2], params[3], draining, timeDomain});
          }
        }
      }
      return data;
    }

    @Parameter(0)
    public StateNamespace namespace;

    @Parameter(1)
    public WindmillNamespacePrefix prefix;

    @Parameter(2)
    public ByteString timerTag;

    @Parameter(3)
    public Coder<? extends BoundedWindow> windowCoder;

    @Parameter(4)
    public boolean draining;

    @Parameter(5)
    public TimeDomain timeDomain;

    @Test
    public void testTimerDataFromTimer() {
      WindmillTagEncodingV2 encoding = WindmillTagEncodingV2.instance();
      Instant timestamp = Instant.now();
      Instant outputTimestamp = timestamp.plus(Duration.standardSeconds(1));
      TimerData timerData =
          TimerData.of(
              TIMER_ID, TIMER_FAMILY_ID, namespace, timestamp, outputTimestamp, timeDomain);
      Timer timer =
          Timer.newBuilder()
              .setTag(timerTag)
              .setTimestamp(WindmillTimeUtils.harnessToWindmillTimestamp(timestamp))
              .setMetadataTimestamp(WindmillTimeUtils.harnessToWindmillTimestamp(outputTimestamp))
              .setType(timerType(timeDomain))
              .build();
      assertEquals(
          timerData, encoding.windmillTimerToTimerData(prefix, timer, windowCoder, draining));
    }
  }

  @RunWith(JUnit4.class)
  public static class TimerHoldTagTest {
    @Test
    public void testTimerHoldTagUsesTimerTag() {
      TimerData timerData =
          TimerData.of(
              TIMER_ID,
              TIMER_FAMILY_ID,
              GLOBAL_NAMESPACE,
              new Instant(123),
              new Instant(456),
              TimeDomain.EVENT_TIME);
      byte[] bytes = new byte[16];
      ThreadLocalRandom.current().nextBytes(bytes);
      ByteString timerTag = ByteString.copyFrom(bytes);
      assertEquals(
          WindmillTagEncodingV2.instance()
              .timerHoldTag(WindmillNamespacePrefix.SYSTEM_NAMESPACE_PREFIX, timerData, timerTag),
          timerTag);
    }
  }

  private static class CustomWindow extends IntervalWindow {
    private CustomWindow(IntervalWindow intervalWindow) {
      super(intervalWindow.start(), intervalWindow.end());
    }

    private static class CustomWindowCoder extends Coder<CustomWindow> {

      @Override
      public void verifyDeterministic() throws NonDeterministicException {
        IntervalWindowCoder.of().verifyDeterministic();
      }

      @Override
      public List<? extends Coder<?>> getCoderArguments() {
        return IntervalWindowCoder.of().getCoderArguments();
      }

      @Override
      public void encode(CustomWindow value, OutputStream outStream) throws IOException {
        IntervalWindowCoder.of().encode(value, outStream);
      }

      @Override
      public CustomWindow decode(InputStream inStream) throws IOException {
        return new CustomWindow(IntervalWindowCoder.of().decode(inStream));
      }
    }
  }

  private static <T> ByteString encode(Coder<T> coder, T value) {
    try {
      ByteString.Output out = ByteString.newOutput();
      coder.encode(value, out);
      return out.toByteString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static Timer.Type timerType(TimeDomain domain) {
    switch (domain) {
      case EVENT_TIME:
        return Timer.Type.WATERMARK;
      case PROCESSING_TIME:
        return Timer.Type.REALTIME;
      case SYNCHRONIZED_PROCESSING_TIME:
        return Timer.Type.DEPENDENT_REALTIME;
      default:
        throw new IllegalArgumentException("Unrecognized TimeDomain: " + domain);
    }
  }
}
