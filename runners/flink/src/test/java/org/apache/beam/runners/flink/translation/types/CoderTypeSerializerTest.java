package org.apache.beam.runners.flink.translation.types;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.flink.api.common.typeutils.ComparatorTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class CoderTypeSerializerTest {

  @Test
  public void shouldBeAbleToWriteSnapshotForAnonymousClassCoder() throws Exception {
    AtomicCoder<String> anonymousClassCoder = new AtomicCoder<String>() {

      @Override public void encode(String value, OutputStream outStream)
          throws CoderException, IOException {

      }

      @Override public String decode(InputStream inStream) throws CoderException, IOException {
        return "";
      }
    };

    CoderTypeSerializer<String> serializer = new CoderTypeSerializer<>(anonymousClassCoder);

    TypeSerializerConfigSnapshot configSnapshot = serializer.snapshotConfiguration();
    configSnapshot.write(new ComparatorTestBase.TestOutputView());
  }

  @Test
  public void shouldBeAbleToWriteSnapshotForConcreteClassCoder() throws Exception { //passes
    Coder<String> concreteClassCoder = StringUtf8Coder.of();
    CoderTypeSerializer<String> coderTypeSerializer = new CoderTypeSerializer<>(concreteClassCoder);
    TypeSerializerConfigSnapshot typeSerializerConfigSnapshot = coderTypeSerializer
        .snapshotConfiguration();
    typeSerializerConfigSnapshot.write(new ComparatorTestBase.TestOutputView());
  }
}

