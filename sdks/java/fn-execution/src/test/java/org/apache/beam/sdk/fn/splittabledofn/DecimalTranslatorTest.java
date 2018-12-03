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
package org.apache.beam.sdk.fn.splittabledofn;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Decimal;
import org.apache.beam.vendor.grpc.v1_13_1.com.google.protobuf.ByteString;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DecimalTranslator}. */
@RunWith(JUnit4.class)
public class DecimalTranslatorTest {
  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testToProtoWithNegativeValue() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("expected to be a non-negative number");
    DecimalTranslator.toProto(new BigDecimal(-1));
  }

  @Test
  public void testFromProtoWithNegativeValue() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("expected to be a non-negative number");
    DecimalTranslator.fromProto(
        Decimal.newBuilder()
            .setUnscaledValue(ByteString.copyFrom(new byte[] {(byte) 0xff}))
            .build());
  }

  @Test
  public void testWithPositiveValue() {
    BigDecimal bigNumber =
        new BigDecimal(Double.MAX_VALUE).multiply(new BigDecimal(Double.MAX_VALUE));
    assertEquals(bigNumber, DecimalTranslator.fromProto(DecimalTranslator.toProto(bigNumber)));
  }

  @Test
  public void testWithZero() {
    assertEquals(
        BigDecimal.ZERO, DecimalTranslator.fromProto(DecimalTranslator.toProto(BigDecimal.ZERO)));
  }
}
