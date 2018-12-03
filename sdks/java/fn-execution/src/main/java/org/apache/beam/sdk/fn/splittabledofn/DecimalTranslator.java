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

import static com.google.common.base.Preconditions.checkArgument;

import java.math.BigDecimal;
import java.math.BigInteger;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Decimal;
import org.apache.beam.vendor.grpc.v1_13_1.com.google.protobuf.ByteString;

/** Translates from {@link BigDecimal} to proto {@link Decimal} and back. */
public class DecimalTranslator {
  /** Translate from {@link BigDecimal} to a proto {@link Decimal}. */
  public static Decimal toProto(BigDecimal decimal) {
    checkArgument(
        decimal.compareTo(BigDecimal.ZERO) >= 0,
        "Value is expected to be a non-negative number but received %s",
        decimal);
    return Decimal.newBuilder()
        .setUnscaledValue(ByteString.copyFrom(decimal.unscaledValue().toByteArray()))
        .setScale(decimal.scale())
        .build();
  }

  /** Translate from proto {@link Decimal} to a {@link BigDecimal}. */
  public static BigDecimal fromProto(Decimal decimal) {
    BigDecimal rval =
        new BigDecimal(
            new BigInteger(decimal.getUnscaledValue().toByteArray()), decimal.getScale());
    checkArgument(
        rval.compareTo(BigDecimal.ZERO) >= 0,
        "Value is expected to be a non-negative number but received %s",
        decimal);
    return rval;
  }
}
