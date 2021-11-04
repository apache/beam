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
package org.apache.beam.sdk.extensions.sql.impl.rel;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.StructuredCoder;

public class BeamCalcRelErrorCoder extends StructuredCoder<BeamCalcRelError> {

  public static BeamCalcRelErrorCoder of() {
    return new BeamCalcRelErrorCoder();
  }

  @Override
  public void encode(BeamCalcRelError value, OutputStream outStream)
      throws CoderException, IOException {
    StringUtf8Coder.of().encode(value.getError(), outStream);
    StringUtf8Coder.of().encode(value.getRow(), outStream);
  }

  @Override
  public BeamCalcRelError decode(InputStream inStream) throws CoderException, IOException {

    String error = StringUtf8Coder.of().decode(inStream);
    String row = StringUtf8Coder.of().decode(inStream);
    return new BeamCalcRelError(row, error);
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Arrays.asList();
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    //    verifyDeterministic(this, "Row coder must be deterministic", rowCoder);
  }
}
