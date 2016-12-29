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
package org.apache.beam.sdk.io.hdfs;

import static com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StandardCoder;
import org.apache.beam.sdk.util.CloudObject;
import org.apache.beam.sdk.util.PropertyNames;

/**
 * A {@code AvroWrapperCoder} is a {@link Coder} for a Java class that implements {@link
 * AvroWrapper}.
 *
 * @param <WrapperT> the type of the wrapper
 * @param <DatumT> the type of the datum
 */
public class AvroWrapperCoder<WrapperT extends AvroWrapper<DatumT>, DatumT>
    extends StandardCoder<WrapperT> {
  private static final long serialVersionUID = 0L;

  private final Class<WrapperT> wrapperType;
  private final AvroCoder<DatumT> datumCoder;

  private AvroWrapperCoder(Class<WrapperT> wrapperType, AvroCoder<DatumT> datumCoder) {
    this.wrapperType = wrapperType;
    this.datumCoder = datumCoder;
  }

  /**
   * Return a {@code AvroWrapperCoder} instance for the provided element class.
   * @param <WrapperT> the type of the wrapper
   * @param <DatumT> the type of the datum
   */
  public static <WrapperT extends AvroWrapper<DatumT>, DatumT>
  AvroWrapperCoder<WrapperT, DatumT>of(Class<WrapperT> wrapperType, AvroCoder<DatumT> datumCoder) {
    return new AvroWrapperCoder<>(wrapperType, datumCoder);
  }

  @JsonCreator
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static AvroWrapperCoder<?, ?> of(
      @JsonProperty("wrapperType") String wrapperType,
      @JsonProperty(PropertyNames.COMPONENT_ENCODINGS) List<Coder<?>> components)
      throws ClassNotFoundException {
    Class<?> clazz = Class.forName(wrapperType);
    if (!AvroWrapper.class.isAssignableFrom(clazz)) {
      throw new ClassNotFoundException(
          "Class " + wrapperType + " does not implement AvroWrapper");
    }
    checkArgument(components.size() == 1, "Expecting 1 component, got " + components.size());
    return of((Class<? extends AvroWrapper>) clazz, (AvroCoder<?>) components.get(0));
  }

  @Override
  public void encode(WrapperT value, OutputStream outStream, Context context) throws IOException {
    datumCoder.encode(value.datum(), outStream, context);
  }

  @Override
  public WrapperT decode(InputStream inStream, Context context) throws IOException {
    try {
      WrapperT wrapper = wrapperType.newInstance();
      wrapper.datum(datumCoder.decode(inStream, context));
      return wrapper;
    } catch (InstantiationException | IllegalAccessException e) {
      throw new CoderException("unable to deserialize record", e);
    }
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Collections.singletonList(datumCoder);
  }

  @Override
  public CloudObject initializeCloudObject() {
    CloudObject result = CloudObject.forClass(getClass());
    result.put("wrapperType", wrapperType.getName());
    return result;
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    datumCoder.verifyDeterministic();
  }

}
