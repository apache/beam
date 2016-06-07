/**
 * Copyright (c) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not  use this file except  in compliance with the License. You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.contrib.firebase.contrib;

import static com.google.cloud.dataflow.sdk.util.Structs.addString;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.StandardCoder;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * Encodes objects using {@link ObjectReader} and {@link ObjectWriter}.
 */
public class JacksonCoder<T> extends StandardCoder<T> {

  private static final long serialVersionUID = -754345287170755870L;

  private static final ObjectMapper MAPPER = new ObjectMapper()
      .enableDefaultTyping(DefaultTyping.NON_FINAL)
      .enable(DeserializationFeature.WRAP_EXCEPTIONS)
      .enable(SerializationFeature.WRAP_EXCEPTIONS)
      .disable(SerializationFeature.FAIL_ON_UNWRAPPED_TYPE_IDENTIFIERS)
      .enable(SerializationFeature.WRITE_NULL_MAP_VALUES)
      .configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false)
      .configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);

  // Threadsafe as per http://wiki.fasterxml.com/JacksonBestPracticesPerformance
  private final ObjectWriter writer;
  private final ObjectReader reader;

  protected final Class<T> type;

  public static <K> JacksonCoder<K> of(TypeDescriptor<K> type) {
    @SuppressWarnings("unchecked")
    Class<K> clazz = (Class<K>) type.getRawType();
    return of(clazz);
  }

  /**
   * JacksonCoder will encode any type that can be serialized/deserialized by Jackson.
   * This can be achieved by using either Jackson annotations (as seen in this library), or
   * implementing @see <a href=http://wiki.fasterxml.com/JacksonHowToCustomSerializers>
   * custom serializers</a>.
   * @param clazz Type to encode
   * @return A JacksonCoder parameterized with type {@code clazz}
   */
  public static <K> JacksonCoder<K> of(Class<K> clazz){
    return new JacksonCoder<>(clazz);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @JsonCreator
  public static JacksonCoder<?> of(
      @JsonProperty("type") String classType) throws ClassNotFoundException {
    return new JacksonCoder(Class.forName(classType));
  }

  protected JacksonCoder(Class<T> clazz){
    this.type = clazz;
    writer = MAPPER.writer();
    reader = MAPPER.readerFor(type);
  }

  @Override
  public void encode(T value, OutputStream outStream, Context context)
          throws IOException {
    writer.writeValue(outStream, value);
  }

  @Override
  public T decode(InputStream inStream, com.google.cloud.dataflow.sdk.coders.Coder.Context context)
      throws IOException {
    return reader.readValue(inStream);
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Collections.emptyList();
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException{
    throw new Coder.NonDeterministicException(this, "JSON is not a deterministic encoding");
  }

  public Class<T> getType(){
    return this.type;
  }

  protected Object writeReplace() {
    // When serialized by Java, instances of AvroCoder should be replaced by
    // a SerializedJacksonCoderProxy.
    return new SerializedJacksonCoderProxy<>(type);
  }

  @Override
  public CloudObject asCloudObject() {
    CloudObject result = super.asCloudObject();
    addString(result, "type", type.getName());
    return result;
  }

  /**
   * Proxy to use in place of serializing the JacksonCoder. This allows the fields
   * to remain final.
   */
  protected static class SerializedJacksonCoderProxy<T> implements Serializable {
    protected final Class<T> type;

    public SerializedJacksonCoderProxy(Class<T> type) {
      this.type = type;
    }

    private Object readResolve() {
      // When deserialized, instances of this object should be replaced by
      // constructing an JacksonCoder.
      return new JacksonCoder<>(type);
    }
  }
}
