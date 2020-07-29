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
package org.apache.beam.sdk.io;

import static org.apache.avro.file.DataFileConstants.BZIP2_CODEC;
import static org.apache.avro.file.DataFileConstants.DEFLATE_CODEC;
import static org.apache.avro.file.DataFileConstants.NULL_CODEC;
import static org.apache.avro.file.DataFileConstants.SNAPPY_CODEC;
import static org.apache.avro.file.DataFileConstants.XZ_CODEC;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.avro.file.CodecFactory;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A wrapper that allows {@link org.apache.avro.file.CodecFactory}s to be serialized using Java's
 * standard serialization mechanisms.
 */
class SerializableAvroCodecFactory implements Externalizable {
  private static final long serialVersionUID = 7445324844109564303L;
  private static final List<String> noOptAvroCodecs =
      Arrays.asList(NULL_CODEC, SNAPPY_CODEC, BZIP2_CODEC);
  private static final Pattern deflatePattern = Pattern.compile(DEFLATE_CODEC + "-(?<level>-?\\d)");
  private static final Pattern xzPattern = Pattern.compile(XZ_CODEC + "-(?<level>\\d)");

  private @Nullable CodecFactory codecFactory;

  // For java.io.Externalizable
  public SerializableAvroCodecFactory() {}

  public SerializableAvroCodecFactory(CodecFactory codecFactory) {
    checkNotNull(codecFactory, "Codec can't be null");
    checkState(checkIsSupportedCodec(codecFactory), "%s is not supported", codecFactory);
    this.codecFactory = codecFactory;
  }

  private boolean checkIsSupportedCodec(CodecFactory codecFactory) {
    final String codecStr = codecFactory.toString();
    return noOptAvroCodecs.contains(codecStr)
        || deflatePattern.matcher(codecStr).matches()
        || xzPattern.matcher(codecStr).matches();
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeUTF(codecFactory.toString());
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    final String codecStr = in.readUTF();

    switch (codecStr) {
      case NULL_CODEC:
      case SNAPPY_CODEC:
      case BZIP2_CODEC:
        codecFactory = CodecFactory.fromString(codecStr);
        return;
    }

    Matcher deflateMatcher = deflatePattern.matcher(codecStr);
    if (deflateMatcher.find()) {
      codecFactory = CodecFactory.deflateCodec(Integer.parseInt(deflateMatcher.group("level")));
      return;
    }

    Matcher xzMatcher = xzPattern.matcher(codecStr);
    if (xzMatcher.find()) {
      codecFactory = CodecFactory.xzCodec(Integer.parseInt(xzMatcher.group("level")));
      return;
    }

    throw new IllegalStateException(codecStr + " is not supported");
  }

  public CodecFactory getCodec() {
    return codecFactory;
  }

  @Override
  public String toString() {
    checkNotNull(codecFactory, "Inner CodecFactory is null, please use non default constructor");
    return codecFactory.toString();
  }
}
