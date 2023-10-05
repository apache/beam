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
package org.apache.beam.sdk.extensions.sbe;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.apache.beam.sdk.schemas.Schema.Field;

/** Interface for SBE fields. */
interface SbeField extends Serializable {

  /** Returns the name of the field. */
  String name();

  /** Returns whether this field is required. */
  Boolean isRequired();

  /** Returns the Beam schema equivalent of this field. */
  Field asBeamField(SbeFieldOptions options);

  /** Options for configuring the behavior of how to handle fields. */
  @AutoValue
  abstract class SbeFieldOptions {
    abstract UnsignedOptions unsignedOptions();

    static Builder builder() {
      return new AutoValue_SbeField_SbeFieldOptions.Builder();
    }

    /** Builder for {@link SbeFieldOptions}. */
    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setUnsignedOptions(UnsignedOptions value);

      abstract SbeFieldOptions build();
    }
  }
}
