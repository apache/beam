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
package org.apache.beam.runners.flink.adapter;

import java.io.IOException;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.runners.fnexecution.wire.LengthPrefixUnknownCoders;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.util.construction.CoderTranslation;
import org.apache.beam.sdk.util.construction.RehydratedComponents;
import org.apache.flink.api.common.typeinfo.TypeInformation;

class BeamAdapterCoderUtils {
  static <T> Coder<T> typeInformationToCoder(
      TypeInformation<T> typeInfo, CoderRegistry coderRegistry) {
    Class<T> clazz = typeInfo.getTypeClass();
    if (typeInfo instanceof CoderTypeInformation) {
      return ((CoderTypeInformation) typeInfo).getCoder();
    } else if (clazz.getTypeParameters().length == 0) {
      try {
        return coderRegistry.getCoder(clazz);
      } catch (CannotProvideCoderException exn) {
        throw new RuntimeException(exn);
      }
    } else if (Iterable.class.isAssignableFrom(clazz)) {
      TypeInformation<?> elementType =
          Preconditions.checkArgumentNotNull(typeInfo.getGenericParameters().get("T"));
      return (Coder) IterableCoder.of(typeInformationToCoder(elementType, coderRegistry));
    } else if (Map.class.isAssignableFrom(clazz)) {
      TypeInformation<?> keyType =
          Preconditions.checkArgumentNotNull(typeInfo.getGenericParameters().get("K"));
      TypeInformation<?> valueType =
          Preconditions.checkArgumentNotNull(typeInfo.getGenericParameters().get("V"));
      return (Coder)
          MapCoder.of(
              typeInformationToCoder(keyType, coderRegistry),
              typeInformationToCoder(valueType, coderRegistry));
    } else {
      throw new RuntimeException("Coder translation for " + typeInfo + " not yet supported.");
    }
  }

  static <T> TypeInformation<T> coderToTypeInformation(Coder<T> coder, PipelineOptions options) {
    // TODO(robertwb): Consider mapping some common types.
    return new CoderTypeInformation<>(coder, options);
  }

  static <T> Coder<T> lookupCoder(RunnerApi.Pipeline p, String pCollectionId) {
    try {
      return (Coder<T>)
          CoderTranslation.fromProto(
              p.getComponents()
                  .getCodersOrThrow(
                      p.getComponents().getPcollectionsOrThrow(pCollectionId).getCoderId()),
              RehydratedComponents.forComponents(p.getComponents()),
              CoderTranslation.TranslationContext.DEFAULT);
    } catch (IOException exn) {
      throw new RuntimeException(exn);
    }
  }

  static void registerKnownCoderFor(RunnerApi.Pipeline p, String pCollectionId) {
    registerAsKnownCoder(p, p.getComponents().getPcollectionsOrThrow(pCollectionId).getCoderId());
  }

  static void registerAsKnownCoder(RunnerApi.Pipeline p, String coderId) {
    RunnerApi.Coder coder = p.getComponents().getCodersOrThrow(coderId);
    // It'd be more targeted to note the coder id rather than the URN,
    // but the length prefixing code is invoked within a deeply nested
    // sequence of static method calls.
    LengthPrefixUnknownCoders.addKnownCoderUrn(coder.getSpec().getUrn());
    for (String componentCoderId : coder.getComponentCoderIdsList()) {
      registerAsKnownCoder(p, componentCoderId);
    }
  }
}
