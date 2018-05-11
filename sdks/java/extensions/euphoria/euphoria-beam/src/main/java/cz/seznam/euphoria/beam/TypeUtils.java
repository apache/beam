/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.beam;

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;

final class TypeUtils {

  private TypeUtils() {}

  @SuppressWarnings("unchecked")
  static <K, V> TypeDescriptor<KV<K, V>> kvOf(TypeToken<K> keyType, TypeToken<V> valueType) {
    final TypeToken<KV<K, V>> type =
        new TypeToken<KV<K, V>>() {}.where(new TypeParameter<K>() {}, keyType)
            .where(new TypeParameter<V>() {}, valueType);
    return (TypeDescriptor) TypeDescriptor.of(type.getType());
  }
}
