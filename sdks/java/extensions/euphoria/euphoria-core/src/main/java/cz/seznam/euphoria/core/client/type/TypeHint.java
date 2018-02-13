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
package cz.seznam.euphoria.core.client.type;

import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.shadow.com.google.common.reflect.TypeParameter;
import cz.seznam.euphoria.shadow.com.google.common.reflect.TypeToken;
import java.io.Serializable;
import java.lang.reflect.Type;

public abstract class TypeHint<T> implements Serializable {

  private final TypeToken<T> type;

  protected TypeHint() {
    this.type = new TypeToken<T>(this.getClass()) {};
  }

  private TypeHint(TypeToken<T> type) {
    this.type = type;
  }

  public final Type getType() {
    return type.getType();
  }

  public final TypeToken<T> getTypeToken() {
    return type;
  }

  public static <T> TypeHint<T> of(TypeToken<T> type) {
    return new SimpleTypeHint<>(type);
  }

  public static <T> TypeHint<T> of(Class<T> clazz) {
    return new SimpleTypeHint<>(TypeToken.of(clazz));
  }

  public static TypeHint<String> ofString() {
    return TypeHint.of(String.class);
  }

  public static TypeHint<Long> ofLong() {
    return TypeHint.of(Long.class);
  }

  public static TypeHint<Integer> ofInt() {
    return TypeHint.of(Integer.class);
  }

  public static <A, B> TypeHint<Pair<A, B>> ofPair(TypeToken<A> left, TypeToken<B> right) {
    return new SimpleTypeHint<>(new TypeToken<Pair<A, B>>(Pair.class) { }
        .where(new TypeParameter<A>() { }, left)
        .where(new TypeParameter<B>() { }, right));
  }

  public static <A, B> TypeHint<Pair<A, B>> ofPair(Class<A> left, Class<B> right) {
    return new SimpleTypeHint<>(new TypeToken<Pair<A, B>>(Pair.class) { }
        .where(new TypeParameter<A>() { }, left)
        .where(new TypeParameter<B>() { }, right));
  }

  private static class SimpleTypeHint<T> extends TypeHint<T> {

    private SimpleTypeHint(TypeToken<T> tt) {
      super(tt);
    }
  }

}
