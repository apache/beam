package org.apache.beam.sdk.values;

import static com.google.common.base.Preconditions.checkArgument;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;

/**
 * Captures a free type variable that can be used in {@link TypeDescriptor#where}. For example:
 *
 * <pre>   {@code
 *   static <T> TypeDescriptor<List<T>> listOf(Class<T> elementType) {
 *     return new TypeDescriptor<List<T>>() {}
 *         .where(new TypeParameter<T>() {}, elementType);
 *   }}</pre>
 */
public abstract class TypeParameter<T> {
  final TypeVariable<?> typeVariable;

  public TypeParameter() {
    Type superclass = getClass().getGenericSuperclass();
    checkArgument(superclass instanceof ParameterizedType, "%s isn't parameterized", superclass);
    typeVariable = (TypeVariable<?>) ((ParameterizedType) superclass).getActualTypeArguments()[0];
  }

  @Override
  public int hashCode() {
    return typeVariable.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof TypeParameter)) {
      return false;
    }
    TypeParameter<?> that = (TypeParameter<?>) obj;
    return typeVariable.equals(that.typeVariable);
  }

  @Override
  public String toString() {
    return typeVariable.toString();
  }
}
