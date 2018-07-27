package org.apache.beam.sdk.extensions.euphoria.core.client.type;


import java.lang.reflect.TypeVariable;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Pair;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Assert;

/**
 * Buncg of methods to assert type descriptors in operators.
 */
public class TypePropagationAssert {

  public static void assertOperatorTypeAwareness(Operator<?, ?> operator,
      TypeDescriptor<?> outputType, TypeDescriptor<?> keyType, TypeDescriptor<?> valueType) {

    if (keyType != null || operator instanceof TypeAware.Key) {
      Assert.assertSame(keyType, ((TypeAware.Key) operator).getKeyType());

      TypeDescriptor<Pair<?, ?>> pairedOutputType =
          (TypeDescriptor<Pair<?, ?>>) operator.getOutputType();

      TypeVariable<Class<Pair>>[] pairParameters = Pair.class.getTypeParameters();

      TypeDescriptor<?> firstType = pairedOutputType.resolveType(pairParameters[0]);
      TypeDescriptor<?> secondType = pairedOutputType.resolveType(pairParameters[1]);

      Assert.assertEquals(keyType, firstType);
      Assert.assertEquals(outputType, secondType);

    } else {
      // assert output of non keyed operator
      Assert.assertSame(outputType, operator.getOutputType());
    }

    if (valueType != null || operator instanceof TypeAware.Value) {
      Assert.assertSame(valueType, ((TypeAware.Value) operator).getValueType());
    }
  }

  public static void assertOperatorTypeAwareness(
      Operator<?, ?> operator, TypeDescriptor<?> outputType) {
    assertOperatorTypeAwareness(operator, outputType, null, null);
  }

  public static void assertOperatorTypeAwareness(Operator<?, ?> operator,
      TypeDescriptor<?> outputType, TypeDescriptor<?> keyType) {
    assertOperatorTypeAwareness(operator, outputType, keyType, null);
  }

}
