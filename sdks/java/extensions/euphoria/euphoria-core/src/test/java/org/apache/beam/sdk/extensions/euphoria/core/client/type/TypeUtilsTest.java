package org.apache.beam.sdk.extensions.euphoria.core.client.type;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Type;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Pair;
import org.junit.Test;

public class TypeUtilsTest {

  private final UnaryFunction<String, String> unaryFunction = a -> a;

  interface FunctionalInterface {

    Integer testMethod(Integer i);
  }

  interface GenericOutputFunctionalInterface<T> {

    T produceSomething();
  }

  interface GenericInOutFunctionalInterface<T> {

    T produceSomething(T in);
  }

  @Test
  public void testLambdaReturnTypeNoGenerics() {

    assertEquals(String.class, TypeUtils.getLambdaReturnType(unaryFunction));

    UnaryFunction<String, Pair<String, String>> unaryFunction2 = a -> Pair.of(a, a);

    assertEquals(Pair.class, TypeUtils.getLambdaReturnType(unaryFunction2));

    //noinspection Convert2Lambda - test returning type for anonymous inner class
    assertEquals(
        Integer.class,
        TypeUtils.getLambdaReturnType(
            new FunctionalInterface() {
              @Override
              public Integer testMethod(Integer i) {
                return 0;
              }
            }));

    functionalIterfaceConsumer((Integer i) -> i + 1);
    functionalIterfaceConsumer(i -> i + 1);
  }

  @Test
  public void testLambdaReturnTypeParametrizedOut() {
    GenericOutputFunctionalInterface<String> s = () -> "hi";
    genericFunctionalInterfaceConsumer(s, String.class);
  }

  @Test
  public void testLambdaReturnTypeParametrizedInOut() {
    genericIoOutFunctionalInterfaceConsumer((Integer i) -> i + 2, Integer.class);
  }

  private void functionalIterfaceConsumer(FunctionalInterface instance) {
    assertEquals(Integer.class, TypeUtils.getLambdaReturnType(instance));
  }

  private <T> void genericFunctionalInterfaceConsumer(
      GenericOutputFunctionalInterface<T> instance, Type expectedType) {
    assertEquals(expectedType, TypeUtils.getLambdaReturnType(instance));
  }

  private <T> void genericIoOutFunctionalInterfaceConsumer(
      GenericInOutFunctionalInterface<T> instance, Type expectedType) {
    assertEquals(expectedType, TypeUtils.getLambdaReturnType(instance));
  }

}