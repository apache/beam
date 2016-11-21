package cz.seznam.euphoria.operator.test.ng.junit;

import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Lists;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.List;

/**
 * Annotation used in tests. Can be put on {@link AbstractOperatorTest} implementation
 * as well as on {@link ExecutorProvider} implementation to tell testkit which data processing
 * is supported (bounded, unbounded, any). The result is an intersecting subset of 
 * both declarations.
 */
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface Processing {
  
  public static enum Type {
    
    BOUNDED, UNBOUNDED, ANY;
    
    List<Type> asList() {
      return this == ANY ? Lists.newArrayList(BOUNDED, UNBOUNDED) : Lists.newArrayList(this);
    }
    
    boolean isBounded() {
      return this == BOUNDED;
    }
    
    Type merge(Type that) {
      if (this == ANY) return that;
      if (that == ANY) return this;
      if (this == that) return this;
      throw new IllegalStateException("Incompatible processing types: " + this + " vs " + that);
    }
  }
  
  Type value() default Type.ANY;

}
