package cz.seznam.euphoria.operator.test.junit;

import com.google.common.collect.Lists;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.List;
import java.util.Optional;

/**
 * Annotation used in tests. Can be put on {@link AbstractOperatorTest} implementation
 * as well as on {@link ExecutorProvider} implementation to tell testkit which data processing
 * is supported (bounded, unbounded, any). The result is an intersecting subset of 
 * both declarations.
 */
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface Processing {
  
  enum Type {
    
    BOUNDED, UNBOUNDED, ALL;
    
    List<Type> asList() {
      return this == ALL ? Lists.newArrayList(BOUNDED, UNBOUNDED) : Lists.newArrayList(this);
    }
    
    Optional<Type> merge(Type that) {
      if (this == ALL) return Optional.of(that);
      if (that == ALL) return Optional.of(this);
      if (this == that) return Optional.of(this);
      return Optional.empty();
    }
  }
  
  Type value();
}
