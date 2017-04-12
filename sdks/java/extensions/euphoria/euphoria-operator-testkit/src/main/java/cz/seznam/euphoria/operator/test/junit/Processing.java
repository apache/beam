/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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
package cz.seznam.euphoria.operator.test.junit;

import cz.seznam.euphoria.shaded.guava.com.google.common.collect.Lists;

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
