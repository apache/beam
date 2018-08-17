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
package cz.seznam.euphoria.core.client.flow;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.io.MockStreamDataSourceFactory;
import cz.seznam.euphoria.core.client.operator.Filter;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.operator.Union;
import cz.seznam.euphoria.core.util.Settings;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

/**
 * Test some basic features of flow.
 */
public class TestFlow {
  
  Settings settings;
  Flow flow;
  
  @Before
  public void before() {
    settings = new Settings();
    settings.setClass("euphoria.io.datasource.factory.mock",
        MockStreamDataSourceFactory.class);

    flow = Flow.create("TestFlow", settings);
  }

  @Test
  public void testDatasetConsumers() throws Exception {
    Dataset<Object> input = flow.createInput(URI.create("mock:///"));
    Dataset<Object> transformed = MapElements.of(input).using(e -> e).output();
    Dataset<Object> transformed2 = Filter.of(transformed).by(e -> false).output();
    Dataset<Object> union = Union.of(transformed, transformed2).output();

    assertEquals(1, input.getConsumers().size());
    assertEquals(2, transformed.getConsumers().size());
    assertEquals(1, transformed2.getConsumers().size());
    assertEquals(0, union.getConsumers().size());

    // the 'transformed' data set is consumed by Filter and Union operators
    assertEquals(toSet(Arrays.asList(Filter.class, Union.class)),
        toSet(transformed.getConsumers().stream().map(Object::getClass)));

  }


  private static <X> Set<X> toSet(Collection<X> c) {
    return toSet(c.stream());
  }

  private static <X> Set<X> toSet(Stream<X> s) {
    return s.collect(Collectors.toSet());
  }

}
