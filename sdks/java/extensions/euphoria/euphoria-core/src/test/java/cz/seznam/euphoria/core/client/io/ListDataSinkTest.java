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

package cz.seznam.euphoria.core.client.io;

import cz.seznam.euphoria.core.client.dataset.asserts.DatasetAssert;
import cz.seznam.euphoria.shaded.guava.com.google.common.collect.Iterables;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ListDataSinkTest {

  @Test
  public void testMultipleSinks() throws Exception {
    ListDataSink<String> sink1 = ListDataSink.get();
    ListDataSink<String> sink2 = ListDataSink.get();

    // write to first sink
    Writer<String> w = sink1.openWriter(0);
    w.write("first");
    w.commit();

    // write to seconds sink
    w = sink2.openWriter(0);
    w.write("second-0");
    w.commit();

    w = sink2.openWriter(1);
    w.write("second-1");
    w.commit();

    assertEquals("first", Iterables.getOnlyElement(sink1.getOutputs()));

    DatasetAssert.unorderedEquals(sink2.getOutputs(), "second-0", "second-1");
  }
}