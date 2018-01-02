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
package cz.seznam.euphoria.spark.testkit;

import com.esotericsoftware.kryo.Kryo;
import cz.seznam.euphoria.operator.test.IntWindow;
import cz.seznam.euphoria.operator.test.ReduceByKeyTest;
import cz.seznam.euphoria.operator.test.ReduceStateByKeyTest;
import cz.seznam.euphoria.operator.test.WindowingTest;
import org.apache.spark.serializer.KryoRegistrator;

import java.util.ArrayList;

public class TestRegistrator implements KryoRegistrator {

  @Override
  public void registerClasses(Kryo kryo) {
    kryo.register(ArrayList.class);
    kryo.register(IntWindow.class);
    kryo.register(ReduceByKeyTest.CWindow.class);
    kryo.register(ReduceByKeyTest.Word.class);
    kryo.register(ReduceStateByKeyTest.Word.class);
    kryo.register(WindowingTest.ComparablePair.class);
    kryo.register(WindowingTest.Type.class);
  }
}
