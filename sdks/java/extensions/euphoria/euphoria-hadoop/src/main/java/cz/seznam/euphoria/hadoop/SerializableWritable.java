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
package cz.seznam.euphoria.hadoop;

import cz.seznam.euphoria.core.util.ExceptionUtils;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Decorates {@link Writable} with {@link Serializable} interface.
 */
public class SerializableWritable<W extends Writable> implements Serializable {

  private W writable;

  public SerializableWritable(W writable) {
    this.writable = writable;
  }

  public W get() {
    return writable;
  }

  private void writeObject(ObjectOutputStream oos) throws IOException {
    oos.writeObject(writable.getClass());
    writable.write(oos);
  }

  @SuppressWarnings("unchecked")
  private void readObject(ObjectInputStream ois) throws IOException {
    ExceptionUtils.unchecked(() -> {
      final Class<W> clazz = (Class<W>) ois.readObject();
      writable = clazz.newInstance();
      writable.readFields(ois);
    });
  }
}
