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
package cz.seznam.euphoria.core.util;

import java.lang.reflect.Constructor;

/**
 * Util class that helps instantiations of objects throwing {@link RuntimeException}.
 * For core purposes only. Should not be used in client code.
 */
public class InstanceUtils {

  public static <T> T create(Class<T> cls) {
    try {
      Constructor<T> constr = cls.getDeclaredConstructor();
      constr.setAccessible(true);
      return constr.newInstance();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  public static <T> T create(String className, Class<T> superType) {
    return create(forName(className, superType));
  }
  
  @SuppressWarnings("unchecked")
  public static <T> Class<? extends T> forName(String className, Class<T> superType) {
    try {
      Class<?> cls = Thread.currentThread().getContextClassLoader().loadClass(className);
      if (superType.isAssignableFrom(cls)) {
        return (Class<? extends T>) cls;
      } else {
        throw new IllegalStateException(className + " is not " + superType);
      }
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(e);
    }
  }

}
