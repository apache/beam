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
