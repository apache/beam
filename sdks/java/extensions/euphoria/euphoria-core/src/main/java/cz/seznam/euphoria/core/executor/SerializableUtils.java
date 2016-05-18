
package cz.seznam.euphoria.core.executor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Various serialization related utils.
 */
public class SerializableUtils {

  /**
   * Clone given instance using {@code Serializable} interface.
   */
  @SuppressWarnings("unchecked")
  public static <T extends Serializable> T cloneSerializable(T orig) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);

      oos.writeObject(orig);
      oos.close();
      ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
      ObjectInputStream ois = new ObjectInputStream(bais);
      Object ret = ois.readObject();
      return (T) ret;
    } catch (IOException | ClassNotFoundException ex) {
      throw new RuntimeException(ex);
    }
  }

}
