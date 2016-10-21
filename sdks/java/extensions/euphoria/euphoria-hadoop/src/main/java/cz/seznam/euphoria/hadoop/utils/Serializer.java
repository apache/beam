
package cz.seznam.euphoria.hadoop.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Java serializer.
 */
public class Serializer {

  /**
   * Convert {@code Serializable} to bytes using java serialization.
   */
  public static byte[] toBytes(Serializable object) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(object);
    oos.close();
    return baos.toByteArray();
  }

  /**
   * Convert bytes to {@code Serializable} object.
   */
  public static <T extends Serializable> T fromBytes(byte[] bytes)
      throws IOException, ClassNotFoundException {
    
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    ObjectInputStream ois = new ObjectInputStream(bais);
    return (T) ois.readObject();
  }

}
