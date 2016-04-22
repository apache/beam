package cz.seznam.euphoria.hadoop;

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

  public W getWritable() {
    return writable;
  }

  private void writeObject(ObjectOutputStream oos) throws IOException {
    oos.writeObject(writable.getClass());
    writable.write(oos);
  }

  private void readObject(ObjectInputStream ois)
          throws IOException, ClassNotFoundException,
          InstantiationException, IllegalAccessException
  {
    Class<W> cls = (Class<W>) ois.readObject();
    writable = cls.newInstance();
    writable.readFields(ois);
  }
}
