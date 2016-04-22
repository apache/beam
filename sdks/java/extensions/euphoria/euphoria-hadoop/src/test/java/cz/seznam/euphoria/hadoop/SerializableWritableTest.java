package cz.seznam.euphoria.hadoop;


import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.junit.Assert.*;

public class SerializableWritableTest {

  @Test
  public void testSerialization() throws Exception {
    Configuration conf = new Configuration();
    conf.set("key", "value");

    SerializableWritable<Configuration> sw = new SerializableWritable<>(conf);

    // clone by serialization
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(sw);

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);

    sw = (SerializableWritable<Configuration>) ois.readObject();
    Configuration newConf = sw.getWritable();

    assertEquals(conf.get("key"), newConf.get("key"));
  }
}