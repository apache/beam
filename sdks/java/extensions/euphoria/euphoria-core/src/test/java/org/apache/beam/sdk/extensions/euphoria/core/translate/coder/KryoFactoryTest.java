package org.apache.beam.sdk.extensions.euphoria.core.translate.coder;

import com.esotericsoftware.kryo.Kryo;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.junit.Assert;
import org.junit.Test;

public class KryoFactoryTest {

  @Test
  public void testGiveTheSameKrioAfterKryoRegistrarDeserialized()
      throws IOException, ClassNotFoundException {

    KryoRegistrar registrar = (k) -> k.register(TestClass.class);

    Kryo firstKryo = KryoFactory.getOrCreateKryo(registrar);

    ByteArrayOutputStream outStr = new ByteArrayOutputStream();
    ObjectOutputStream oss = new ObjectOutputStream(outStr);

    oss.writeObject(registrar);
    oss.flush();
    oss.close();

    ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(outStr.toByteArray()));

    @SuppressWarnings("unchecked")
    KryoRegistrar deserializedRegistrar =
        (KryoRegistrar) ois.readObject();

    Kryo secondKryo = KryoFactory.getOrCreateKryo(deserializedRegistrar);

    Assert.assertSame(firstKryo, secondKryo);
  }

  private static class TestClass {

  }

}