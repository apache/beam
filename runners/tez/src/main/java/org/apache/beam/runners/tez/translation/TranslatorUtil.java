package org.apache.beam.runners.tez.translation;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Base64;
import org.apache.beam.sdk.transforms.DoFn;
import java.util.List;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;

/**
 * Translator Utilities to convert between hadoop and java types.
 */
public class TranslatorUtil {

  /**
   * Utility to convert java objects to bytes and place them in BytesWritable wrapper for hadoop use.
   * @param element java object to be converted
   * @return BytesWritable wrapped object
   */
  public static Object convertToBytesWritable(Object element) {
    byte[] bytes;
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = new ObjectOutputStream(bos)) {
      out.writeObject(element);
      out.flush();
      bytes = bos.toByteArray();
    } catch (Exception e){
      throw new RuntimeException("Failed to serialize object into byte array: " + e.getMessage());
    }
    if (bytes != null) {
      return new BytesWritable(bytes);
    } else {
      throw new RuntimeException("Cannot convert null element to BytesWritable!");
    }
  }

  /**
   * Utility to convert hadoop objects back to their java equivalent.
   * @param element hadoop object to be converted
   * @return original java object
   */
  public static Object convertToJavaType(Object element) {
    Object returnValue;
    if (element instanceof BytesWritable){
      BytesWritable myElement = (BytesWritable) element;
      byte[] data = myElement.getBytes();
      try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
          ObjectInput in = new ObjectInputStream(bis)) {
        returnValue = in.readObject();
      } catch (Exception e){
        throw new RuntimeException("Failed to deserialize object from byte array: " + e.getMessage());
      }
    } else if (element instanceof Text) {
      returnValue = element.toString();
    } else if (element instanceof BooleanWritable) {
      returnValue = ((BooleanWritable) element).get();
    } else if (element instanceof IntWritable){
      returnValue = ((IntWritable) element).get();
    } else if (element instanceof DoubleWritable){
      returnValue = ((DoubleWritable) element).get();
    } else if (element instanceof FloatWritable){
      returnValue = ((FloatWritable) element).get();
    } else if (element instanceof LongWritable){
      returnValue = ((LongWritable) element).get();
    } else if (element instanceof ShortWritable){
      returnValue = ((ShortWritable) element).get();
    } else if (element instanceof ObjectWritable){
      returnValue = ((ObjectWritable) element).get();
    } else {
      throw new RuntimeException("Hadoop Type " + element.getClass() + " cannot be converted to Java!");
    }
    return returnValue;
  }

  /**
   * Utility to convert hadoop objects within an iterable back to their java equivalent.
   * @param iterable Iterable containing objects to be converted
   * @return new Iterable with original java objects
   */
  static Iterable<Object> convertIteratorToJavaType(Iterable<Object> iterable){
    List<Object> list = new ArrayList<>();
    iterable.iterator().forEachRemaining((Object element) -> list.add(convertToJavaType(element)));
    return list;
  }

  /**
   * Utility to serialize a serializable object into a string.
   * @param object that is serializable to be serialized.
   * @return serialized string
   * @throws IOException thrown for serialization errors.
   */
  public static String toString( Serializable object ) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(object);
    oos.close();
    return Base64.getEncoder().encodeToString(baos.toByteArray());
  }

  /**
   * Utility to deserialize a string into a serializable object.
   * @param string containing serialized object.
   * @return Original object
   * @throws IOException thrown for serialization errors.
   * @throws ClassNotFoundException thrown for serialization errors.
   */
  public static Object fromString( String string ) throws IOException, ClassNotFoundException {
    byte [] data = Base64.getDecoder().decode(string);
    ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));
    Object object = ois.readObject();
    ois.close();
    return object;
  }

}
