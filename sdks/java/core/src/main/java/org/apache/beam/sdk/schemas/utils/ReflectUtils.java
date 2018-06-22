/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.sdk.schemas.utils;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;

public class ReflectUtils {
  private static class MethodOffset implements Comparable<MethodOffset> {
    MethodOffset(Method _method, int _offset) {
      method=_method;
      offset=_offset;
    }

    @Override
    public int compareTo(MethodOffset target) {
      return offset-target.offset;
    }

    Method method;
    int offset;
  }

  /**
   * Attempt to return the list of declared methods in declaration order.
   * The best we can do is return them in the order they are defined in the byte code, which is
   * not guaranteed to be in declaration order. However, in practice this is good enough.
   */
   static Method[] getDeclaredMethodsInOrder(Class clazz) throws IOException {
    Method[] methods = null;
    String resource = clazz.getName().replace('.', '/') + ".class";

    methods = clazz.getDeclaredMethods();

    InputStream is = clazz.getClassLoader()
        .getResourceAsStream(resource);

    if (is == null) {
      return methods;
    }

    java.util.Arrays.sort(methods, (a, b) -> b.getName().length() - a.getName().length());
    ArrayList<byte[]> blocks = Lists.newArrayList();
    int length = 0;
    for (; ; ) {
      byte[] block = new byte[16 * 1024];
      int n = is.read(block);
      if (n > 0) {
        if (n < block.length) {
          block = java.util.Arrays.copyOf(block, n);
        }
        length += block.length;
        blocks.add(block);
      } else {
        break;
      }
    }

    byte[] data = new byte[length];
    int offset = 0;
    for (byte[] block : blocks) {
      System.arraycopy(block, 0, data, offset, block.length);
      offset += block.length;
    }

    String sdata = new String(data, java.nio.charset.Charset.forName("UTF-8"));
    int lnt = sdata.indexOf("LineNumberTable");
    if (lnt != -1) sdata = sdata.substring(lnt + "LineNumberTable".length() + 3);
    int cde = sdata.lastIndexOf("SourceFile");
    if (cde != -1) sdata = sdata.substring(0, cde);

    MethodOffset mo[] = new MethodOffset[methods.length];


    for (int i = 0; i < methods.length; ++i) {
      int pos = -1;
      for (; ; ) {
        pos = sdata.indexOf(methods[i].getName(), pos);
        if (pos == -1) break;
        boolean subset = false;
        for (int j = 0; j < i; ++j) {
          if (mo[j].offset >= 0 &&
              mo[j].offset <= pos &&
              pos < mo[j].offset + mo[j].method.getName().length()) {
            subset = true;
            break;
          }
        }
        if (subset) {
          pos += methods[i].getName().length();
        } else {
          break;
        }
      }
      mo[i] = new MethodOffset(methods[i], pos);
    }
    java.util.Arrays.sort(mo);
    for (int i = 0; i < mo.length; ++i) {
      methods[i] = mo[i].method;
    }

    return methods;
  }
}
