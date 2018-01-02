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
package cz.seznam.euphoria.core.client.io;

import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class MultiDataSinkTest {

  @Test
  public void testMultiDataSinkWrites() throws IOException {
    ListDataSink<String> firstDataSink = ListDataSink.get();
    ListDataSink<String> secondDataSink = ListDataSink.get();
    ListDataSink<String> thirdDataSink = ListDataSink.get();
    DataSink<InputElement> output = createMultiDataSink(firstDataSink, secondDataSink,
        thirdDataSink);

    // write to first sink
    Writer<InputElement> wr = output.openWriter(0);
    wr.write(new InputElement(InputElement.Type.FIRST, "first-Data1"));
    wr.write(new InputElement(InputElement.Type.FIRST, "first-Data2"));
    wr.write(new InputElement(InputElement.Type.THIRD, "third-Data1"));
    wr.write(new InputElement(InputElement.Type.SECOND, "second-Data1"));
    wr.write(new InputElement(InputElement.Type.FIRST, "first-Data3"));

    wr.commit();

    output.commit();

    assertEquals(3, firstDataSink.getOutputs().size());
    assertEquals("first-Data1", firstDataSink.getOutputs().get(0));
    assertEquals("second-Data1", secondDataSink.getOutputs().get(0));
    assertEquals(1, secondDataSink.getOutputs().size());
    assertEquals(1, thirdDataSink.getOutputs().size());
  }

  /**
   *  Added one datasink twice to test if multiDataSink is calling commit on
   *  all his added DataSinks
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testDataSinkCommitCall() throws IOException {
    ListDataSink<String> mockSink = Mockito.mock(ListDataSink.class);
    ListDataSink<String> mockSink2 = Mockito.mock(ListDataSink.class);

    DataSink<InputElement> output = createMultiDataSink(mockSink, mockSink, mockSink2);

    output.openWriter(0);
    output.commit();
    verify(mockSink, times(2)).commit();
    verify(mockSink2).commit();
    output.rollback();
    verify(mockSink, times(2)).rollback();
    verify(mockSink2).rollback();
  }

  private DataSink<InputElement> createMultiDataSink(
      ListDataSink<String> sink1,
      ListDataSink<String> sink2,
      ListDataSink<String> sink3) {
    return MultiDataSink
        .selectBy(InputElement::getType)
        .addSink(InputElement.Type.FIRST, InputElement::getPayload, sink1)
        .addSink(InputElement.Type.SECOND, InputElement::getPayload, sink2)
        .addSink(InputElement.Type.THIRD, InputElement::getPayload, sink3)
        .build();
  }

  private static class InputElement {

    enum Type {
      FIRST,
      SECOND,
      THIRD
    }

    private final Type type;
    private final String payload;

    InputElement(Type type, String payload) {
      this.type = type;
      this.payload = payload;
    }

    Type getType() {
      return type;
    }

    String getPayload() {
      return payload;
    }
  }
}
