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
package org.apache.beam.sdk.extensions.sbe;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.Iterator;
import java.util.List;
import org.apache.beam.sdk.extensions.sbe.SbeSchema.IrOptions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import uk.co.real_logic.sbe.ir.Encoding.Presence;
import uk.co.real_logic.sbe.ir.Ir;
import uk.co.real_logic.sbe.ir.Token;

/** Utility for generating {@link SbeField}s from an {@link Ir}. */
final class IrFieldGenerator {
  private IrFieldGenerator() {}

  /**
   * Generates the {@link SbeField}s for a given {@link Ir}.
   *
   * @param ir the intermediate representation to use for generating the fields
   * @param irOptions options for handling ambiguous situations
   * @return all the fields in the IR
   */
  public static ImmutableList<SbeField> generateFields(Ir ir, IrOptions irOptions) {
    ImmutableList.Builder<SbeField> fields = ImmutableList.builder();

    TokenIterator iterator = getIteratorForMessage(ir, irOptions);
    while (iterator.hasNext()) {
      Token token = iterator.next();
      switch (token.signal()) {
        case BEGIN_FIELD:
          fields.add(processPrimitive(iterator));
          break;
        default:
          // TODO(https://github.com/apache/beam/issues/21102): Support remaining field types
          break;
      }
    }

    return fields.build();
  }

  /** Helper for getting the tokens for the target message. */
  @SuppressWarnings("nullness") // False positive on already-checked messageId
  private static TokenIterator getIteratorForMessage(Ir ir, IrOptions irOptions) {
    List<Token> messages;

    if (irOptions.messageId() == null && irOptions.messageName() == null) {
      messages =
          ir.messages().stream()
              .collect(
                  collectingAndThen(
                      toList(),
                      lists -> {
                        checkArgument(!lists.isEmpty(), "No messages in IR");
                        checkArgument(
                            lists.size() == 1,
                            "More than one message in IR but no identifier provided.");
                        return lists.get(0);
                      }));
    } else if (irOptions.messageName() != null) {
      String name = irOptions.messageName();
      messages =
          ir.messages().stream()
              .filter(li -> !li.isEmpty() && li.get(0).name().equals(name))
              .collect(
                  collectingAndThen(
                      toList(),
                      lists -> {
                        checkArgument(!lists.isEmpty(), "No messages found with name %s", name);
                        checkArgument(
                            lists.size() == 1, "More than one message found with name %s", name);
                        return lists.get(0);
                      }));
    } else {
      messages = ir.getMessage(irOptions.messageId());
      checkArgument(messages != null, "No message found with id %s", irOptions.messageId());
    }

    return new TokenIterator(messages);
  }

  /** Handles creating a field from the iterator. */
  private static SbeField processPrimitive(TokenIterator iterator) {
    checkArgument(iterator.hasNext());

    PrimitiveSbeField.Builder primitiveField = PrimitiveSbeField.builder();
    Token beginMessageToken = iterator.current();
    primitiveField.setName(beginMessageToken.name());
    // At least for primitive fields, the presence is either REQUIRED or OPTIONAL, never CONSTANT.
    primitiveField.setIsRequired(beginMessageToken.encoding().presence() == Presence.REQUIRED);

    Token encodingToken = iterator.next();
    primitiveField.setType(encodingToken.encoding().primitiveType());

    return primitiveField.build();
  }

  /** {@link Iterator} over {@link Token}s with support for getting current one. */
  private static final class TokenIterator implements Iterator<Token> {
    private final List<Token> tokens;
    private int idx;

    private TokenIterator(List<Token> tokens) {
      this.tokens = tokens;
      this.idx = -1;
    }

    @Override
    public boolean hasNext() {
      return idx < tokens.size() - 1;
    }

    @Override
    public Token next() {
      ++idx;
      return tokens.get(idx);
    }

    public Token current() {
      return tokens.get(idx);
    }
  }
}
