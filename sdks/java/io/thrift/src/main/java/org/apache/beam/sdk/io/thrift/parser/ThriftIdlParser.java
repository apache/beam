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
package org.apache.beam.sdk.io.thrift.parser;

import java.io.IOException;
import java.io.Reader;
import org.antlr.runtime.ANTLRReaderStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.BufferedTreeNodeStream;
import org.antlr.runtime.tree.Tree;
import org.antlr.runtime.tree.TreeNodeStream;
import org.apache.beam.sdk.io.thrift.parser.antlr.DocumentGenerator;
import org.apache.beam.sdk.io.thrift.parser.antlr.ThriftLexer;
import org.apache.beam.sdk.io.thrift.parser.antlr.ThriftParser;
import org.apache.beam.sdk.io.thrift.parser.model.Document;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.CharSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftIdlParser {

  private static final Logger LOG = LoggerFactory.getLogger(ThriftIdlParser.class);

  /** Generates {@link Document} from {@link org.antlr.runtime.tree.Tree}. */
  public static Document parseThriftIdl(CharSource input) throws IOException {
    Tree tree = parseTree(input);
    TreeNodeStream stream = new BufferedTreeNodeStream(tree);
    DocumentGenerator generator = new DocumentGenerator(stream);
    try {
      return generator.document().value;
    } catch (RecognitionException e) {
      LOG.error("Failed to generate document: " + e.getMessage());
      throw new RuntimeException(e);
    }
  }

  /** Generates {@link org.antlr.runtime.tree.Tree} from input. */
  static Tree parseTree(CharSource input) throws IOException {
    try (Reader reader = input.openStream()) {
      ThriftLexer lexer = new ThriftLexer(new ANTLRReaderStream(reader));
      ThriftParser parser = new ThriftParser(new CommonTokenStream(lexer));
      try {
        Tree tree = (Tree) parser.document().getTree();
        if (parser.getNumberOfSyntaxErrors() > 0) {
          LOG.error("Parsing generated " + parser.getNumberOfSyntaxErrors() + " errors.");
          throw new RuntimeException("syntax error");
        }
        return tree;
      } catch (RecognitionException e) {
        LOG.error("Could not recognize input at " + e.index);
        throw new RuntimeException(e);
      }
    }
  }
}
