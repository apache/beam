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
package org.apache.beam.sdk.schemas.parser;

import javax.annotation.Nullable;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.TokenStream;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.parser.generated.FieldSpecifierNotationBaseVisitor;
import org.apache.beam.sdk.schemas.parser.generated.FieldSpecifierNotationLexer;
import org.apache.beam.sdk.schemas.parser.generated.FieldSpecifierNotationParser;
import org.apache.beam.sdk.schemas.parser.generated.FieldSpecifierNotationParser.DotExpressionContext;
import org.apache.beam.sdk.schemas.parser.generated.FieldSpecifierNotationParser.FieldSpecifierContext;
import org.apache.beam.sdk.schemas.parser.generated.FieldSpecifierNotationParser.QualifiedComponentContext;
import org.apache.beam.sdk.schemas.parser.generated.FieldSpecifierNotationParser.QualifyComponentContext;
import org.apache.beam.sdk.schemas.parser.generated.FieldSpecifierNotationParser.SimpleIdentifierContext;
import org.apache.beam.sdk.schemas.parser.generated.FieldSpecifierNotationParser.WildcardContext;

/** Parse a textual representation of a field access descriptor. */
public class FieldAccessDescriptorParser {
  @Nullable
  /** Parse the string. */
  public static FieldAccessDescriptor parse(String expr) {
    CharStream charStream = CharStreams.fromString(expr);
    FieldSpecifierNotationLexer lexer = new FieldSpecifierNotationLexer(charStream);
    TokenStream tokens = new CommonTokenStream(lexer);
    FieldSpecifierNotationParser parser = new FieldSpecifierNotationParser(tokens);
    return new BuildFieldAccessDescriptor().visit(parser.dotExpression());
  }

  private static class BuildFieldAccessDescriptor
      extends FieldSpecifierNotationBaseVisitor<FieldAccessDescriptor> {

    @Nullable
    @Override
    public FieldAccessDescriptor visitFieldSpecifier(FieldSpecifierContext ctx) {
      return ctx.dotExpression().accept(this);
    }

    @Nullable
    @Override
    public FieldAccessDescriptor visitDotExpression(DotExpressionContext ctx) {
      return null;
    }

    @Nullable
    @Override
    public FieldAccessDescriptor visitQualifyComponent(QualifyComponentContext ctx) {
      return ctx.qualifiedComponent().accept(this);
    }

    @Nullable
    @Override
    public FieldAccessDescriptor visitSimpleIdentifier(SimpleIdentifierContext ctx) {
      return null;
    }

    @Nullable
    @Override
    public FieldAccessDescriptor visitWildcard(WildcardContext ctx) {
      return null;
    }

    @Nullable
    @Override
    public FieldAccessDescriptor visitQualifiedComponent(QualifiedComponentContext ctx) {
      return null;
    }
  }
}
