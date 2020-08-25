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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.List;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.TokenStream;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor.ListQualifier;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor.MapQualifier;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor.Qualifier;
import org.apache.beam.sdk.schemas.parser.generated.FieldSpecifierNotationBaseVisitor;
import org.apache.beam.sdk.schemas.parser.generated.FieldSpecifierNotationLexer;
import org.apache.beam.sdk.schemas.parser.generated.FieldSpecifierNotationParser;
import org.apache.beam.sdk.schemas.parser.generated.FieldSpecifierNotationParser.ArrayQualifierListContext;
import org.apache.beam.sdk.schemas.parser.generated.FieldSpecifierNotationParser.DotExpressionContext;
import org.apache.beam.sdk.schemas.parser.generated.FieldSpecifierNotationParser.FieldSpecifierContext;
import org.apache.beam.sdk.schemas.parser.generated.FieldSpecifierNotationParser.MapQualifierListContext;
import org.apache.beam.sdk.schemas.parser.generated.FieldSpecifierNotationParser.QualifiedComponentContext;
import org.apache.beam.sdk.schemas.parser.generated.FieldSpecifierNotationParser.QualifyComponentContext;
import org.apache.beam.sdk.schemas.parser.generated.FieldSpecifierNotationParser.SimpleIdentifierContext;
import org.apache.beam.sdk.schemas.parser.generated.FieldSpecifierNotationParser.WildcardContext;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Parser for textual field-access selector. */
@Internal
public class FieldAccessDescriptorParser {
  public static FieldAccessDescriptor parse(String expr) {
    CharStream charStream = CharStreams.fromString(expr);
    FieldSpecifierNotationLexer lexer = new FieldSpecifierNotationLexer(charStream);
    TokenStream tokens = new CommonTokenStream(lexer);
    FieldSpecifierNotationParser parser = new FieldSpecifierNotationParser(tokens);
    return new BuildFieldAccessDescriptor().visit(parser.dotExpression());
  }

  private static class BuildFieldAccessDescriptor
      extends FieldSpecifierNotationBaseVisitor<FieldAccessDescriptor> {

    @Override
    public FieldAccessDescriptor visitFieldSpecifier(FieldSpecifierContext ctx) {
      return ctx.dotExpression().accept(this);
    }

    @Override
    public FieldAccessDescriptor visitDotExpression(DotExpressionContext ctx) {
      List<FieldAccessDescriptor> components =
          ctx.dotExpressionComponent().stream()
              .map(dotE -> dotE.accept(this))
              .collect(Collectors.toList());

      // Walk backwards through the list to build up the nested FieldAccessDescriptor.
      checkArgument(!components.isEmpty());
      FieldAccessDescriptor fieldAccessDescriptor = components.get(components.size() - 1);
      for (int i = components.size() - 2; i >= 0; --i) {
        FieldAccessDescriptor component = components.get(i);
        if (component.getAllFields()) {
          throw new IllegalArgumentException(
              "We currently only support wildcards at terminal"
                  + " parts of selectors. 'x.*' is allowed, but x.*.y is not currently allowed.");
          // TODO: We should support expanding out x.*.y expressions.
        }
        FieldDescriptor fieldAccessed =
            component.getFieldsAccessed().stream()
                .findFirst()
                .orElseThrow(IllegalArgumentException::new);

        fieldAccessDescriptor =
            FieldAccessDescriptor.withFields()
                .withNestedField(fieldAccessed, fieldAccessDescriptor);
      }
      return fieldAccessDescriptor;
    }

    @Override
    public FieldAccessDescriptor visitQualifyComponent(QualifyComponentContext ctx) {
      return ctx.qualifiedComponent().accept(this);
    }

    @Override
    public FieldAccessDescriptor visitSimpleIdentifier(SimpleIdentifierContext ctx) {
      FieldDescriptor field =
          FieldDescriptor.builder().setFieldName(ctx.IDENTIFIER().getText()).build();
      return FieldAccessDescriptor.withFields(field);
    }

    @Override
    public FieldAccessDescriptor visitWildcard(WildcardContext ctx) {
      return FieldAccessDescriptor.withAllFields();
    }

    @Override
    public FieldAccessDescriptor visitQualifiedComponent(QualifiedComponentContext ctx) {
      QualifierVisitor qualifierVisitor = new QualifierVisitor();
      ctx.qualifierList().accept(qualifierVisitor);
      FieldDescriptor field =
          FieldDescriptor.builder()
              .setFieldName(ctx.IDENTIFIER().getText())
              .setQualifiers(qualifierVisitor.getQualifiers())
              .build();
      return FieldAccessDescriptor.withFields(field);
    }
  }

  private static class QualifierVisitor
      extends FieldSpecifierNotationBaseVisitor<FieldAccessDescriptor> {
    private final List<Qualifier> qualifiers = Lists.newArrayList();

    @Override
    public @Nullable FieldAccessDescriptor visitArrayQualifierList(ArrayQualifierListContext ctx) {
      // TODO: Change once we support slices and selectors.
      qualifiers.add(Qualifier.of(ListQualifier.ALL));
      ctx.qualifierList().forEach(subList -> subList.accept(this));
      return null;
    }

    @Override
    public @Nullable FieldAccessDescriptor visitMapQualifierList(MapQualifierListContext ctx) {
      // TODO: Change once we support slices and selectors.
      qualifiers.add(Qualifier.of(MapQualifier.ALL));
      ctx.qualifierList().forEach(subList -> subList.accept(this));
      return null;
    }

    public List<Qualifier> getQualifiers() {
      return qualifiers;
    }
  }
}
