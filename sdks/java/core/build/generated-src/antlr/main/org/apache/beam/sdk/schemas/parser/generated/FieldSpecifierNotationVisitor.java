// Generated from org\apache\beam\sdk\schemas\parser\generated\FieldSpecifierNotation.g4 by ANTLR 4.7

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
package org.apache.beam.sdk.schemas.parser.generated;

import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link FieldSpecifierNotationParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface FieldSpecifierNotationVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link FieldSpecifierNotationParser#fieldSpecifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFieldSpecifier(FieldSpecifierNotationParser.FieldSpecifierContext ctx);
	/**
	 * Visit a parse tree produced by {@link FieldSpecifierNotationParser#dotExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDotExpression(FieldSpecifierNotationParser.DotExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code qualifyComponent}
	 * labeled alternative in {@link FieldSpecifierNotationParser#dotExpressionComponent}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQualifyComponent(FieldSpecifierNotationParser.QualifyComponentContext ctx);
	/**
	 * Visit a parse tree produced by the {@code simpleIdentifier}
	 * labeled alternative in {@link FieldSpecifierNotationParser#dotExpressionComponent}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSimpleIdentifier(FieldSpecifierNotationParser.SimpleIdentifierContext ctx);
	/**
	 * Visit a parse tree produced by the {@code wildcard}
	 * labeled alternative in {@link FieldSpecifierNotationParser#dotExpressionComponent}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWildcard(FieldSpecifierNotationParser.WildcardContext ctx);
	/**
	 * Visit a parse tree produced by {@link FieldSpecifierNotationParser#qualifiedComponent}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQualifiedComponent(FieldSpecifierNotationParser.QualifiedComponentContext ctx);
	/**
	 * Visit a parse tree produced by the {@code arrayQualifierList}
	 * labeled alternative in {@link FieldSpecifierNotationParser#qualifierList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitArrayQualifierList(FieldSpecifierNotationParser.ArrayQualifierListContext ctx);
	/**
	 * Visit a parse tree produced by the {@code mapQualifierList}
	 * labeled alternative in {@link FieldSpecifierNotationParser#qualifierList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMapQualifierList(FieldSpecifierNotationParser.MapQualifierListContext ctx);
	/**
	 * Visit a parse tree produced by {@link FieldSpecifierNotationParser#arrayQualifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitArrayQualifier(FieldSpecifierNotationParser.ArrayQualifierContext ctx);
	/**
	 * Visit a parse tree produced by {@link FieldSpecifierNotationParser#mapQualifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMapQualifier(FieldSpecifierNotationParser.MapQualifierContext ctx);
}