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

import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link FieldSpecifierNotationParser}.
 */
public interface FieldSpecifierNotationListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link FieldSpecifierNotationParser#fieldSpecifier}.
	 * @param ctx the parse tree
	 */
	void enterFieldSpecifier(FieldSpecifierNotationParser.FieldSpecifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link FieldSpecifierNotationParser#fieldSpecifier}.
	 * @param ctx the parse tree
	 */
	void exitFieldSpecifier(FieldSpecifierNotationParser.FieldSpecifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link FieldSpecifierNotationParser#dotExpression}.
	 * @param ctx the parse tree
	 */
	void enterDotExpression(FieldSpecifierNotationParser.DotExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link FieldSpecifierNotationParser#dotExpression}.
	 * @param ctx the parse tree
	 */
	void exitDotExpression(FieldSpecifierNotationParser.DotExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code qualifyComponent}
	 * labeled alternative in {@link FieldSpecifierNotationParser#dotExpressionComponent}.
	 * @param ctx the parse tree
	 */
	void enterQualifyComponent(FieldSpecifierNotationParser.QualifyComponentContext ctx);
	/**
	 * Exit a parse tree produced by the {@code qualifyComponent}
	 * labeled alternative in {@link FieldSpecifierNotationParser#dotExpressionComponent}.
	 * @param ctx the parse tree
	 */
	void exitQualifyComponent(FieldSpecifierNotationParser.QualifyComponentContext ctx);
	/**
	 * Enter a parse tree produced by the {@code simpleIdentifier}
	 * labeled alternative in {@link FieldSpecifierNotationParser#dotExpressionComponent}.
	 * @param ctx the parse tree
	 */
	void enterSimpleIdentifier(FieldSpecifierNotationParser.SimpleIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by the {@code simpleIdentifier}
	 * labeled alternative in {@link FieldSpecifierNotationParser#dotExpressionComponent}.
	 * @param ctx the parse tree
	 */
	void exitSimpleIdentifier(FieldSpecifierNotationParser.SimpleIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by the {@code wildcard}
	 * labeled alternative in {@link FieldSpecifierNotationParser#dotExpressionComponent}.
	 * @param ctx the parse tree
	 */
	void enterWildcard(FieldSpecifierNotationParser.WildcardContext ctx);
	/**
	 * Exit a parse tree produced by the {@code wildcard}
	 * labeled alternative in {@link FieldSpecifierNotationParser#dotExpressionComponent}.
	 * @param ctx the parse tree
	 */
	void exitWildcard(FieldSpecifierNotationParser.WildcardContext ctx);
	/**
	 * Enter a parse tree produced by {@link FieldSpecifierNotationParser#qualifiedComponent}.
	 * @param ctx the parse tree
	 */
	void enterQualifiedComponent(FieldSpecifierNotationParser.QualifiedComponentContext ctx);
	/**
	 * Exit a parse tree produced by {@link FieldSpecifierNotationParser#qualifiedComponent}.
	 * @param ctx the parse tree
	 */
	void exitQualifiedComponent(FieldSpecifierNotationParser.QualifiedComponentContext ctx);
	/**
	 * Enter a parse tree produced by the {@code arrayQualifierList}
	 * labeled alternative in {@link FieldSpecifierNotationParser#qualifierList}.
	 * @param ctx the parse tree
	 */
	void enterArrayQualifierList(FieldSpecifierNotationParser.ArrayQualifierListContext ctx);
	/**
	 * Exit a parse tree produced by the {@code arrayQualifierList}
	 * labeled alternative in {@link FieldSpecifierNotationParser#qualifierList}.
	 * @param ctx the parse tree
	 */
	void exitArrayQualifierList(FieldSpecifierNotationParser.ArrayQualifierListContext ctx);
	/**
	 * Enter a parse tree produced by the {@code mapQualifierList}
	 * labeled alternative in {@link FieldSpecifierNotationParser#qualifierList}.
	 * @param ctx the parse tree
	 */
	void enterMapQualifierList(FieldSpecifierNotationParser.MapQualifierListContext ctx);
	/**
	 * Exit a parse tree produced by the {@code mapQualifierList}
	 * labeled alternative in {@link FieldSpecifierNotationParser#qualifierList}.
	 * @param ctx the parse tree
	 */
	void exitMapQualifierList(FieldSpecifierNotationParser.MapQualifierListContext ctx);
	/**
	 * Enter a parse tree produced by {@link FieldSpecifierNotationParser#arrayQualifier}.
	 * @param ctx the parse tree
	 */
	void enterArrayQualifier(FieldSpecifierNotationParser.ArrayQualifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link FieldSpecifierNotationParser#arrayQualifier}.
	 * @param ctx the parse tree
	 */
	void exitArrayQualifier(FieldSpecifierNotationParser.ArrayQualifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link FieldSpecifierNotationParser#mapQualifier}.
	 * @param ctx the parse tree
	 */
	void enterMapQualifier(FieldSpecifierNotationParser.MapQualifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link FieldSpecifierNotationParser#mapQualifier}.
	 * @param ctx the parse tree
	 */
	void exitMapQualifier(FieldSpecifierNotationParser.MapQualifierContext ctx);
}