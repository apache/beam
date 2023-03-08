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

import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class FieldSpecifierNotationParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.7", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, IDENTIFIER=6, WILDCARD=7, WS=8;
	public static final int
		RULE_fieldSpecifier = 0, RULE_dotExpression = 1, RULE_dotExpressionComponent = 2, 
		RULE_qualifiedComponent = 3, RULE_qualifierList = 4, RULE_arrayQualifier = 5, 
		RULE_mapQualifier = 6;
	public static final String[] ruleNames = {
		"fieldSpecifier", "dotExpression", "dotExpressionComponent", "qualifiedComponent", 
		"qualifierList", "arrayQualifier", "mapQualifier"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'.'", "'[]'", "'[*]'", "'{}'", "'{*}'", null, "'*'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, null, null, null, "IDENTIFIER", "WILDCARD", "WS"
	};
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "FieldSpecifierNotation.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public FieldSpecifierNotationParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}
	public static class FieldSpecifierContext extends ParserRuleContext {
		public DotExpressionContext dotExpression() {
			return getRuleContext(DotExpressionContext.class,0);
		}
		public FieldSpecifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fieldSpecifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof FieldSpecifierNotationListener ) ((FieldSpecifierNotationListener)listener).enterFieldSpecifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof FieldSpecifierNotationListener ) ((FieldSpecifierNotationListener)listener).exitFieldSpecifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof FieldSpecifierNotationVisitor ) return ((FieldSpecifierNotationVisitor<? extends T>)visitor).visitFieldSpecifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FieldSpecifierContext fieldSpecifier() throws RecognitionException {
		FieldSpecifierContext _localctx = new FieldSpecifierContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_fieldSpecifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(14);
			dotExpression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DotExpressionContext extends ParserRuleContext {
		public List<DotExpressionComponentContext> dotExpressionComponent() {
			return getRuleContexts(DotExpressionComponentContext.class);
		}
		public DotExpressionComponentContext dotExpressionComponent(int i) {
			return getRuleContext(DotExpressionComponentContext.class,i);
		}
		public DotExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dotExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof FieldSpecifierNotationListener ) ((FieldSpecifierNotationListener)listener).enterDotExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof FieldSpecifierNotationListener ) ((FieldSpecifierNotationListener)listener).exitDotExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof FieldSpecifierNotationVisitor ) return ((FieldSpecifierNotationVisitor<? extends T>)visitor).visitDotExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DotExpressionContext dotExpression() throws RecognitionException {
		DotExpressionContext _localctx = new DotExpressionContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_dotExpression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(16);
			dotExpressionComponent();
			setState(21);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__0) {
				{
				{
				setState(17);
				match(T__0);
				setState(18);
				dotExpressionComponent();
				}
				}
				setState(23);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DotExpressionComponentContext extends ParserRuleContext {
		public DotExpressionComponentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dotExpressionComponent; }
	 
		public DotExpressionComponentContext() { }
		public void copyFrom(DotExpressionComponentContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class SimpleIdentifierContext extends DotExpressionComponentContext {
		public TerminalNode IDENTIFIER() { return getToken(FieldSpecifierNotationParser.IDENTIFIER, 0); }
		public SimpleIdentifierContext(DotExpressionComponentContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof FieldSpecifierNotationListener ) ((FieldSpecifierNotationListener)listener).enterSimpleIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof FieldSpecifierNotationListener ) ((FieldSpecifierNotationListener)listener).exitSimpleIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof FieldSpecifierNotationVisitor ) return ((FieldSpecifierNotationVisitor<? extends T>)visitor).visitSimpleIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class QualifyComponentContext extends DotExpressionComponentContext {
		public QualifiedComponentContext qualifiedComponent() {
			return getRuleContext(QualifiedComponentContext.class,0);
		}
		public QualifyComponentContext(DotExpressionComponentContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof FieldSpecifierNotationListener ) ((FieldSpecifierNotationListener)listener).enterQualifyComponent(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof FieldSpecifierNotationListener ) ((FieldSpecifierNotationListener)listener).exitQualifyComponent(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof FieldSpecifierNotationVisitor ) return ((FieldSpecifierNotationVisitor<? extends T>)visitor).visitQualifyComponent(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class WildcardContext extends DotExpressionComponentContext {
		public TerminalNode WILDCARD() { return getToken(FieldSpecifierNotationParser.WILDCARD, 0); }
		public WildcardContext(DotExpressionComponentContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof FieldSpecifierNotationListener ) ((FieldSpecifierNotationListener)listener).enterWildcard(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof FieldSpecifierNotationListener ) ((FieldSpecifierNotationListener)listener).exitWildcard(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof FieldSpecifierNotationVisitor ) return ((FieldSpecifierNotationVisitor<? extends T>)visitor).visitWildcard(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DotExpressionComponentContext dotExpressionComponent() throws RecognitionException {
		DotExpressionComponentContext _localctx = new DotExpressionComponentContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_dotExpressionComponent);
		try {
			setState(27);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,1,_ctx) ) {
			case 1:
				_localctx = new QualifyComponentContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(24);
				qualifiedComponent();
				}
				break;
			case 2:
				_localctx = new SimpleIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(25);
				match(IDENTIFIER);
				}
				break;
			case 3:
				_localctx = new WildcardContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(26);
				match(WILDCARD);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QualifiedComponentContext extends ParserRuleContext {
		public TerminalNode IDENTIFIER() { return getToken(FieldSpecifierNotationParser.IDENTIFIER, 0); }
		public QualifierListContext qualifierList() {
			return getRuleContext(QualifierListContext.class,0);
		}
		public QualifiedComponentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_qualifiedComponent; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof FieldSpecifierNotationListener ) ((FieldSpecifierNotationListener)listener).enterQualifiedComponent(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof FieldSpecifierNotationListener ) ((FieldSpecifierNotationListener)listener).exitQualifiedComponent(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof FieldSpecifierNotationVisitor ) return ((FieldSpecifierNotationVisitor<? extends T>)visitor).visitQualifiedComponent(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QualifiedComponentContext qualifiedComponent() throws RecognitionException {
		QualifiedComponentContext _localctx = new QualifiedComponentContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_qualifiedComponent);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(29);
			match(IDENTIFIER);
			setState(30);
			qualifierList();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QualifierListContext extends ParserRuleContext {
		public QualifierListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_qualifierList; }
	 
		public QualifierListContext() { }
		public void copyFrom(QualifierListContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class ArrayQualifierListContext extends QualifierListContext {
		public ArrayQualifierContext arrayQualifier() {
			return getRuleContext(ArrayQualifierContext.class,0);
		}
		public List<QualifierListContext> qualifierList() {
			return getRuleContexts(QualifierListContext.class);
		}
		public QualifierListContext qualifierList(int i) {
			return getRuleContext(QualifierListContext.class,i);
		}
		public ArrayQualifierListContext(QualifierListContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof FieldSpecifierNotationListener ) ((FieldSpecifierNotationListener)listener).enterArrayQualifierList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof FieldSpecifierNotationListener ) ((FieldSpecifierNotationListener)listener).exitArrayQualifierList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof FieldSpecifierNotationVisitor ) return ((FieldSpecifierNotationVisitor<? extends T>)visitor).visitArrayQualifierList(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class MapQualifierListContext extends QualifierListContext {
		public MapQualifierContext mapQualifier() {
			return getRuleContext(MapQualifierContext.class,0);
		}
		public List<QualifierListContext> qualifierList() {
			return getRuleContexts(QualifierListContext.class);
		}
		public QualifierListContext qualifierList(int i) {
			return getRuleContext(QualifierListContext.class,i);
		}
		public MapQualifierListContext(QualifierListContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof FieldSpecifierNotationListener ) ((FieldSpecifierNotationListener)listener).enterMapQualifierList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof FieldSpecifierNotationListener ) ((FieldSpecifierNotationListener)listener).exitMapQualifierList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof FieldSpecifierNotationVisitor ) return ((FieldSpecifierNotationVisitor<? extends T>)visitor).visitMapQualifierList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QualifierListContext qualifierList() throws RecognitionException {
		QualifierListContext _localctx = new QualifierListContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_qualifierList);
		try {
			int _alt;
			setState(46);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__1:
			case T__2:
				_localctx = new ArrayQualifierListContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(32);
				arrayQualifier();
				setState(36);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,2,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(33);
						qualifierList();
						}
						} 
					}
					setState(38);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,2,_ctx);
				}
				}
				break;
			case T__3:
			case T__4:
				_localctx = new MapQualifierListContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(39);
				mapQualifier();
				setState(43);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,3,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(40);
						qualifierList();
						}
						} 
					}
					setState(45);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,3,_ctx);
				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ArrayQualifierContext extends ParserRuleContext {
		public ArrayQualifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_arrayQualifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof FieldSpecifierNotationListener ) ((FieldSpecifierNotationListener)listener).enterArrayQualifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof FieldSpecifierNotationListener ) ((FieldSpecifierNotationListener)listener).exitArrayQualifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof FieldSpecifierNotationVisitor ) return ((FieldSpecifierNotationVisitor<? extends T>)visitor).visitArrayQualifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ArrayQualifierContext arrayQualifier() throws RecognitionException {
		ArrayQualifierContext _localctx = new ArrayQualifierContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_arrayQualifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(48);
			_la = _input.LA(1);
			if ( !(_la==T__1 || _la==T__2) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class MapQualifierContext extends ParserRuleContext {
		public MapQualifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_mapQualifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof FieldSpecifierNotationListener ) ((FieldSpecifierNotationListener)listener).enterMapQualifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof FieldSpecifierNotationListener ) ((FieldSpecifierNotationListener)listener).exitMapQualifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof FieldSpecifierNotationVisitor ) return ((FieldSpecifierNotationVisitor<? extends T>)visitor).visitMapQualifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final MapQualifierContext mapQualifier() throws RecognitionException {
		MapQualifierContext _localctx = new MapQualifierContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_mapQualifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(50);
			_la = _input.LA(1);
			if ( !(_la==T__3 || _la==T__4) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3\n\67\4\2\t\2\4\3"+
		"\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\3\2\3\2\3\3\3\3\3\3\7\3\26"+
		"\n\3\f\3\16\3\31\13\3\3\4\3\4\3\4\5\4\36\n\4\3\5\3\5\3\5\3\6\3\6\7\6%"+
		"\n\6\f\6\16\6(\13\6\3\6\3\6\7\6,\n\6\f\6\16\6/\13\6\5\6\61\n\6\3\7\3\7"+
		"\3\b\3\b\3\b\2\2\t\2\4\6\b\n\f\16\2\4\3\2\4\5\3\2\6\7\2\65\2\20\3\2\2"+
		"\2\4\22\3\2\2\2\6\35\3\2\2\2\b\37\3\2\2\2\n\60\3\2\2\2\f\62\3\2\2\2\16"+
		"\64\3\2\2\2\20\21\5\4\3\2\21\3\3\2\2\2\22\27\5\6\4\2\23\24\7\3\2\2\24"+
		"\26\5\6\4\2\25\23\3\2\2\2\26\31\3\2\2\2\27\25\3\2\2\2\27\30\3\2\2\2\30"+
		"\5\3\2\2\2\31\27\3\2\2\2\32\36\5\b\5\2\33\36\7\b\2\2\34\36\7\t\2\2\35"+
		"\32\3\2\2\2\35\33\3\2\2\2\35\34\3\2\2\2\36\7\3\2\2\2\37 \7\b\2\2 !\5\n"+
		"\6\2!\t\3\2\2\2\"&\5\f\7\2#%\5\n\6\2$#\3\2\2\2%(\3\2\2\2&$\3\2\2\2&\'"+
		"\3\2\2\2\'\61\3\2\2\2(&\3\2\2\2)-\5\16\b\2*,\5\n\6\2+*\3\2\2\2,/\3\2\2"+
		"\2-+\3\2\2\2-.\3\2\2\2.\61\3\2\2\2/-\3\2\2\2\60\"\3\2\2\2\60)\3\2\2\2"+
		"\61\13\3\2\2\2\62\63\t\2\2\2\63\r\3\2\2\2\64\65\t\3\2\2\65\17\3\2\2\2"+
		"\7\27\35&-\60";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}