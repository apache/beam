/*
 * Copyright 2012 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

tree grammar DocumentGenerator;

options {
    tokenVocab = Thrift;
    output = AST;
    ASTLabelType = CommonTree;
}

@header {
    package org.apache.beam.sdk.io.thrift.parser.antlr;

    import org.apache.beam.sdk.io.thrift.parser.model.*;
    import org.apache.beam.sdk.io.thrift.parser.util.*;

    import java.util.ArrayList;
    import java.util.HashMap;
    import java.util.List;
    import java.util.Map;
    import java.util.AbstractMap;
}


document returns [Document value]
    : DOCUMENT                       { $value = Document.emptyDocument(); }
    | ^(DOCUMENT header definitions) { $value = new Document($header.value, $definitions.value); }
    ;


header returns [Header value]
@init {
    List<String> includes = new ArrayList<>();
    List<String> cppIncludes = new ArrayList<>();
    String defaultNamespace = null;
    Map<String, String> namespaces = new HashMap<>();
}
@after {
    $value = new Header(includes, cppIncludes, defaultNamespace, namespaces);
}
    : ( include           { includes.add($include.value); }
      | cpp_include       { cppIncludes.add($cpp_include.value); }
      | default_namespace { defaultNamespace = $default_namespace.value; }
      | namespace         { namespaces.put($namespace.language, $namespace.value); }
      )*
    ;

include returns [String value]
    : ^(INCLUDE LITERAL) { $value = $LITERAL.text; }
    ;

default_namespace returns [String value]
    : ^(DEFAULT_NAMESPACE (v=IDENTIFIER | v=LITERAL)) { $value = $v.text; }
    ;

namespace returns [String language, String value]
    : ^(NAMESPACE k=IDENTIFIER (v=IDENTIFIER | v=LITERAL)) { $language = $k.text ; $value = $v.text; }
    ;

cpp_include returns [String value]
    : ^(CPP_INCLUDE LITERAL) { $value = $LITERAL.text; }
    ;


definitions returns [List<Definition> value = new ArrayList<>()]
    : ( definition { $value.add($definition.value); } )*
    ;

definition returns [Definition value]
    : const_rule { $value = $const_rule.value; }
    | typedef    { $value = $typedef.value; }
    | enum_rule  { $value = $enum_rule.value; }
    | senum      { $value = $senum.value; }
    | struct     { $value = $struct.value; }
    | union      { $value = $union.value; }
    | exception  { $value = $exception.value; }
    | service    { $value = $service.value; }
    ;

const_rule returns [Const value]
    : ^(CONST k=IDENTIFIER t=field_type v=const_value) { $value = new Const($k.text, $t.value, $v.value); }
    ;

typedef returns [Typedef value]
    : ^(TYPEDEF k=IDENTIFIER t=field_type a=type_annotations) { $value = new Typedef($k.text, $t.value, $a.value); }
    ;

enum_rule returns [IntegerEnum value]
    : ^(ENUM k=IDENTIFIER v=enum_fields t=type_annotations) { $value = new IntegerEnum($k.text, $v.value, $t.value); }
    ;

senum returns [StringEnum value]
    : ^(SENUM k=IDENTIFIER v=senum_values) { $value = new StringEnum($k.text, $v.value); }
    ;

struct returns [Struct value]
    : ^(STRUCT k=IDENTIFIER f=fields t=type_annotations) { $value = new Struct($k.text, $f.value, $t.value); }
    ;

union returns [Union value]
    : ^(UNION k=IDENTIFIER f=fields t=type_annotations) { $value = new Union($k.text, $f.value, $t.value); }
    ;

exception returns [ThriftException value]
    : ^(EXCEPTION k=IDENTIFIER f=fields t=type_annotations) { $value = new ThriftException($k.text, $f.value, $t.value); }
    ;

service returns [Service value]
    : ^(SERVICE k=IDENTIFIER ^(EXTENDS e=IDENTIFIER?) f=functions t=type_annotations) { $value = new Service($k.text, $e.text, $f.value, $t.value); }
    ;


const_value returns [ConstValue value]
    : i=integer     { $value = new ConstInteger($i.value); }
    | d=DOUBLE      { $value = new ConstDouble(Double.parseDouble($d.text)); }
    | s=LITERAL     { $value = new ConstString($s.text); }
    | s=IDENTIFIER  { $value = new ConstIdentifier($s.text); }
    | l=const_list  { $value = new ConstList($l.value); }
    | m=const_map   { $value = new ConstMap($m.value); }
    ;

const_list returns [List<ConstValue> value = new ArrayList<>()]
    : ^(LIST ( v=const_value { $value.add($v.value); } )*)
    ;

const_map returns [Map<ConstValue, ConstValue> value = new HashMap<>()]
    : ^(MAP ( e=const_map_entry { $value.put($e.value.getKey(), $e.value.getValue()); } )* )
    ;

const_map_entry returns [Map.Entry<ConstValue, ConstValue> value]
    : ^(ENTRY k=const_value v=const_value) {
        $value = new AbstractMap.SimpleImmutableEntry<ConstValue, ConstValue>($k.value, $v.value);
    }
    ;

enum_fields returns [IntegerEnumFieldList value = new IntegerEnumFieldList()]
    : ( enum_field[$value] { $value.add($enum_field.value); } )*
    ;

enum_field[IntegerEnumFieldList fieldList] returns [IntegerEnumField value]
    : ^(k=IDENTIFIER v=integer? t=type_annotations) {
         $value = new IntegerEnumField($k.text, $v.value, $fieldList.getNextImplicitEnumerationValue(), $t.value);
    }
    ;

senum_values returns [List<String> value = new ArrayList<>()]
    : ( v=LITERAL { $value.add($v.text); } )*
    ;

fields returns [List<ThriftField> value = new ArrayList<>()]
    : ( field { $value.add($field.value); } )*
    ;

functions returns [List<ThriftMethod> value = new ArrayList<>()]
    : (function { $value.add($function.value); } )*
    ;


field returns [ThriftField value]
    : ^(FIELD k=IDENTIFIER t=field_type i=integer? r=field_req c=const_value? a=type_annotations)
        { $value = new ThriftField($k.text, $t.value, $i.value, $r.value, $c.value, $a.value); }
    ;

field_req returns [ThriftField.Requiredness value]
    : REQUIREDNESS             { $value = ThriftField.Requiredness.NONE; }
    | ^(REQUIREDNESS REQUIRED) { $value = ThriftField.Requiredness.REQUIRED; }
    | ^(REQUIREDNESS OPTIONAL) { $value = ThriftField.Requiredness.OPTIONAL; }
    ;


function returns [ThriftMethod value]
    : ^(METHOD k=IDENTIFIER t=function_type f=args o=oneway r=throws_list a=type_annotations)
        { $value = new ThriftMethod($k.text, $t.value, $f.value, $o.value, $r.value, $a.value); }
    ;

args returns [List<ThriftField> value = new ArrayList<>()]
    : ARGS
    | ^(ARGS f=fields) { $value = $f.value; }
    ;

oneway returns [boolean value]
    : ONEWAY { $value = true; }
    |        { $value = false; }
    ;

function_type returns [ThriftType value]
    : field_type { $value = $field_type.value; }
    | VOID       { $value = new VoidType(); }
    ;

throws_list returns [List<ThriftField> value = new ArrayList<>()]
    : ( ^(THROWS fields) { $value = $fields.value; } )?
    ;


type_annotations returns [List<TypeAnnotation> value = new ArrayList<>()]
    : ( ^(TYPES ( t=type_annotation { $value.add($t.value); } )* ) )?
    ;

type_annotation returns [TypeAnnotation value]
    : ^(TYPE k=IDENTIFIER v=annotation_value) { $value = new TypeAnnotation($k.text, $v.value); }
    ;

annotation_value returns [String value]
    : i=integer { $value = String.valueOf($i.value); }
    | s=LITERAL { $value = $s.text; }
    ;


field_type returns [ThriftType value]
    : b=base_type      { $value = $b.value; }
    | s=IDENTIFIER     { $value = new IdentifierType($s.text); }
    | c=container_type { $value = $c.value; }
    ;

base_type returns [BaseType value]
    : b=real_base_type t=type_annotations { $value = new BaseType($b.value, $t.value); }
    ;

real_base_type returns [BaseType.Type value]
    : TYPE_BOOL   { $value = BaseType.Type.BOOL; }
    | TYPE_BYTE   { $value = BaseType.Type.BYTE; }
    | TYPE_I16    { $value = BaseType.Type.I16; }
    | TYPE_I32    { $value = BaseType.Type.I32; }
    | TYPE_I64    { $value = BaseType.Type.I64; }
    | TYPE_DOUBLE { $value = BaseType.Type.DOUBLE; }
    | TYPE_STRING { $value = BaseType.Type.STRING; }
    | TYPE_BINARY { $value = BaseType.Type.BINARY; }
    ;

container_type returns [ContainerType value]
    : ^(MAP k=field_type v=field_type c=cpp_type?) a=type_annotations
        { $value = new MapType($k.value, $v.value, $c.value, $a.value); }
    | ^(SET t=field_type c=cpp_type?) a=type_annotations
        { $value = new SetType($t.value, $c.value, $a.value); }
    | ^(LIST t=field_type c=cpp_type?) a=type_annotations
        { $value = new ListType($t.value, $c.value, $a.value); }
    ;

cpp_type returns [String value]
    : ^(CPP_TYPE s=LITERAL) { $value = $s.text; }
    ;


integer returns [Long value]
    : i=INTEGER     { $value = Long.parseLong($i.text); }
    | h=HEX_INTEGER { $value = Long.decode($h.text); }
    ;
