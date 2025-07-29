<#-- Licensed to the Apache Software Foundation (ASF) under one or more contributor
  license agreements. See the NOTICE file distributed with this work for additional
  information regarding copyright ownership. The ASF licenses this file to
  You under the Apache License, Version 2.0 (the "License"); you may not use
  this file except in compliance with the License. You may obtain a copy of
  the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required
  by applicable law or agreed to in writing, software distributed under the
  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
  OF ANY KIND, either express or implied. See the License for the specific
  language governing permissions and limitations under the License. -->

boolean IfNotExistsOpt() :
{
}
{
    <IF> <NOT> <EXISTS> { return true; }
|
    { return false; }
}

boolean IfExistsOpt() :
{
}
{
    <IF> <EXISTS> { return true; }
|
    { return false; }
}

boolean CascadeOpt() :
{
}
{
    <CASCADE> { return true; }
|
    { return false; }
}

SqlNodeList Options() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <OPTIONS> { s = span(); } <LPAREN>
    [
        Option(list)
        (
            <COMMA>
            Option(list)
        )*
    ]
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

void Option(List<SqlNode> list) :
{
    final SqlIdentifier id;
    final SqlNode value;
}
{
    id = SimpleIdentifier()
    value = Literal() {
        list.add(id);
        list.add(value);
    }
}

List<Schema.Field> FieldListParens() :
{
    final List<Schema.Field> fields;
}
{
    <LPAREN>
        fields = FieldListBody()
    <RPAREN>
    {
        return fields;
    }
}

List<Schema.Field> FieldListAngular() :
{
    final List<Schema.Field> fields;
}
{
    <LT>
        fields = FieldListBody()
    <GT>
    {
        return fields;
    }
}

List<Schema.Field> FieldListBody() :
{
    final List<Schema.Field> fields = new ArrayList<Schema.Field>();
    Schema.Field field = null;
}
{
    field = Field() { fields.add(field); }
    (
        <COMMA> field = Field() { fields.add(field); }
    )*
    {
        return fields;
    }
}

Schema.Field Field() :
{
    final String name;
    final Schema.FieldType type;
    final boolean nullable;
    Schema.Field field = null;
    SqlNode comment = null;
}
{
    name = Identifier()
    type = FieldType()
    {
        field = Schema.Field.of(name, type);
    }
    (
        <NULL> { field = field.withNullable(true); }
    |
        <NOT> <NULL> { field = field.withNullable(false); }
    |
        { field = field.withNullable(true); }
    )
    [
        <COMMENT> comment = StringLiteral()
        {
            if (comment != null) {
                String commentString =
                    ((NlsString) SqlLiteral.value(comment)).getValue();
                field = field.withDescription(commentString);
            }
        }
    ]
    {
        return field;
    }
}

SqlNodeList PropertyList() :
{
    SqlNodeList list = new SqlNodeList(getPos());
    SqlNode property;
}
{
    property = Property() { list.add(property); }
    (
        <COMMA> property = Property() { list.add(property); }
    )*
    {
        return list;
    }
}


SqlNode Property() :
{
    SqlNode key;
    SqlNode value;
}
{
    key = StringLiteral()
    <EQ>
    value = StringLiteral()
    {
        SqlNodeList pair = new SqlNodeList(getPos());
        pair.add(key);
        pair.add(value);
        return pair;
    }
}

/**
 * CREATE CATALOG ( IF NOT EXISTS )? catalog_name
 *   TYPE type_name
 *   ( PROPERTIES '(' key = value ( ',' key = value )* ')' )?
 */
SqlCreate SqlCreateCatalog(Span s, boolean replace) :
{
    final boolean ifNotExists;
    final SqlNode catalogName;
    final SqlNode type;
    SqlNodeList properties = null;
}
{

    <CATALOG> {
        s.add(this);
    }

    ifNotExists = IfNotExistsOpt()
    (
        catalogName = StringLiteral()
        |
        catalogName = SimpleIdentifier()
    )
    <TYPE>
    (
        type = StringLiteral()
        |
        type = SimpleIdentifier()
    )
    [ <PROPERTIES> <LPAREN> properties = PropertyList() <RPAREN> ]

    {
        return new SqlCreateCatalog(
            s.end(this),
            replace,
            ifNotExists,
            catalogName,
            type,
            properties);
    }
}

/**
 * USE CATALOG catalog_name
 */
SqlCall SqlUseCatalog(Span s, String scope) :
{
    final SqlNode catalogName;
}
{
    <USE> {
        s.add(this);
    }
    <CATALOG>
    (
        catalogName = StringLiteral()
        |
        catalogName = SimpleIdentifier()
    )
    {
        return new SqlUseCatalog(
            s.end(this),
            scope,
            catalogName);
    }
}


SqlDrop SqlDropCatalog(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlNode catalogName;
}
{
    <CATALOG> ifExists = IfExistsOpt()
    (
        catalogName = StringLiteral()
        |
        catalogName = SimpleIdentifier()
    )
    {
        return new SqlDropCatalog(s.end(this), ifExists, catalogName);
    }
}

/**
 * CREATE DATABASE ( IF NOT EXISTS )? database_name
 */
SqlCreate SqlCreateDatabase(Span s, boolean replace) :
{
    final boolean ifNotExists;
    final SqlNode databaseName;
}
{
    <DATABASE> {
        s.add(this);
    }

    ifNotExists = IfNotExistsOpt()
    (
        databaseName = StringLiteral()
        |
        databaseName = SimpleIdentifier()
    )

    {
        return new SqlCreateDatabase(
            s.end(this),
            replace,
            ifNotExists,
            databaseName);
    }
}

/**
 * USE DATABASE database_name
 */
SqlCall SqlUseDatabase(Span s, String scope) :
{
    final SqlNode databaseName;
}
{
    <USE> {
        s.add(this);
    }
    <DATABASE>
    (
        databaseName = StringLiteral()
        |
        databaseName = SimpleIdentifier()
    )
    {
        return new SqlUseDatabase(
            s.end(this),
            scope,
            databaseName);
    }
}

/**
 * DROP DATABASE [ IF EXISTS ] database_name [ RESTRICT | CASCADE ]
 */
SqlDrop SqlDropDatabase(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlNode databaseName;
    final boolean cascade;
}
{
    <DATABASE>
    ifExists = IfExistsOpt()
    (
        databaseName = StringLiteral()
        |
        databaseName = SimpleIdentifier()
    )

    cascade = CascadeOpt()

    {
        return new SqlDropDatabase(s.end(this), ifExists, databaseName, cascade);
    }
}


SqlNodeList PartitionFieldList() :
{
    final List<SqlNode> list = new ArrayList<SqlNode>();
    SqlNode field;
}
{
    field = StringLiteral() { list.add(field); }
    (
        <COMMA> field = StringLiteral() { list.add(field); }
    )*
    {
        return new SqlNodeList(list, getPos());
    }
}

/**
 * Note: This example is probably out of sync with the code.
 *
 * CREATE EXTERNAL TABLE ( IF NOT EXISTS )?
 *   ( database_name '.' )? table_name '(' column_def ( ',' column_def )* ')'
 *   TYPE type_name
 *   ( PARTITIONED BY '(' partition_field ( ',' partition_field )* ')' )?
 *   ( COMMENT comment_string )?
 *   ( LOCATION location_string )?
 *   ( TBLPROPERTIES tbl_properties )?
 */
SqlCreate SqlCreateExternalTable(Span s, boolean replace) :
{
    final boolean ifNotExists;
    final SqlIdentifier id;
    List<Schema.Field> fieldList = null;
    final SqlNode type;
    SqlNodeList partitionFields = null;
    SqlNode comment = null;
    SqlNode location = null;
    SqlNode tblProperties = null;
}
{

    <EXTERNAL> <TABLE> {
        s.add(this);
    }

    ifNotExists = IfNotExistsOpt()
    id = CompoundIdentifier()
    fieldList = FieldListParens()
    <TYPE>
    (
        type = StringLiteral()
    |
        type = SimpleIdentifier()
    )
    [ <PARTITIONED> <BY> <LPAREN> partitionFields = PartitionFieldList() <RPAREN> ]
    [ <COMMENT> comment = StringLiteral() ]
    [ <LOCATION> location = StringLiteral() ]
    [ <TBLPROPERTIES> tblProperties = StringLiteral() ]
    {
        return
            new SqlCreateExternalTable(
                s.end(this),
                replace,
                ifNotExists,
                id,
                fieldList,
                type,
                partitionFields,
                comment,
                location,
                tblProperties);
    }
}

SqlCreate SqlCreateFunction(Span s, boolean replace) :
{
    boolean isAggregate = false;
    final SqlIdentifier name;
    final SqlNode jarName;
}
{
    (
        <AGGREGATE> {
            isAggregate = true;
        }
    )?
    <FUNCTION> {
        s.add(this);
    }
    name = CompoundIdentifier()
    <USING> <JAR>
        jarName = StringLiteral()
    {
        return
            new SqlCreateFunction(
                s.end(this),
                replace,
                name,
                jarName,
                isAggregate);
    }
}

SqlCreate SqlCreateTableNotSupportedMessage(Span s, boolean replace) :
{
}
{
  <TABLE>
  {
    throw new ParseException("'CREATE TABLE' is not supported in SQL. You can use "
    + "'CREATE EXTERNAL TABLE' to register an external data source to SQL. For more details, "
    + "please check: https://beam.apache.org/documentation/dsls/sql/create-external-table");
  }
}

SqlDrop SqlDropTable(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
}
{
    <TABLE> ifExists = IfExistsOpt() id = CompoundIdentifier() {
        return SqlDdlNodes.dropTable(s.end(this), ifExists, id);
    }
}

Schema.FieldType FieldType() :
{
    final SqlTypeName collectionTypeName;
    Schema.FieldType fieldType;
    final Span s = Span.of();
}
{
    (
        fieldType = Map()
    |
        fieldType = Array()
    |
        fieldType = Row()
    |
        fieldType = SimpleType()
    )
    {
        return fieldType;
    }
}

Schema.FieldType Array() :
{
    final Schema.FieldType arrayElementType;
}
{
    <ARRAY> <LT> arrayElementType = FieldType() <GT>
    {
        return Schema.FieldType.array(arrayElementType);
    }

}

Schema.FieldType Map() :
{
    final Schema.FieldType mapKeyType;
    final Schema.FieldType mapValueType;
}
{
    <MAP>
        <LT>
            mapKeyType = SimpleType()
        <COMMA>
            mapValueType = FieldType()
        <GT>
    {
        return Schema.FieldType.map(mapKeyType, mapValueType);
    }
}

Schema.FieldType Row() :
{
    final List<Schema.Field> fields;
}
{
    <ROW> fields = RowFields()
    {
        Schema rowSchema = Schema.builder().addFields(fields).build();
        return Schema.FieldType.row(rowSchema);
    }
}

List<Schema.Field> RowFields() :
{
    final List<Schema.Field> fields;
}
{
    (
        fields = FieldListParens()
    |
        fields = FieldListAngular()
    )
    {
        return fields;
    }
}

Schema.FieldType SimpleType() :
{
    final Span s = Span.of();
    final SqlTypeNameSpec simpleTypeName;
}
{
    simpleTypeName = SqlTypeName(s)
    {
        s.end(this);
        return CalciteUtils.toFieldType(simpleTypeName);
    }
}

SqlSetOptionBeam SqlSetOptionBeam(Span s, String scope) :
{
    SqlIdentifier name;
    final SqlNode val;
}
{
    (
        <SET> {
            s.add(this);
        }
        name = CompoundIdentifier()
        <EQ>
        (
            val = Literal()
        |
            val = SimpleIdentifier()
        |
            <ON> {
                // OFF is handled by SimpleIdentifier, ON handled here.
                val = new SqlIdentifier(token.image.toUpperCase(Locale.ROOT),
                    getPos());
            }
        )
        {
            return new SqlSetOptionBeam(s.end(val), scope, name, val);
        }
    |
        <RESET> {
            s.add(this);
        }
        (
            name = CompoundIdentifier()
        |
            <ALL> {
                name = new SqlIdentifier(token.image.toUpperCase(Locale.ROOT),
                    getPos());
            }
        )
        {
            return new SqlSetOptionBeam(s.end(name), scope, name, null);
        }
    )
}

// End parserImpls.ftl
