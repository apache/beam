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

SqlNodeList ArgList() :
{
    SqlNodeList list = new SqlNodeList(getPos());
    SqlNode property;
}
{
    property = StringLiteral() { list.add(property); }
    (
        <COMMA> property = StringLiteral() { list.add(property); }
    )*
    {
        return list;
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


/**
 * ALTER CATALOG catalog_name
 *   [ SET (key1=val1, key2=val2, ...) ]
 *   [ (RESET | UNSET) (key1, key2, ...) ]
 */
SqlCall SqlAlterCatalog(Span s, String scope) :
{
    final SqlNode catalogName;
    SqlNodeList setProps = null;
    SqlNodeList resetProps = null;
}
{
    <ALTER> {
        s.add(this);
    }
    <CATALOG>
    (
        catalogName = CompoundIdentifier()
        |
        catalogName = StringLiteral()
    )
    [ <SET> <LPAREN> setProps = PropertyList() <RPAREN> ]
    [ (<RESET> | <UNSET>) <LPAREN> resetProps = ArgList() <RPAREN> ]

    {
        return new SqlAlterCatalog(
            s.end(this),
            scope,
            catalogName,
            setProps,
            resetProps);
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
 * SHOW CATALOGS [ LIKE regex_pattern ]
 */
SqlCall SqlShowCatalogs(Span s) :
{
    SqlNode regex = null;
}
{
    <SHOW> <CATALOGS> { s.add(this); }
    [ <LIKE> regex = StringLiteral() ]
    {
        List<String> path = new ArrayList<String>();
        path.add("beamsystem");
        path.add("catalogs");
        SqlNodeList selectList = SqlNodeList.of(SqlIdentifier.star(s.end(this)));
        SqlIdentifier from = new SqlIdentifier(path, s.end(this));
        SqlNode where = null;
        if (regex != null) {
            SqlIdentifier nameIdentifier = new SqlIdentifier("NAME", s.end(this));
            where = SqlStdOperatorTable.LIKE.createCall(
                s.end(this),
                nameIdentifier, regex);
        }

        return new SqlSelect(
            s.end(this),
            null,
            selectList,
            from,
            where,
            null,
            null,
            null,
            null,
            null,
            null,
            null);
    }
}


/**
 * CREATE DATABASE ( IF NOT EXISTS )? ( catalog_name '.' )? database_name
 */
SqlCreate SqlCreateDatabase(Span s, boolean replace) :
{
    final boolean ifNotExists;
    final SqlIdentifier databaseName;
}
{
    <DATABASE> {
        s.add(this);
    }

    ifNotExists = IfNotExistsOpt()
    databaseName = CompoundIdentifier()

    {
        return new SqlCreateDatabase(
            s.end(this),
            replace,
            ifNotExists,
            databaseName);
    }
}

/**
 * USE [ DATABASE ] ( catalog_name '.' )? database_name
 */
SqlCall SqlUseDatabase(Span s, String scope) :
{
    final SqlIdentifier databaseName;
}
{
    <USE> {
        s.add(this);
    }
    [ <DATABASE> ]
    databaseName = CompoundIdentifier()
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
    final SqlIdentifier databaseName;
    final boolean cascade;
}
{
    <DATABASE>
    ifExists = IfExistsOpt()
    databaseName = CompoundIdentifier()

    cascade = CascadeOpt()

    {
        return new SqlDropDatabase(s.end(this), ifExists, databaseName, cascade);
    }
}

/**
 * SHOW DATABASES [ ( FROM | IN )? catalog_name ] [LIKE regex_pattern ]
 */
SqlCall SqlShowDatabases(Span s) :
{
    SqlIdentifier catalogName = null;
    SqlNode regex = null;
}
{
    <SHOW> <DATABASES> { s.add(this); }
    [ ( <FROM> | <IN> ) catalogName = SimpleIdentifier() ]
    [ <LIKE> regex = StringLiteral() ]
    {
        List<String> path = new ArrayList<String>();
        path.add("beamsystem");
        path.add("databases");
        SqlNodeList selectList = SqlNodeList.of(SqlIdentifier.star(s.end(this)));
        SqlNode where = null;
        if (regex != null) {
            SqlIdentifier nameIdentifier = new SqlIdentifier("NAME", s.end(this));
            where = SqlStdOperatorTable.LIKE.createCall(
                s.end(this),
                nameIdentifier, regex);
        }
        if (catalogName != null) {
            path.add(catalogName.getSimple());
        } else {
            path.add("__current_catalog__");
        }
        SqlIdentifier from = new SqlIdentifier(path, s.end(this));

        return new SqlSelect(
            s.end(this),
            null,
            selectList,
            from,
            where,
            null,
            null,
            null,
            null,
            null,
            null,
            null);
    }
}

/**
 * SHOW CURRENT ( CATALOG | DATABASE )
 */
SqlCall SqlShowCurrent(Span s) :
{
}
{
    <SHOW> <CURRENT> { s.add(this); }
    {
        List<String> path = new ArrayList<String>();
        path.add("beamsystem");
    }
    (
        <CATALOG> {
            path.add("__current_catalog__");
        }
    |
        <DATABASE> {
            path.add("__current_database__");
        }
    )
    {
        if (path.size() != 2) {
            throw new ParseException(
                "Expected SHOW CURRENT CATALOG or SHOW CURRENT DATABASE");
        }
        SqlNodeList selectList = SqlNodeList.of(SqlIdentifier.star(s.end(this)));
        SqlIdentifier from = new SqlIdentifier(path, s.end(this));

        return new SqlSelect(
            s.end(this),
            null,
            selectList,
            from,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null);
    }
}

SqlNodeList PartitionFieldsParens() :
{
    final SqlNodeList partitions;
}
{
    <LPAREN>
    partitions = PartitionFieldList()
    <RPAREN>
    {
        return partitions;
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
 *   ( catalog_name '.' )? ( database_name '.' )? table_name '(' column_def ( ',' column_def )* ')'
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
    [ <PARTITIONED> <BY> partitionFields = PartitionFieldsParens() ]
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

/**
 * Loosely following Flink's grammar: https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sql/alter/#alter-table
 * ALTER TABLE table_name
 *   [ ADD COLUMNS <column_def, column_def, ...> ]
 *   [ DROP COLUMNS <column_name, column_name> ]
 *   [ ADD PARTITIONS <partition_field, partition_field, ...> ]
 *   [ DROP PARTITIONS <partition_field, partition_field, ...> ]
 *   [ SET (key1=val1, key2=val2, ...) ]
 *   [ (RESET | UNSET) (key1, key2, ...) ]
 */
SqlCall SqlAlterTable(Span s, String scope) :
{
    final SqlNode tableName;
    SqlNodeList columnsToDrop = null;
    List<Schema.Field> columnsToAdd = null;
    SqlNodeList partitionsToDrop = null;
    SqlNodeList partitionsToAdd = null;
    SqlNodeList setProps = null;
    SqlNodeList resetProps = null;
}
{
    <ALTER> {
        s.add(this);
    }
    <TABLE>
    tableName = CompoundIdentifier()

    [ <DROP> (
      <COLUMNS> columnsToDrop = ParenthesizedSimpleIdentifierList()
        |
      <PARTITIONS> partitionsToDrop = ParenthesizedLiteralOptionCommaList()
    ) ]

    [ <ADD> (
      <COLUMNS> columnsToAdd = FieldListParens()
        |
      <PARTITIONS> partitionsToAdd = ParenthesizedLiteralOptionCommaList()
    ) ]

    [ (<RESET> | <UNSET>) <LPAREN> resetProps = ArgList() <RPAREN> ]
    [ <SET> <LPAREN> setProps = PropertyList() <RPAREN> ]

    {
        return new SqlAlterTable(
            s.end(this),
            scope,
            tableName,
            columnsToAdd,
            columnsToDrop,
            partitionsToAdd,
            partitionsToDrop,
            setProps,
            resetProps);
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

/**
 * SHOW TABLES [ ( FROM | IN )? [ catalog_name '.' ] database_name ] [ LIKE regex_pattern ]
 */
SqlCall SqlShowTables(Span s) :
{
    SqlIdentifier databaseCatalog = null;
    SqlNode regex = null;
}
{
    <SHOW> <TABLES> { s.add(this); }
    [ (<FROM> | <IN>) databaseCatalog = CompoundIdentifier() ]
    [ <LIKE> regex = StringLiteral() ]
    {
        List<String> path = new ArrayList<String>();
        path.add("beamsystem");
        path.add("tables");
        SqlNodeList selectList = SqlNodeList.of(SqlIdentifier.star(s.end(this)));
        SqlNode where = null;
        if (regex != null) {
            SqlIdentifier nameIdentifier = new SqlIdentifier("NAME", s.end(this));
            where = SqlStdOperatorTable.LIKE.createCall(
                s.end(this),
                nameIdentifier, regex);
        }
        if (databaseCatalog != null) {
            List<String> components = databaseCatalog.names;
            if (components.size() == 1) {
                path.add("__current_catalog__");
                path.add(components.get(0));
            } else if (components.size() == 2) {
                path.addAll(components);
            } else {
                throw new ParseException(
                    "SHOW TABLES FROM/IN accepts at most a catalog name and a database name.");
            }
        } else {
            path.add("__current_catalog__");
            path.add("__current_database__");
        }
        SqlIdentifier from = new SqlIdentifier(path, s.end(this));

        return new SqlSelect(
            s.end(this),
            null,
            selectList,
            from,
            where,
            null,
            null,
            null,
            null,
            null,
            null,
            null);
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

/*
 * ===========================================================================================
 * Spark/PostgreSQL infix cast:  expr :: TYPE
 * ===========================================================================================
 *
 * LAYER A (clean, self-contained): the operator hookup. Wired in via the `extraBinaryExpressions`
 * FreeMarker hook, which the vendored Parser.jj invokes inside Expression2's operator loop as
 * `InfixCast(list, exprContext, s)` (Parser.jj:3827-3830). We push the vendored
 * SqlLibraryOperators.INFIX_CAST operator (a SqlBinaryOperator named "::", kind CAST, prec 94,
 * left-assoc) plus the RHS type spec onto the flat precedence list; SqlParserUtil.toTree reduces it
 * to INFIX_CAST(expr, typeSpec), which is exactly how CAST(expr AS type) is represented internally.
 * Precedence 94 binds tighter than arithmetic/comparison, so 1+2::int = 1+(2::int) and a::int<5 =
 * (a::int)<5; chaining x::int::string = (x::int)::string falls out for free.
 *
 * LAYER B (SparkDataType & friends below): the Spark-aware RHS type grammar. This re-expresses
 * SparkSqlPreprocessor.translateSparkType in JavaCC. *** SHARED BLAST RADIUS *** with
 * rewriteAngleBracketTypes (owned by a different agent): both need the same array<>/map<>/struct<>
 * + scalar-alias grammar. This copy is the merge seed; reconcile into ONE shared production before
 * deleting either regex. Do NOT delete translateSparkType (still used by the angle-bracket rewrite).
 */
void InfixCast(List<Object> list, ExprContext exprContext, Span s) :
{
    final SqlDataTypeSpec dt;
}
{
    <DOUBLE_COLON> {
        checkNonQueryExpression(exprContext);
    }
    dt = SparkDataType() {
        list.add(
            new SqlParserUtil.ToTreeListItem(
                SqlLibraryOperators.INFIX_CAST, getPos()));
        list.add(dt);
    }
}

/**
 * Spark-aware data type for the RHS of "::". Tries Spark-only spellings first, then falls back to
 * core DataType()'s TypeName(); keeps the SQL-standard postfix collection loop (e.g. INT ARRAY).
 */
SqlDataTypeSpec SparkDataType() :
{
    SqlTypeNameSpec tn;
    final Span s = Span.of();
}
{
    tn = SparkTypeName(s)
    (
        tn = CollectionsTypeName(tn)
    )*
    {
        return new SqlDataTypeSpec(tn, s.add(tn.getParserPos()).pos());
    }
}

SqlTypeNameSpec SparkTypeName(Span s) :
{
    SqlTypeNameSpec t;
    final SqlDataTypeSpec elem;
    final SqlDataTypeSpec key;
    final SqlDataTypeSpec val;
    final SqlTypeName scalar;
}
{
    (
        // Spark angle-bracket ARRAY (core ARRAY is postfix). ARRAY is a case-insensitive keyword
        // token; nested array<array<int>> closes via two GT tokens (no ">>" shift token exists).
        <ARRAY> <LT> elem = SparkDataType() <GT> {
            t = new SqlCollectionTypeNameSpec(
                    elem.getTypeNameSpec(), SqlTypeName.ARRAY, getPos());
        }
    |
        // Spark MAP<K,V> with Spark-aware inner types (core MapTypeName recurses into core
        // DataType, which rejects e.g. map<string,int>). LA(2) so a bare MAP without '<' falls
        // through to the core handler.
        LOOKAHEAD(2)
        <MAP> <LT> key = SparkDataType() <COMMA> val = SparkDataType() <GT> {
            t = new SqlMapTypeNameSpec(key, val, getPos());
        }
    |
        // Spark STRUCT<f:T, ...>. STRUCT is NOT a keyword token (it lexes as IDENTIFIER), so guard
        // by image + a following '<'.
        LOOKAHEAD({ getToken(1).kind == IDENTIFIER
                    && "STRUCT".equalsIgnoreCase(getToken(1).image)
                    && getToken(2).kind == LT })
        t = SparkStructType()
    |
        // Spark scalar aliases core DataType() rejects (string/long/short/byte/bool/
        // timestamp_ltz/timestamp_ntz), matched by IDENTIFIER image. The check is inlined as a plain
        // boolean (not a JAVACODE helper) so it can run inside the generated jj_3R_* lookahead
        // methods, which are not declared to throw ParseException.
        LOOKAHEAD({ getToken(1).kind == IDENTIFIER && getToken(1).image != null
                    && (getToken(1).image.equalsIgnoreCase("STRING")
                        || getToken(1).image.equalsIgnoreCase("LONG")
                        || getToken(1).image.equalsIgnoreCase("SHORT")
                        || getToken(1).image.equalsIgnoreCase("BYTE")
                        || getToken(1).image.equalsIgnoreCase("BOOL")
                        || getToken(1).image.equalsIgnoreCase("TIMESTAMP_LTZ")
                        || getToken(1).image.equalsIgnoreCase("TIMESTAMP_NTZ")) })
        scalar = SparkScalarAlias(s) {
            t = new SqlBasicTypeNameSpec(scalar, s.end(this));
        }
    |
        // Everything core already handles: int, bigint, decimal(p,s), varchar, char, date,
        // timestamp, real, binary, MAP<>, ROW(), user-defined type, etc.
        t = TypeName()
    )
    {
        return t;
    }
}

SqlTypeNameSpec SparkStructType() :
{
    final List<SqlIdentifier> names = new ArrayList<SqlIdentifier>();
    final List<SqlDataTypeSpec> types = new ArrayList<SqlDataTypeSpec>();
    SqlIdentifier n;
    SqlDataTypeSpec ft;
}
{
    // "STRUCT" (guaranteed IDENTIFIER by the LOOKAHEAD at the call site).
    <IDENTIFIER>
    <LT>
        n = SimpleIdentifier() <COLON> ft = SparkDataType() {
            names.add(n);
            types.add(ft);
        }
        (
            <COMMA> n = SimpleIdentifier() <COLON> ft = SparkDataType() {
                names.add(n);
                types.add(ft);
            }
        )*
    <GT>
    {
        return new SqlRowTypeNameSpec(getPos(), names, types);
    }
}

SqlTypeName SparkScalarAlias(Span s) :
{
    final SqlTypeName tn;
}
{
    // Only reached when isSparkScalarAlias(getToken(1)) already matched, so the switch is total.
    <IDENTIFIER> {
        s.add(this);
        switch (token.image.toUpperCase(Locale.ROOT)) {
            case "STRING":        tn = SqlTypeName.VARCHAR;   break;
            case "LONG":          tn = SqlTypeName.BIGINT;    break;
            case "SHORT":         tn = SqlTypeName.SMALLINT;  break;
            case "BYTE":          tn = SqlTypeName.TINYINT;   break;
            case "BOOL":          tn = SqlTypeName.BOOLEAN;   break;
            case "TIMESTAMP_LTZ":
            case "TIMESTAMP_NTZ": tn = SqlTypeName.TIMESTAMP; break;
            default:
                throw new ParseException("not a Spark scalar type alias: " + token.image);
        }
        return tn;
    }
}

/**
 * Spark numeric type-constructor functions float(x) / double(x).
 *
 * <p>FLOAT and DOUBLE are RESERVED keyword tokens, so the lexer emits <FLOAT>/<DOUBLE> (never
 * IDENTIFIER) and the call can never reach NamedFunctionCall / function resolution -- registering
 * them as scalar UDFs is therefore impossible (the route DATE()/TIMESTAMP() take). A dedicated
 * builtin-function production is the only native option. We emit a standard CAST(expr AS FLOAT/
 * DOUBLE), which RexImpTable lowers natively. Strictly better than the regex it retires, whose
 * non-nesting [^)]+? group broke on nested parens (float(abs(x))).
 *
 * <p>Hooked in via the `builtinFunctionCallMethods` FreeMarker list, so this is one alternative of
 * the generated BuiltinFunctionCall(); <FLOAT>/<DOUBLE> are unique first tokens there, so there is
 * no choice conflict.
 */
SqlNode SparkTypeConstructorCall() :
{
    final Span s;
    final SqlNode e;
    final SqlTypeName tn;
}
{
    ( <FLOAT> { tn = SqlTypeName.FLOAT; } | <DOUBLE> { tn = SqlTypeName.DOUBLE; } )
    { s = span(); }
    <LPAREN> e = Expression(ExprContext.ACCEPT_SUB_QUERY) <RPAREN>
    {
        return SqlStdOperatorTable.CAST.createCall(
            s.end(this),
            e,
            new SqlDataTypeSpec(new SqlBasicTypeNameSpec(tn, getPos()), getPos()));
    }
}

/**
 * Spark angle-bracket collection / struct types in ANY DataType position: array<E>, map<K,V>,
 * struct<f:T,..>. Returns SqlTypeNameSpec so it can be wired into the core TypeName() choice via the
 * dataTypeParserMethods hook (config.fmpp), making these spellings parse in CAST(x AS array<int>)
 * etc. -- not only on the '::' RHS (where SparkTypeName already accepts them). Inner element / key /
 * value / field types use SparkDataType() so Spark scalar spellings resolve inside the brackets
 * (array<string>, map<string,int>).
 *
 * <p>Each branch is decidable in <=2 tokens (ARRAY / MAP keyword, or IDENTIFIER-then-'<' for struct),
 * so under the template's LOOKAHEAD(2) wrapper a bare core type (a keyword token) or a user-defined
 * type identifier (IDENTIFIER not followed by '<') never enters here and falls through to core
 * SqlTypeName() / CompoundIdentifier(). No semantic guard on the struct branch: struct is the only
 * IDENTIFIER-initial alternative, so IDENTIFIER-'<' in a type position is unambiguously a struct.
 */
SqlTypeNameSpec SparkAngleType() :
{
    SqlTypeNameSpec t;
    final SqlDataTypeSpec elem;
    final SqlDataTypeSpec key;
    final SqlDataTypeSpec val;
    final SqlTypeName scalar;
    final Span s = Span.of();
}
{
    (
        <ARRAY> <LT> elem = SparkDataType() <GT> {
            t = new SqlCollectionTypeNameSpec(
                    elem.getTypeNameSpec(), SqlTypeName.ARRAY, getPos());
        }
    |
        <MAP> <LT> key = SparkDataType() <COMMA> val = SparkDataType() <GT> {
            t = new SqlMapTypeNameSpec(key, val, getPos());
        }
    |
        // Spark scalar aliases (string/long/short/byte/bool/timestamp_ltz/timestamp_ntz) in ANY
        // DataType position -- e.g. CAST(x AS STRING) -- not only on the '::' RHS (where
        // SparkTypeName already accepts them). SparkScalarAlias maps STRING->VARCHAR, LONG->BIGINT,
        // etc. (line ~1041). This is the native, nested-paren-safe replacement for the regex
        // CAST(.. AS STRING)->CAST(.. AS VARCHAR) rewrite that lived in SparkSqlPreprocessor.
        // Guarded by an inlined boolean LOOKAHEAD (mirroring SparkTypeName's branch) so it runs
        // inside the generated jj_3R_* lookahead methods, which are not declared to throw
        // ParseException; the wrapping core TypeName() LOOKAHEAD(2) only commits here when token 1
        // is one of these aliases (an IDENTIFIER, never a keyword core type).
        LOOKAHEAD({ getToken(1).kind == IDENTIFIER && getToken(1).image != null
                    && (getToken(1).image.equalsIgnoreCase("STRING")
                        || getToken(1).image.equalsIgnoreCase("LONG")
                        || getToken(1).image.equalsIgnoreCase("SHORT")
                        || getToken(1).image.equalsIgnoreCase("BYTE")
                        || getToken(1).image.equalsIgnoreCase("BOOL")
                        || getToken(1).image.equalsIgnoreCase("TIMESTAMP_LTZ")
                        || getToken(1).image.equalsIgnoreCase("TIMESTAMP_NTZ")) })
        scalar = SparkScalarAlias(s) {
            t = new SqlBasicTypeNameSpec(scalar, s.end(this));
        }
    |
        t = SparkStructType()
    )
    {
        return t;
    }
}

// End parserImpls.ftl
