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


private void ColumnDef(List<ColumnDefinition> list) :
{
    SqlParserPos pos;
    SqlIdentifier name;
    SqlDataTypeSpec type;
    ColumnConstraint constraint = null;
    SqlNode comment = null;
}
{
    name = SimpleIdentifier() { pos = getPos(); }
    type = DataType()
    [
      <PRIMARY> <KEY>
      { constraint = new ColumnConstraint.PrimaryKey(getPos()); }
    ]
    [
      <COMMENT> comment = StringLiteral()
    ]
    {
        list.add(new ColumnDefinition(name, type, constraint, comment, pos));
    }
}

SqlNodeList ColumnDefinitionList() :
{
    SqlParserPos pos;
    List<ColumnDefinition> list = Lists.newArrayList();
}
{
    <LPAREN> { pos = getPos(); }
    ColumnDef(list)
    ( <COMMA> ColumnDef(list) )*
    <RPAREN> {
        return new SqlNodeList(list, pos.plus(getPos()));
    }
}

/**
 * CREATE TABLE ( IF NOT EXISTS )?
 *   ( database_name '.' )? table_name ( '(' column_def ( ',' column_def )* ')'
 *   ( STORED AS INPUTFORMAT input_format_classname OUTPUTFORMAT output_format_classname )?
 *   LOCATION location_uri
 *   ( TBLPROPERTIES tbl_properties )?
 *   ( AS select_stmt )
 */
SqlNode SqlCreateTable() :
{
    SqlParserPos pos;
    SqlIdentifier tblName;
    SqlNodeList fieldList;
    SqlNode type = null;
    SqlNode comment = null;
    SqlNode location = null;
    SqlNode tbl_properties = null;
    SqlNode select = null;
}
{
    <CREATE> { pos = getPos(); }
    <TABLE>
    tblName = CompoundIdentifier()
    fieldList = ColumnDefinitionList()
    <TYPE>
    type = StringLiteral()
    [
    <COMMENT>
    comment = StringLiteral()
    ]
    [
    <LOCATION>
    location = StringLiteral()
    ]
    [ <TBLPROPERTIES> tbl_properties = StringLiteral() ]
    [ <AS> select = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY) ] {
        return new SqlCreateTable(pos, tblName, fieldList, type, comment,
        location, tbl_properties, select);
    }
}

/**
 * DROP TABLE table_name
 */
SqlNode SqlDropTable() :
{
    SqlParserPos pos;
    SqlIdentifier tblName;
}
{
    <DROP> { pos = getPos(); }
    <TABLE>
    tblName = SimpleIdentifier() {
        return new SqlDropTable(pos, tblName);
    }
}

