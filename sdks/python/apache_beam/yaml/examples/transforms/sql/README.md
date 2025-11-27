# SQL Transform calcite_connection_properties Configuration Guide

This directory contains examples demonstrating how to use `calcite_connection_properties` in Beam YAML pipelines to configure SQL transforms for different database dialects and use cases.

## Overview

The `calcite_connection_properties` option in pipeline options allows you to configure Apache Calcite's SQL parser and function library to support database-specific SQL syntax and functions. This is particularly useful when you need to use SQL functions or syntax that are specific to certain databases like PostgreSQL, BigQuery, MySQL, or Oracle.

## Configuration Options

The most commonly used `calcite_connection_properties` include:

### Function Libraries (`fun`)
- `"standard"` - Standard SQL functions (default)
- `"postgresql"` - PostgreSQL-specific functions (e.g., SPLIT_PART, STRING_AGG)
- `"bigquery"` - BigQuery-specific functions (e.g., FORMAT_TIMESTAMP, ARRAY_TO_STRING)
- `"mysql"` - MySQL-specific functions (e.g., DATEDIFF, SUBSTRING_INDEX)
- `"oracle"` - Oracle-specific functions (e.g., NVL, SUBSTR)

### Lexical Rules (`lex`)
- `"standard"` - Standard SQL lexical rules (default)
- `"big_query"` - BigQuery lexical rules and syntax
- `"mysql"` - MySQL lexical rules
- `"oracle"` - Oracle lexical rules

### Other Properties
- `conformance` - SQL conformance level ("LENIENT", "STRICT", etc.)
- `caseSensitive` - Whether identifiers are case sensitive ("true"/"false")
- `quotedCasing` - How to handle quoted identifiers ("UNCHANGED", "TO_UPPER", "TO_LOWER")
- `unquotedCasing` - How to handle unquoted identifiers

## Usage Patterns

### Basic Configuration
```yaml
options:
  calcite_connection_properties:
    fun: "postgresql"
```

### Advanced Configuration
```yaml
options:
  calcite_connection_properties:
    fun: "bigquery"
    lex: "big_query"
    conformance: "LENIENT"
    caseSensitive: "false"
```

## Examples in this Directory

1. **sql_basic_example.yaml** - Basic SQL transform without special configuration
2. **sql_postgresql_functions.yaml** - Using PostgreSQL functions like SPLIT_PART
3. **sql_bigquery_functions.yaml** - BigQuery syntax and functions
4. **sql_mysql_functions.yaml** - MySQL-specific date and string functions
5. **sql_advanced_configuration.yaml** - Multiple configuration options

## Common Use Cases

### PostgreSQL Functions
Useful for string manipulation and array operations:
```yaml
options:
  calcite_connection_properties:
    fun: "postgresql"
```

### BigQuery Compatibility
For BigQuery-style syntax and functions:
```yaml
options:
  calcite_connection_properties:
    fun: "bigquery"
    lex: "big_query"
```

### Lenient SQL Parsing
For more flexible SQL parsing:
```yaml
options:
  calcite_connection_properties:
    conformance: "LENIENT"
```

## Important Notes

- These properties affect only the SQL parsing and function availability, not the actual data processing semantics
- Some database-specific functions may not be available depending on the Calcite version used
- Always test your SQL queries with the intended configuration before deploying to production
- The `calcite_connection_properties` must be specified in the pipeline `options` section, not in individual transform configurations

## Troubleshooting

If you encounter SQL parsing errors:

1. Check that the function you're using is supported by the specified function library
2. Verify that the lexical rules (`lex`) match your SQL syntax style
3. Try using `conformance: "LENIENT"` for more flexible parsing
4. Refer to the Apache Calcite documentation for supported functions in each dialect

For more information about Beam SQL and supported functions, see:
- [Beam SQL Documentation](https://beam.apache.org/documentation/dsls/sql/overview/)
- [Apache Calcite SQL Reference](https://calcite.apache.org/docs/reference.html)