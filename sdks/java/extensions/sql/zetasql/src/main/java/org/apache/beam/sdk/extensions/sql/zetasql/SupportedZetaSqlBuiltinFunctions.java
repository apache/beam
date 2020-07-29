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
package org.apache.beam.sdk.extensions.sql.zetasql;

import com.google.zetasql.ZetaSQLFunction.FunctionSignatureId;
import java.util.List;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

/**
 * List of ZetaSQL builtin functions supported by Beam ZetaSQL. Keep this list in sync with
 * https://github.com/google/zetasql/blob/master/zetasql/public/builtin_function.proto. Uncomment
 * the corresponding entries to enable parser support to the operators/functions.
 *
 * <p>Last synced ZetaSQL release: 2020.06.01
 */
class SupportedZetaSqlBuiltinFunctions {
  static final List<FunctionSignatureId> ALLOWLIST =
      ImmutableList.of(
          FunctionSignatureId.FN_ADD_DOUBLE, // $add
          FunctionSignatureId.FN_ADD_INT64, // $add
          FunctionSignatureId.FN_ADD_NUMERIC, // $add
          // FunctionSignatureId.FN_ADD_BIGNUMERIC, // $add
          FunctionSignatureId.FN_AND, // $and
          FunctionSignatureId.FN_CASE_NO_VALUE, // $case_no_value
          FunctionSignatureId.FN_CASE_WITH_VALUE, // $case_with_value
          FunctionSignatureId.FN_DIVIDE_DOUBLE, // $divide
          FunctionSignatureId.FN_DIVIDE_NUMERIC, // $divide
          // FunctionSignatureId.FN_DIVIDE_BIGNUMERIC, // $divide
          FunctionSignatureId.FN_GREATER, // $greater
          FunctionSignatureId.FN_GREATER_OR_EQUAL, // $greater_or_equal
          FunctionSignatureId.FN_LESS, // $less
          FunctionSignatureId.FN_LESS_OR_EQUAL, // $less_or_equal
          FunctionSignatureId.FN_EQUAL, // $equal
          FunctionSignatureId.FN_STRING_LIKE, // $like
          FunctionSignatureId.FN_BYTE_LIKE, // $like
          // FunctionSignatureId.FN_IN, // $in
          // FunctionSignatureId.FN_IN_ARRAY, // $in_array
          // FunctionSignatureId.FN_BETWEEN, // $between
          FunctionSignatureId.FN_IS_NULL, // $is_null
          FunctionSignatureId.FN_IS_TRUE, // $is_true
          FunctionSignatureId.FN_IS_FALSE, // $is_false
          FunctionSignatureId.FN_MULTIPLY_DOUBLE, // $multiply
          FunctionSignatureId.FN_MULTIPLY_INT64, // $multiply
          FunctionSignatureId.FN_MULTIPLY_NUMERIC, // $multiply
          // FunctionSignatureId.FN_MULTIPLY_BIGNUMERIC, // $multiply
          FunctionSignatureId.FN_NOT, // $not
          FunctionSignatureId.FN_NOT_EQUAL, // $not_equal
          FunctionSignatureId.FN_OR, // $or
          FunctionSignatureId.FN_SUBTRACT_DOUBLE, // $subtract
          FunctionSignatureId.FN_SUBTRACT_INT64, // $subtract
          FunctionSignatureId.FN_SUBTRACT_NUMERIC, // $subtract
          // FunctionSignatureId.FN_SUBTRACT_BIGNUMERIC, // $subtract
          FunctionSignatureId.FN_UNARY_MINUS_INT64, // $unary_minus
          FunctionSignatureId.FN_UNARY_MINUS_DOUBLE, // $unary_minus
          FunctionSignatureId.FN_UNARY_MINUS_NUMERIC, // $unary_minus
          // FunctionSignatureId.FN_UNARY_MINUS_BIGNUMERIC, // $unary_minus

          // Bitwise unary operators.
          // FunctionSignatureId.FN_BITWISE_NOT_INT64, // $bitwise_not
          // FunctionSignatureId.FN_BITWISE_NOT_BYTES, // $bitwise_not
          // Bitwise binary operators.
          // FunctionSignatureId.FN_BITWISE_OR_INT64, // $bitwise_or
          // FunctionSignatureId.FN_BITWISE_OR_BYTES, // $bitwise_or
          // FunctionSignatureId.FN_BITWISE_XOR_INT64, // $bitwise_xor
          // FunctionSignatureId.FN_BITWISE_XOR_BYTES, // $bitwise_xor
          // FunctionSignatureId.FN_BITWISE_AND_INT64, // $bitwise_and
          // FunctionSignatureId.FN_BITWISE_AND_BYTES, // $bitwise_and
          // FunctionSignatureId.FN_BITWISE_LEFT_SHIFT_INT64, // $bitwise_left_shift
          // FunctionSignatureId.FN_BITWISE_LEFT_SHIFT_BYTES, // $bitwise_left_shift
          // FunctionSignatureId.FN_BITWISE_RIGHT_SHIFT_INT64, // $bitwise_right_shift
          // FunctionSignatureId.FN_BITWISE_RIGHT_SHIFT_BYTES, // $bitwise_right_shift

          // BIT_COUNT functions.
          // FunctionSignatureId.FN_BIT_COUNT_INT64, // bit_count(int64) -> int64
          // FunctionSignatureId.FN_BIT_COUNT_BYTES, // bit_count(bytes) -> int64

          // FunctionSignatureId.FN_ERROR,// error(string) -> {unused result, coercible to any type}

          FunctionSignatureId.FN_COUNT_STAR, // $count_star

          //
          // The following functions use standard function call syntax.
          //

          // String functions
          FunctionSignatureId.FN_CONCAT_STRING, // concat(repeated string) -> string
          // FunctionSignatureId.FN_CONCAT_BYTES, // concat(repeated bytes) -> bytes
          // FunctionSignatureId.FN_CONCAT_OP_STRING, // concat(string, string) -> string
          // FunctionSignatureId.FN_CONCAT_OP_BYTES, // concat(bytes, bytes) -> bytes
          // FunctionSignatureId.FN_STRPOS_STRING, // strpos(string, string) -> int64
          // FunctionSignatureId.FN_STRPOS_BYTES, // strpos(bytes, bytes) -> int64

          // FunctionSignatureId.FN_INSTR_STRING,// instr(string, string[, int64[, int64]]) -> int64
          // FunctionSignatureId.FN_INSTR_BYTES, // instr(bytes, bytes[, int64[, int64]]) -> int64
          // FunctionSignatureId.FN_LOWER_STRING, // lower(string) -> string
          // FunctionSignatureId.FN_LOWER_BYTES, // lower(bytes) -> bytes
          // FunctionSignatureId.FN_UPPER_STRING, // upper(string) -> string
          // FunctionSignatureId.FN_UPPER_BYTES, // upper(bytes) -> bytes
          // FunctionSignatureId.FN_LENGTH_STRING, // length(string) -> int64
          // FunctionSignatureId.FN_LENGTH_BYTES, // length(bytes) -> int64
          FunctionSignatureId.FN_STARTS_WITH_STRING, // starts_with(string, string) -> string
          // FunctionSignatureId.FN_STARTS_WITH_BYTES, // starts_with(bytes, bytes) -> bytes
          FunctionSignatureId.FN_ENDS_WITH_STRING, // ends_with(string, string) -> string
          // FunctionSignatureId.FN_ENDS_WITH_BYTES, // ends_with(bytes, bytes) -> bytes
          FunctionSignatureId.FN_SUBSTR_STRING, // substr(string, int64[, int64]) -> string
          // FunctionSignatureId.FN_SUBSTR_BYTES, // substr(bytes, int64[, int64]) -> bytes
          FunctionSignatureId.FN_TRIM_STRING, // trim(string[, string]) -> string
          // FunctionSignatureId.FN_TRIM_BYTES, // trim(bytes, bytes) -> bytes
          FunctionSignatureId.FN_LTRIM_STRING, // ltrim(string[, string]) -> string
          // FunctionSignatureId.FN_LTRIM_BYTES, // ltrim(bytes, bytes) -> bytes
          FunctionSignatureId.FN_RTRIM_STRING, // rtrim(string[, string]) -> string
          // FunctionSignatureId.FN_RTRIM_BYTES, // rtrim(bytes, bytes) -> bytes
          FunctionSignatureId.FN_REPLACE_STRING, // replace(string, string, string) -> string
          // FunctionSignatureId.FN_REPLACE_BYTES, // replace(bytes, bytes, bytes) -> bytes
          // FunctionSignatureId.FN_REGEXP_MATCH_STRING, // regexp_match(string, string) -> bool
          // FunctionSignatureId.FN_REGEXP_MATCH_BYTES, // regexp_match(bytes, bytes) -> bool
          // FunctionSignatureId.FN_REGEXP_EXTRACT_STRING,//regexp_extract(string, string) -> string
          // FunctionSignatureId.FN_REGEXP_EXTRACT_BYTES, // regexp_extract(bytes, bytes) -> bytes
          // FunctionSignatureId.FN_REGEXP_REPLACE_STRING,
          // regexp_replace(string, string, string) -> string
          // FunctionSignatureId.FN_REGEXP_REPLACE_BYTES,
          // regexp_replace(bytes, bytes, bytes) -> bytes
          // FunctionSignatureId.FN_REGEXP_EXTRACT_ALL_STRING,
          // regexp_extract_all(string, string) -> array of string
          // FunctionSignatureId.FN_REGEXP_EXTRACT_ALL_BYTES,
          // regexp_extract_all(bytes, bytes) -> array of bytes
          // FunctionSignatureId.FN_BYTE_LENGTH_STRING, // byte_length(string) -> int64
          // FunctionSignatureId.FN_BYTE_LENGTH_BYTES, // byte_length(bytes) -> int64
          // semantically identical to FN_LENGTH_BYTES
          FunctionSignatureId.FN_CHAR_LENGTH_STRING, // char_length(string) -> int64
          // semantically identical to FN_LENGTH_STRING
          // FunctionSignatureId.FN_SPLIT_STRING, // split(string, string) -> array of string
          // FunctionSignatureId.FN_SPLIT_BYTES, // split(bytes, bytes) -> array of bytes
          // FunctionSignatureId.FN_REGEXP_CONTAINS_STRING,//regexp_contains(string, string) -> bool
          // FunctionSignatureId.FN_REGEXP_CONTAINS_BYTES, // regexp_contains(bytes, bytes) -> bool
          // Converts bytes to string by replacing invalid UTF-8 characters with
          // replacement char U+FFFD.
          // FunctionSignatureId.FN_SAFE_CONVERT_BYTES_TO_STRING,
          // Unicode normalization and casefolding functions.
          // FunctionSignatureId.FN_NORMALIZE_STRING, // normalize(string [, mode]) -> string
          // normalize_and_casefold(string [, mode]) -> string
          // FunctionSignatureId.FN_NORMALIZE_AND_CASEFOLD_STRING,
          // FunctionSignatureId.FN_TO_BASE64, // to_base64(bytes) -> string
          // FunctionSignatureId.FN_FROM_BASE64, // from_base64(string) -> bytes
          // FunctionSignatureId.FN_TO_HEX, // to_hex(bytes) -> string
          // FunctionSignatureId.FN_FROM_HEX, // from_hex(string) -> bytes
          // FunctionSignatureId.FN_TO_BASE32, // to_base32(bytes) -> string
          // FunctionSignatureId.FN_FROM_BASE32, // from_base32(string) -> bytes
          // to_code_points(string) -> array<int64>
          // FunctionSignatureId.FN_TO_CODE_POINTS_STRING,
          // to_code_points(bytes) -> array<int64>
          // FunctionSignatureId.FN_TO_CODE_POINTS_BYTES,
          // code_points_to_string(array<int64>) -> string
          // FunctionSignatureId.FN_CODE_POINTS_TO_STRING,
          // code_points_to_bytes(array<int64>) -> bytes
          // FunctionSignatureId.FN_CODE_POINTS_TO_BYTES,
          // FunctionSignatureId.FN_LPAD_BYTES, // lpad(bytes, int64[, bytes]) -> bytes
          // FunctionSignatureId.FN_LPAD_STRING, // lpad(string, int64[, string]) -> string
          // FunctionSignatureId.FN_RPAD_BYTES, // rpad(bytes, int64[, bytes]) -> bytes
          // FunctionSignatureId.FN_RPAD_STRING, // rpad(string, int64[, string]) -> string
          // FunctionSignatureId.FN_LEFT_STRING, // left(string, int64) -> string
          // FunctionSignatureId.FN_LEFT_BYTES, // left(bytes, int64) -> bytes
          // FunctionSignatureId.FN_RIGHT_STRING, // right(string, int64) -> string
          // FunctionSignatureId.FN_RIGHT_BYTES, // right(bytes, int64) -> bytes
          // FunctionSignatureId.FN_REPEAT_BYTES, // repeat(bytes, int64) -> bytes
          // FunctionSignatureId.FN_REPEAT_STRING, // repeat(string, int64) -> string
          FunctionSignatureId.FN_REVERSE_STRING, // reverse(string) -> string
          // FunctionSignatureId.FN_REVERSE_BYTES, // reverse(bytes) -> bytes
          // FunctionSignatureId.FN_SOUNDEX_STRING, // soundex(string) -> string
          // FunctionSignatureId.FN_ASCII_STRING, // ASCII(string) -> int64
          // FunctionSignatureId.FN_ASCII_BYTES, // ASCII(bytes) -> int64
          // FunctionSignatureId.FN_TRANSLATE_STRING, // translate(string, string, string) -> string
          // FunctionSignatureId.FN_TRANSLATE_BYTES, // soundex(bytes, bytes, bytes) -> bytes
          // FunctionSignatureId.FN_INITCAP_STRING, // initcap(string[, string]) -> string

          // Control flow functions
          FunctionSignatureId.FN_IF, // if
          // Coalesce is used to express the output join column in FULL JOIN.
          FunctionSignatureId.FN_COALESCE, // coalesce
          FunctionSignatureId.FN_IFNULL, // ifnull
          FunctionSignatureId.FN_NULLIF, // nullif

          // Time functions
          FunctionSignatureId.FN_CURRENT_DATE, // current_date
          FunctionSignatureId.FN_CURRENT_DATETIME, // current_datetime
          FunctionSignatureId.FN_CURRENT_TIME, // current_time
          FunctionSignatureId.FN_CURRENT_TIMESTAMP, // current_timestamp
          FunctionSignatureId.FN_DATE_ADD_DATE, // date_add
          FunctionSignatureId.FN_DATETIME_ADD, // datetime_add
          FunctionSignatureId.FN_TIME_ADD, // time_add
          FunctionSignatureId.FN_TIMESTAMP_ADD, // timestamp_add
          FunctionSignatureId.FN_DATE_DIFF_DATE, // date_diff
          FunctionSignatureId.FN_DATETIME_DIFF, // datetime_diff
          FunctionSignatureId.FN_TIME_DIFF, // time_diff
          FunctionSignatureId.FN_TIMESTAMP_DIFF, // timestamp_diff
          FunctionSignatureId.FN_DATE_SUB_DATE, // date_sub
          FunctionSignatureId.FN_DATETIME_SUB, // datetime_sub
          FunctionSignatureId.FN_TIME_SUB, // time_sub
          FunctionSignatureId.FN_TIMESTAMP_SUB, // timestamp_sub
          FunctionSignatureId.FN_DATE_TRUNC_DATE, // date_trunc
          FunctionSignatureId.FN_DATETIME_TRUNC, // datetime_trunc
          FunctionSignatureId.FN_TIME_TRUNC, // time_trunc
          FunctionSignatureId.FN_TIMESTAMP_TRUNC, // timestamp_trunc
          FunctionSignatureId.FN_DATE_FROM_UNIX_DATE, // date_from_unix_date
          FunctionSignatureId.FN_TIMESTAMP_FROM_INT64_SECONDS, // timestamp_seconds
          FunctionSignatureId.FN_TIMESTAMP_FROM_INT64_MILLIS, // timestamp_millis
          // FunctionSignatureId.FN_TIMESTAMP_FROM_INT64_MICROS, // timestamp_micros
          FunctionSignatureId.FN_TIMESTAMP_FROM_UNIX_SECONDS_INT64, // timestamp_from_unix_seconds
          // timestamp_from_unix_seconds
          // FunctionSignatureId.FN_TIMESTAMP_FROM_UNIX_SECONDS_TIMESTAMP,
          FunctionSignatureId.FN_TIMESTAMP_FROM_UNIX_MILLIS_INT64, // timestamp_from_unix_millis
          // timestamp_from_unix_millis
          // FunctionSignatureId.FN_TIMESTAMP_FROM_UNIX_MILLIS_TIMESTAMP,
          // FunctionSignatureId.FN_TIMESTAMP_FROM_UNIX_MICROS_INT64, // timestamp_from_unix_micros
          // timestamp_from_unix_micros
          // FunctionSignatureId.FN_TIMESTAMP_FROM_UNIX_MICROS_TIMESTAMP,
          FunctionSignatureId.FN_UNIX_DATE, // unix_date
          FunctionSignatureId.FN_UNIX_SECONDS_FROM_TIMESTAMP,
          FunctionSignatureId.FN_UNIX_MILLIS_FROM_TIMESTAMP,
          // FunctionSignatureId.FN_UNIX_MICROS_FROM_TIMESTAMP,
          FunctionSignatureId.FN_DATE_FROM_TIMESTAMP, // date
          FunctionSignatureId.FN_DATE_FROM_DATETIME, // date
          FunctionSignatureId.FN_DATE_FROM_YEAR_MONTH_DAY, // date
          FunctionSignatureId.FN_TIMESTAMP_FROM_STRING, // timestamp
          FunctionSignatureId.FN_TIMESTAMP_FROM_DATE, // timestamp
          FunctionSignatureId.FN_TIMESTAMP_FROM_DATETIME, // timestamp
          FunctionSignatureId.FN_TIME_FROM_HOUR_MINUTE_SECOND, // time
          FunctionSignatureId.FN_TIME_FROM_TIMESTAMP, // time
          FunctionSignatureId.FN_TIME_FROM_DATETIME, // time
          FunctionSignatureId.FN_DATETIME_FROM_DATE_AND_TIME, // datetime
          FunctionSignatureId.FN_DATETIME_FROM_YEAR_MONTH_DAY_HOUR_MINUTE_SECOND, // datetime
          FunctionSignatureId.FN_DATETIME_FROM_TIMESTAMP, // datetime
          FunctionSignatureId.FN_DATETIME_FROM_DATE, // datetime
          FunctionSignatureId.FN_STRING_FROM_TIMESTAMP, // string

          // Signatures for extracting date parts, taking a date/timestamp
          // and the target date part as arguments.
          FunctionSignatureId.FN_EXTRACT_FROM_DATE, // $extract
          FunctionSignatureId.FN_EXTRACT_FROM_DATETIME, // $extract
          FunctionSignatureId.FN_EXTRACT_FROM_TIME, // $extract
          FunctionSignatureId.FN_EXTRACT_FROM_TIMESTAMP, // $extract

          // Signatures specific to extracting the DATE date part from a DATETIME or a
          // TIMESTAMP.
          FunctionSignatureId.FN_EXTRACT_DATE_FROM_DATETIME, // $extract_date
          FunctionSignatureId.FN_EXTRACT_DATE_FROM_TIMESTAMP, // $extract_date

          // Signatures specific to extracting the TIME date part from a DATETIME or a
          // TIMESTAMP.
          FunctionSignatureId.FN_EXTRACT_TIME_FROM_DATETIME, // $extract_time
          FunctionSignatureId.FN_EXTRACT_TIME_FROM_TIMESTAMP, // $extract_time

          // Signature specific to extracting the DATETIME date part from a TIMESTAMP.
          FunctionSignatureId.FN_EXTRACT_DATETIME_FROM_TIMESTAMP, // $extract_datetime
          FunctionSignatureId.FN_FORMAT_DATE, // format_date
          FunctionSignatureId.FN_FORMAT_DATETIME, // format_datetime
          FunctionSignatureId.FN_FORMAT_TIME, // format_time
          FunctionSignatureId.FN_FORMAT_TIMESTAMP, // format_timestamp
          FunctionSignatureId.FN_PARSE_DATE, // parse_date
          FunctionSignatureId.FN_PARSE_DATETIME, // parse_datetime
          FunctionSignatureId.FN_PARSE_TIME, // parse_time
          FunctionSignatureId.FN_PARSE_TIMESTAMP, // parse_timestamp

          // Math functions
          // FunctionSignatureId.FN_ABS_INT64, // abs
          // FunctionSignatureId.FN_ABS_DOUBLE, // abs
          // FunctionSignatureId.FN_ABS_NUMERIC, // abs
          // FunctionSignatureId.FN_ABS_BIGNUMERIC, // abs
          // FunctionSignatureId.FN_SIGN_INT64, // sign
          // FunctionSignatureId.FN_SIGN_DOUBLE, // sign
          // FunctionSignatureId.FN_SIGN_NUMERIC, // sign
          // FunctionSignatureId.FN_SIGN_BIGNUMERIC, // sign

          // FunctionSignatureId.FN_ROUND_DOUBLE, // round(double) -> double
          // FunctionSignatureId.FN_ROUND_NUMERIC, // round(numeric) -> numeric
          // FunctionSignatureId.FN_ROUND_BIGNUMERIC, // round(bignumeric) -> bignumeric
          // FunctionSignatureId.FN_ROUND_WITH_DIGITS_DOUBLE, // round(double, int64) -> double
          // FunctionSignatureId.FN_ROUND_WITH_DIGITS_NUMERIC, // round(numeric, int64) -> numeric
          // round(bignumeric, int64) -> bignumeric
          // FunctionSignatureId.FN_ROUND_WITH_DIGITS_BIGNUMERIC,
          // FunctionSignatureId.FN_TRUNC_DOUBLE, // trunc(double) -> double
          // FunctionSignatureId.FN_TRUNC_NUMERIC, // trunc(numeric) -> numeric
          // FunctionSignatureId.FN_TRUNC_BIGNUMERIC, // trunc(bignumeric) -> bignumeric
          // FunctionSignatureId.FN_TRUNC_WITH_DIGITS_DOUBLE, // trunc(double, int64) -> double
          // FunctionSignatureId.FN_TRUNC_WITH_DIGITS_NUMERIC, // trunc(numeric, int64) -> numeric
          // trunc(bignumeric, int64) -> bignumeric
          // FunctionSignatureId.FN_TRUNC_WITH_DIGITS_BIGNUMERIC,
          FunctionSignatureId.FN_CEIL_DOUBLE, // ceil(double) -> double
          FunctionSignatureId.FN_CEIL_NUMERIC, // ceil(numeric) -> numeric
          // FunctionSignatureId.FN_CEIL_BIGNUMERIC, // ceil(bignumeric) -> bignumeric
          FunctionSignatureId.FN_FLOOR_DOUBLE, // floor(double) -> double
          FunctionSignatureId.FN_FLOOR_NUMERIC, // floor(numeric) -> numeric
          // FunctionSignatureId.FN_FLOOR_BIGNUMERIC, // floor(bignumeric) -> bignumeric

          FunctionSignatureId.FN_MOD_INT64, // mod(int64, int64) -> int64
          FunctionSignatureId.FN_MOD_NUMERIC, // mod(numeric, numeric) -> numeric
          // FunctionSignatureId.FN_MOD_BIGNUMERIC, // mod(bignumeric, bignumeric) -> bignumeric
          // FunctionSignatureId.FN_DIV_INT64, // div(int64, int64) -> int64
          // FunctionSignatureId.FN_DIV_NUMERIC, // div(numeric, numeric) -> numeric
          // FunctionSignatureId.FN_DIV_BIGNUMERIC, // div(bignumeric, bignumeric) -> bignumeric

          FunctionSignatureId.FN_IS_INF, // is_inf
          FunctionSignatureId.FN_IS_NAN, // is_nan
          // FunctionSignatureId.FN_IEEE_DIVIDE_DOUBLE, // ieee_divide
          // FunctionSignatureId.FN_SAFE_DIVIDE_DOUBLE, // safe_divide
          // FunctionSignatureId.FN_SAFE_DIVIDE_NUMERIC, // safe_divide
          // FunctionSignatureId.FN_SAFE_DIVIDE_BIGNUMERIC, // safe_divide
          // FunctionSignatureId.FN_SAFE_ADD_INT64, // safe_add
          // FunctionSignatureId.FN_SAFE_ADD_DOUBLE, // safe_add
          // FunctionSignatureId.FN_SAFE_ADD_NUMERIC, // safe_add
          // FunctionSignatureId.FN_SAFE_ADD_BIGNUMERIC, // safe_add
          // FunctionSignatureId.FN_SAFE_SUBTRACT_INT64, // safe_subtract
          // FunctionSignatureId.FN_SAFE_SUBTRACT_DOUBLE, // safe_subtract
          // FunctionSignatureId.FN_SAFE_SUBTRACT_NUMERIC, // safe_subtract
          // FunctionSignatureId.FN_SAFE_SUBTRACT_BIGNUMERIC, // safe_subtract
          // FunctionSignatureId.FN_SAFE_MULTIPLY_INT64, // safe_multiply
          // FunctionSignatureId.FN_SAFE_MULTIPLY_DOUBLE, // safe_multiply
          // FunctionSignatureId.FN_SAFE_MULTIPLY_NUMERIC, // safe_multiply
          // FunctionSignatureId.FN_SAFE_MULTIPLY_BIGNUMERIC, // safe_multiply
          // FunctionSignatureId.FN_SAFE_UNARY_MINUS_INT64, // safe_negate
          // FunctionSignatureId.FN_SAFE_UNARY_MINUS_DOUBLE, // safe_negate
          // FunctionSignatureId.FN_SAFE_UNARY_MINUS_NUMERIC, // safe_negate
          // FunctionSignatureId.FN_SAFE_UNARY_MINUS_BIGNUMERIC, // safe_negate

          // FunctionSignatureId.FN_GREATEST, // greatest
          // FunctionSignatureId.FN_LEAST, // least

          // FunctionSignatureId.FN_SQRT_DOUBLE, // sqrt
          // FunctionSignatureId.FN_POW_DOUBLE, // pow
          // FunctionSignatureId.FN_POW_NUMERIC, // pow(numeric, numeric) -> numeric
          // FunctionSignatureId.FN_POW_BIGNUMERIC, // pow(bignumeric, bignumeric) -> bignumeric
          // FunctionSignatureId.FN_EXP_DOUBLE, // exp
          // FunctionSignatureId.FN_NATURAL_LOGARITHM_DOUBLE, // ln and log
          // FunctionSignatureId.FN_DECIMAL_LOGARITHM_DOUBLE, // log10
          // FunctionSignatureId.FN_LOGARITHM_DOUBLE, // log

          // FunctionSignatureId.FN_COS_DOUBLE, // cos
          // FunctionSignatureId.FN_COSH_DOUBLE, // cosh
          // FunctionSignatureId.FN_ACOS_DOUBLE, // acos
          // FunctionSignatureId.FN_ACOSH_DOUBLE, // acosh
          // FunctionSignatureId.FN_SIN_DOUBLE, // sin
          // FunctionSignatureId.FN_SINH_DOUBLE, // sinh
          // FunctionSignatureId.FN_ASIN_DOUBLE, // asin
          // FunctionSignatureId.FN_ASINH_DOUBLE, // asinh
          // FunctionSignatureId.FN_TAN_DOUBLE, // tan
          // FunctionSignatureId.FN_TANH_DOUBLE, // tanh
          // FunctionSignatureId.FN_ATAN_DOUBLE, // atan
          // FunctionSignatureId.FN_ATANH_DOUBLE, // atanh
          // FunctionSignatureId.FN_ATAN2_DOUBLE, // atan2

          // Aggregate functions.
          FunctionSignatureId.FN_ANY_VALUE, // any_value
          // FunctionSignatureId.FN_ARRAY_AGG, // array_agg
          // FunctionSignatureId.FN_ARRAY_CONCAT_AGG, // array_concat_agg
          FunctionSignatureId.FN_AVG_INT64, // avg
          FunctionSignatureId.FN_AVG_DOUBLE, // avg
          FunctionSignatureId.FN_AVG_NUMERIC, // avg
          // FunctionSignatureId.FN_AVG_BIGNUMERIC, // avg
          FunctionSignatureId.FN_COUNT, // count
          FunctionSignatureId.FN_MAX, // max
          FunctionSignatureId.FN_MIN, // min
          FunctionSignatureId.FN_STRING_AGG_STRING, // string_agg(s)
          // FunctionSignatureId.FN_STRING_AGG_DELIM_STRING, // string_agg(s, delim_s)
          // FunctionSignatureId.FN_STRING_AGG_BYTES, // string_agg(b)
          // FunctionSignatureId.FN_STRING_AGG_DELIM_BYTES, // string_agg(b, delim_b)
          FunctionSignatureId.FN_SUM_INT64, // sum
          FunctionSignatureId.FN_SUM_DOUBLE, // sum
          FunctionSignatureId.FN_SUM_NUMERIC, // sum
          // FunctionSignatureId.FN_SUM_BIGNUMERIC, // sum
          // JIRA link: https://issues.apache.org/jira/browse/BEAM-10379
          // FunctionSignatureId.FN_BIT_AND_INT64, // bit_and
          FunctionSignatureId.FN_BIT_OR_INT64 // bit_or
          // FunctionSignatureId.FN_BIT_XOR_INT64, // bit_xor
          // FunctionSignatureId.FN_LOGICAL_AND, // logical_and
          // FunctionSignatureId.FN_LOGICAL_OR, // logical_or
          // Approximate aggregate functions.
          // FunctionSignatureId.FN_APPROX_COUNT_DISTINCT, // approx_count_distinct
          // FunctionSignatureId.FN_APPROX_QUANTILES, // approx_quantiles
          // FunctionSignatureId.FN_APPROX_TOP_COUNT, // approx_top_count
          // FunctionSignatureId.FN_APPROX_TOP_SUM_INT64, // approx_top_sum
          // FunctionSignatureId.FN_APPROX_TOP_SUM_DOUBLE, // approx_top_sum
          // FunctionSignatureId.FN_APPROX_TOP_SUM_NUMERIC, // approx_top_sum
          // FunctionSignatureId.FN_APPROX_TOP_SUM_BIGNUMERIC, // approx_top_sum

          // Approximate count functions that expose the intermediate sketch.
          // These are all found in the "hll_count.*" namespace.
          // FunctionSignatureId.FN_HLL_COUNT_MERGE, // hll_count.merge(bytes)
          // FunctionSignatureId.FN_HLL_COUNT_EXTRACT, // hll_count.extract(bytes), scalar
          // FunctionSignatureId.FN_HLL_COUNT_INIT_INT64, // hll_count.init(int64)
          // FunctionSignatureId.FN_HLL_COUNT_INIT_NUMERIC, // hll_count.init(numeric)
          // FunctionSignatureId.FN_HLL_COUNT_INIT_BIGNUMERIC, // hll_count.init(bignumeric)
          // FunctionSignatureId.FN_HLL_COUNT_INIT_STRING, // hll_count.init(string)
          // FunctionSignatureId.FN_HLL_COUNT_INIT_BYTES, // hll_count.init(bytes)
          // FunctionSignatureId.FN_HLL_COUNT_MERGE_PARTIAL, // hll_count.merge_partial(bytes)

          // Statistical aggregate functions.
          // FunctionSignatureId.FN_CORR, // corr
          // FunctionSignatureId.FN_CORR_NUMERIC, // corr
          // FunctionSignatureId.FN_COVAR_POP, // covar_pop
          // FunctionSignatureId.FN_COVAR_POP_NUMERIC, // covar_pop
          // FunctionSignatureId.FN_COVAR_SAMP, // covar_samp
          // FunctionSignatureId.FN_COVAR_SAMP_NUMERIC, // covar_samp
          // FunctionSignatureId.FN_STDDEV_POP, // stddev_pop
          // FunctionSignatureId.FN_STDDEV_POP_NUMERIC, // stddev_pop
          // FunctionSignatureId.FN_STDDEV_SAMP, // stddev_samp
          // FunctionSignatureId.FN_STDDEV_SAMP_NUMERIC, // stddev_samp
          // FunctionSignatureId.FN_VAR_POP, // var_pop
          // FunctionSignatureId.FN_VAR_POP_NUMERIC, // var_pop
          // FunctionSignatureId.FN_VAR_SAMP, // var_samp
          // FunctionSignatureId.FN_VAR_SAMP_NUMERIC, // var_samp

          // FunctionSignatureId.FN_COUNTIF, // countif

          // Approximate quantiles functions that produce or consume intermediate
          // sketches. All found in the "kll_quantiles.*" namespace.
          // FunctionSignatureId.FN_KLL_QUANTILES_INIT_INT64,
          // FunctionSignatureId.FN_KLL_QUANTILES_INIT_DOUBLE,
          // FunctionSignatureId.FN_KLL_QUANTILES_MERGE_PARTIAL,
          // FunctionSignatureId.FN_KLL_QUANTILES_MERGE_INT64,
          // FunctionSignatureId.FN_KLL_QUANTILES_MERGE_DOUBLE,
          // FunctionSignatureId.FN_KLL_QUANTILES_EXTRACT_INT64, // scalar
          // FunctionSignatureId.FN_KLL_QUANTILES_EXTRACT_DOUBLE, // scalar
          // FunctionSignatureId.FN_KLL_QUANTILES_MERGE_POINT_INT64,
          // FunctionSignatureId.FN_KLL_QUANTILES_MERGE_POINT_DOUBLE,
          // FunctionSignatureId.FN_KLL_QUANTILES_EXTRACT_POINT_INT64, // scalar
          // FunctionSignatureId.FN_KLL_QUANTILES_EXTRACT_POINT_DOUBLE, // scalar

          // Analytic functions.
          // FunctionSignatureId.FN_DENSE_RANK, // dense_rank
          // FunctionSignatureId.FN_RANK, // rank
          // FunctionSignatureId.FN_ROW_NUMBER, // row_number
          // FunctionSignatureId.FN_PERCENT_RANK, // percent_rank
          // FunctionSignatureId.FN_CUME_DIST, // cume_dist
          // FunctionSignatureId.FN_NTILE, // ntile
          // FunctionSignatureId.FN_LEAD, // lead
          // FunctionSignatureId.FN_LAG, // lag
          // FunctionSignatureId.FN_FIRST_VALUE, // first_value
          // FunctionSignatureId.FN_LAST_VALUE, // last_value
          // FunctionSignatureId.FN_NTH_VALUE, // nth_value
          // FunctionSignatureId.FN_PERCENTILE_CONT, // percentile_cont
          // FunctionSignatureId.FN_PERCENTILE_CONT_NUMERIC, // percentile_cont
          // FunctionSignatureId.FN_PERCENTILE_DISC, // percentile_disc
          // FunctionSignatureId.FN_PERCENTILE_DISC_NUMERIC, // percentile_disc

          // Misc functions.
          // FunctionSignatureId.FN_BIT_CAST_INT64_TO_INT64, // bit_cast_to_int64(int64)

          // FunctionSignatureId.FN_SESSION_USER, // session_user

          // FunctionSignatureId.FN_GENERATE_ARRAY_INT64, // generate_array(int64)
          // FunctionSignatureId.FN_GENERATE_ARRAY_NUMERIC, // generate_array(numeric)
          // FunctionSignatureId.FN_GENERATE_ARRAY_BIGNUMERIC, // generate_array(bignumeric)
          // FunctionSignatureId.FN_GENERATE_ARRAY_DOUBLE, // generate_array(double)
          // FunctionSignatureId.FN_GENERATE_DATE_ARRAY, // generate_date_array(date)
          // FunctionSignatureId.FN_GENERATE_TIMESTAMP_ARRAY, // generate_timestamp_array(timestamp)

          // FunctionSignatureId.FN_ARRAY_REVERSE, // array_reverse(array) -> array

          // FunctionSignatureId.FN_RANGE_BUCKET, //  range_bucket(T, array<T>) -> int64

          // FunctionSignatureId.FN_RAND, // rand() -> double
          // FunctionSignatureId.FN_GENERATE_UUID, // generate_uuid() -> string

          // FunctionSignatureId.FN_JSON_EXTRACT, // json_extract(string, string)
          // FunctionSignatureId.FN_JSON_EXTRACT_SCALAR, // json_extract_scalar(string, string)
          // json_extract_array(string[, string]) -> array
          // FunctionSignatureId.FN_JSON_EXTRACT_ARRAY,

          // FunctionSignatureId.FN_TO_JSON_STRING, // to_json_string(any[, bool]) -> string
          // FunctionSignatureId.FN_JSON_QUERY, // json_query(string, string)
          // FunctionSignatureId.FN_JSON_VALUE, // json_value(string, string)

          // Net functions. These are all found in the "net.*" namespace.
          // FunctionSignatureId.FN_NET_FORMAT_IP,
          // FunctionSignatureId.FN_NET_PARSE_IP,
          // FunctionSignatureId.FN_NET_FORMAT_PACKED_IP,
          // FunctionSignatureId.FN_NET_PARSE_PACKED_IP,
          // FunctionSignatureId.FN_NET_IP_IN_NET,
          // FunctionSignatureId.FN_NET_MAKE_NET,
          // FunctionSignatureId.FN_NET_HOST, // net.host(string)
          // FunctionSignatureId.FN_NET_REG_DOMAIN, // net.reg_domain(string)
          // FunctionSignatureId.FN_NET_PUBLIC_SUFFIX, // net.public_suffix(string)
          // FunctionSignatureId.FN_NET_IP_FROM_STRING, // net.ip_from_string(string)
          // FunctionSignatureId.FN_NET_SAFE_IP_FROM_STRING, // net.safe_ip_from_string(string)
          // FunctionSignatureId.FN_NET_IP_TO_STRING, // net.ip_to_string(bytes)
          // FunctionSignatureId.FN_NET_IP_NET_MASK, // net.ip_net_mask(int64, int64)
          // FunctionSignatureId.FN_NET_IP_TRUNC, // net.ip_net_mask(bytes, int64)
          // FunctionSignatureId.FN_NET_IPV4_FROM_INT64, // net.ipv4_from_int64(int64)
          // FunctionSignatureId.FN_NET_IPV4_TO_INT64, // net.ipv4_to_int64(bytes)

          // Hashing functions.
          // FunctionSignatureId.FN_MD5_BYTES, // md5(bytes)
          // FunctionSignatureId.FN_MD5_STRING, // md5(string)
          // FunctionSignatureId.FN_SHA1_BYTES, // sha1(bytes)
          // FunctionSignatureId.FN_SHA1_STRING, // sha1(string)
          // FunctionSignatureId.FN_SHA256_BYTES, // sha256(bytes)
          // FunctionSignatureId.FN_SHA256_STRING, // sha256(string)
          // FunctionSignatureId.FN_SHA512_BYTES, // sha512(bytes)
          // FunctionSignatureId.FN_SHA512_STRING, // sha512(string)

          // Fingerprinting functions
          // FunctionSignatureId.FN_FARM_FINGERPRINT_BYTES, // farm_fingerprint(bytes) -> int64
          // FunctionSignatureId.FN_FARM_FINGERPRINT_STRING, // farm_fingerprint(string) -> int64

          // Keyset management, encryption, and decryption functions
          // Requires that FEATURE_ENCRYPTION is enabled.
          // FunctionSignatureId.FN_KEYS_NEW_KEYSET, // keys.new_keyset(string)
          // keys.add_key_from_raw_bytes(bytes, string, bytes)
          // FunctionSignatureId.FN_KEYS_ADD_KEY_FROM_RAW_BYTES,
          // FunctionSignatureId.FN_KEYS_ROTATE_KEYSET, // keys.rotate_keyset(bytes, string)
          // FunctionSignatureId.FN_KEYS_KEYSET_LENGTH, // keys.keyset_length(bytes)
          // FunctionSignatureId.FN_KEYS_KEYSET_TO_JSON, // keys.keyset_to_json(bytes)
          // FunctionSignatureId.FN_KEYS_KEYSET_FROM_JSON, // keys.keyset_from_json(string)
          // FunctionSignatureId.FN_AEAD_ENCRYPT_STRING, // aead.encrypt(bytes, string, string)
          // FunctionSignatureId.FN_AEAD_ENCRYPT_BYTES, // aead.encrypt(bytes, bytes, bytes)
          // FunctionSignatureId.FN_AEAD_DECRYPT_STRING,// aead.decrypt_string(bytes, bytes, string)
          // FunctionSignatureId.FN_AEAD_DECRYPT_BYTES, // aead.decrypt_bytes(bytes, bytes, bytes)
          // FunctionSignatureId.FN_KMS_ENCRYPT_STRING, // kms.encrypt(string, string)
          // FunctionSignatureId.FN_KMS_ENCRYPT_BYTES, // kms.encrypt(string, bytes)
          // FunctionSignatureId.FN_KMS_DECRYPT_STRING, // kms.decrypt_string(string, bytes)
          // FunctionSignatureId.FN_KMS_DECRYPT_BYTES, // kms.decrypt_bytes(string, bytes)

          // ST_ family of functions (Geography related)
          // Constructors
          // FunctionSignatureId.FN_ST_GEOG_POINT,
          // FunctionSignatureId.FN_ST_MAKE_LINE,
          // FunctionSignatureId.FN_ST_MAKE_LINE_ARRAY,
          // FunctionSignatureId.FN_ST_MAKE_POLYGON,
          // FunctionSignatureId.FN_ST_MAKE_POLYGON_ORIENTED,
          // Transformations
          // FunctionSignatureId.FN_ST_INTERSECTION,
          // FunctionSignatureId.FN_ST_UNION,
          // FunctionSignatureId.FN_ST_UNION_ARRAY,
          // FunctionSignatureId.FN_ST_DIFFERENCE,
          // FunctionSignatureId.FN_ST_UNARY_UNION,
          // FunctionSignatureId.FN_ST_CENTROID,
          // FunctionSignatureId.FN_ST_BUFFER,
          // FunctionSignatureId.FN_ST_BUFFER_WITH_TOLERANCE,
          // FunctionSignatureId.FN_ST_SIMPLIFY,
          // FunctionSignatureId.FN_ST_SNAP_TO_GRID,
          // FunctionSignatureId.FN_ST_CLOSEST_POINT,
          // FunctionSignatureId.FN_ST_BOUNDARY,
          // FunctionSignatureId.FN_ST_CONVEXHULL,
          // Predicates
          // FunctionSignatureId.FN_ST_EQUALS,
          // FunctionSignatureId.FN_ST_INTERSECTS,
          // FunctionSignatureId.FN_ST_CONTAINS,
          // FunctionSignatureId.FN_ST_COVERS,
          // FunctionSignatureId.FN_ST_DISJOINT,
          // FunctionSignatureId.FN_ST_INTERSECTS_BOX,
          // FunctionSignatureId.FN_ST_DWITHIN,
          // FunctionSignatureId.FN_ST_WITHIN,
          // FunctionSignatureId.FN_ST_COVEREDBY,
          // FunctionSignatureId.FN_ST_TOUCHES,
          // Accessors
          // FunctionSignatureId.FN_ST_IS_EMPTY,
          // FunctionSignatureId.FN_ST_IS_COLLECTION,
          // FunctionSignatureId.FN_ST_DIMENSION,
          // FunctionSignatureId.FN_ST_NUM_POINTS,
          // FunctionSignatureId.FN_ST_DUMP,
          // Measures
          // FunctionSignatureId.FN_ST_LENGTH,
          // FunctionSignatureId.FN_ST_PERIMETER,
          // FunctionSignatureId.FN_ST_AREA,
          // FunctionSignatureId.FN_ST_DISTANCE,
          // FunctionSignatureId.FN_ST_MAX_DISTANCE,
          // Parsers/formatters
          // FunctionSignatureId.FN_ST_GEOG_FROM_TEXT,
          // FunctionSignatureId.FN_ST_GEOG_FROM_KML,
          // FunctionSignatureId.FN_ST_GEOG_FROM_GEO_JSON,
          // FunctionSignatureId.FN_ST_GEOG_FROM_WKB,
          // FunctionSignatureId.FN_ST_AS_TEXT,
          // FunctionSignatureId.FN_ST_AS_KML,
          // FunctionSignatureId.FN_ST_AS_GEO_JSON,
          // FunctionSignatureId.FN_ST_AS_BINARY,
          // FunctionSignatureId.FN_ST_GEOHASH,
          // FunctionSignatureId.FN_ST_GEOG_POINT_FROM_GEOHASH,
          // Aggregate functions
          // FunctionSignatureId.FN_ST_UNION_AGG,
          // FunctionSignatureId.FN_ST_ACCUM,
          // FunctionSignatureId.FN_ST_CENTROID_AGG,
          // Other geography functions
          // FunctionSignatureId.FN_ST_X,
          // FunctionSignatureId.FN_ST_Y,

          // Array functions.
          // FunctionSignatureId.FN_FLATTEN, // flatten(array path) -> array
          // FunctionSignatureId.FN_ARRAY_AT_OFFSET, // $array_at_offset
          // FunctionSignatureId.FN_ARRAY_AT_ORDINAL, // $array_at_ordinal
          // FunctionSignatureId.FN_ARRAY_CONCAT, // array_concat(repeated array) -> array
          // FunctionSignatureId.FN_ARRAY_CONCAT_OP, // array_concat(array, array) -> array
          // FunctionSignatureId.FN_ARRAY_LENGTH, // array_length(array) -> int64
          // array_to_string(array, bytes[, bytes]) -> bytes
          // FunctionSignatureId.FN_ARRAY_TO_BYTES,
          // array_to_string(array, string[, string]) -> string
          // FunctionSignatureId.FN_ARRAY_TO_STRING,
          // FunctionSignatureId.FN_MAKE_ARRAY, // $make_array
          // FunctionSignatureId.FN_SAFE_ARRAY_AT_OFFSET, // $safe_array_at_offset
          // FunctionSignatureId.FN_SAFE_ARRAY_AT_ORDINAL, // $safe_array_at_ordinal
          );
}
