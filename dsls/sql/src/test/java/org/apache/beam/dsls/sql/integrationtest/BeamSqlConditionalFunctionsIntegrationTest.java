package org.apache.beam.dsls.sql.integrationtest;

import org.junit.Test;

/**
 * Integration test for conditional functions.
 */
public class BeamSqlConditionalFunctionsIntegrationTest
    extends BeamSqlBuiltinFunctionsIntegrationTestBase {
    @Test
    public void testConditionalFunctions() throws Exception {
      ExpressionChecker checker = new ExpressionChecker()
          .addExpr(
              "CASE 1 WHEN 1 THEN 'hello' ELSE 'world' END",
              "hello"
          )
          .addExpr(
              "CASE 2 "
                  + "WHEN 1 THEN 'hello' "
                  + "WHEN 3 THEN 'bond' "
                  + "ELSE 'world' END",
              "world"
          )
          .addExpr(
              "CASE "
                  + "WHEN 1 = 1 THEN 'hello' "
                  + "ELSE 'world' END",
              "hello"
          )
          .addExpr(
              "CASE "
                  + "WHEN 1 > 1 THEN 'hello' "
                  + "ELSE 'world' END",
              "world"
          )
          .addExpr("NULLIF(5, 4) ", 5)
          .addExpr("COALESCE(1, 5) ", 1)
          .addExpr("COALESCE(NULL, 5) ", 5)
          ;

      checker.buildRunAndCheck();
    }
}
