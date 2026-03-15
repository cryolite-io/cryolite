package io.cryolite.sql.filter;

import static org.junit.jupiter.api.Assertions.*;

import io.cryolite.filter.AndPredicate;
import io.cryolite.filter.BatchPredicate;
import io.cryolite.filter.ComparisonOperator;
import io.cryolite.filter.ComparisonPredicate;
import io.cryolite.sql.SqlExecutionException;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SqlWhereConverter}.
 *
 * <p>Verifies the conversion of Calcite WHERE clause AST nodes into {@link BatchPredicate} trees.
 */
class SqlWhereConverterTest {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required(2, "name", Types.StringType.get()),
          Types.NestedField.optional(3, "age", Types.IntegerType.get()));

  // --- Simple comparisons ---

  @Test
  void convertsEqualsComparison() {
    // id = 42
    SqlNode where = comparison(SqlStdOperatorTable.EQUALS, "id", 42);
    BatchPredicate pred = SqlWhereConverter.convert(where, SCHEMA);

    assertInstanceOf(ComparisonPredicate.class, pred);
    ComparisonPredicate cp = (ComparisonPredicate) pred;
    assertEquals("id", cp.getColumnName());
    assertEquals(ComparisonOperator.EQUALS, cp.getOperator());
    assertEquals(42L, cp.getLiteral()); // target type is LONG
  }

  @Test
  void convertsNotEqualsComparison() {
    SqlNode where = comparison(SqlStdOperatorTable.NOT_EQUALS, "name", "Bob");
    BatchPredicate pred = SqlWhereConverter.convert(where, SCHEMA);

    assertInstanceOf(ComparisonPredicate.class, pred);
    ComparisonPredicate cp = (ComparisonPredicate) pred;
    assertEquals("name", cp.getColumnName());
    assertEquals(ComparisonOperator.NOT_EQUALS, cp.getOperator());
    assertEquals("Bob", cp.getLiteral());
  }

  @Test
  void convertsLessThanComparison() {
    SqlNode where = comparison(SqlStdOperatorTable.LESS_THAN, "age", 30);
    BatchPredicate pred = SqlWhereConverter.convert(where, SCHEMA);

    assertInstanceOf(ComparisonPredicate.class, pred);
    ComparisonPredicate cp = (ComparisonPredicate) pred;
    assertEquals("age", cp.getColumnName());
    assertEquals(ComparisonOperator.LESS_THAN, cp.getOperator());
    assertEquals(30, cp.getLiteral()); // target type is INTEGER
  }

  // --- AND ---

  @Test
  void convertsAndWithTwoOperands() {
    // id = 1 AND name = 'Alice'
    SqlNode left = comparison(SqlStdOperatorTable.EQUALS, "id", 1);
    SqlNode right = comparison(SqlStdOperatorTable.EQUALS, "name", "Alice");
    SqlNode andNode =
        new SqlBasicCall(SqlStdOperatorTable.AND, new SqlNode[] {left, right}, SqlParserPos.ZERO);

    BatchPredicate pred = SqlWhereConverter.convert(andNode, SCHEMA);
    assertInstanceOf(AndPredicate.class, pred);
    AndPredicate and = (AndPredicate) pred;
    assertEquals(2, and.getOperands().size());
  }

  // --- Reversed operands (literal op column) ---

  @Test
  void convertsReversedComparisonLiteralOpColumn() {
    // 42 = id  → becomes  id = 42
    SqlNode literal = SqlLiteral.createExactNumeric("42", SqlParserPos.ZERO);
    SqlNode column = new SqlIdentifier("id", SqlParserPos.ZERO);
    SqlNode reversed =
        new SqlBasicCall(
            SqlStdOperatorTable.EQUALS, new SqlNode[] {literal, column}, SqlParserPos.ZERO);

    BatchPredicate pred = SqlWhereConverter.convert(reversed, SCHEMA);
    assertInstanceOf(ComparisonPredicate.class, pred);
    ComparisonPredicate cp = (ComparisonPredicate) pred;
    assertEquals("id", cp.getColumnName());
    assertEquals(42L, cp.getLiteral());
  }

  // --- Remaining comparison operators ---

  @Test
  void convertsLessThanOrEqualComparison() {
    SqlNode where = comparison(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, "age", 30);
    BatchPredicate pred = SqlWhereConverter.convert(where, SCHEMA);

    assertInstanceOf(ComparisonPredicate.class, pred);
    ComparisonPredicate cp = (ComparisonPredicate) pred;
    assertEquals(ComparisonOperator.LESS_THAN_OR_EQUAL, cp.getOperator());
  }

  @Test
  void convertsGreaterThanComparison() {
    SqlNode where = comparison(SqlStdOperatorTable.GREATER_THAN, "id", 5);
    BatchPredicate pred = SqlWhereConverter.convert(where, SCHEMA);

    assertInstanceOf(ComparisonPredicate.class, pred);
    ComparisonPredicate cp = (ComparisonPredicate) pred;
    assertEquals(ComparisonOperator.GREATER_THAN, cp.getOperator());
  }

  @Test
  void convertsGreaterThanOrEqualComparison() {
    SqlNode where = comparison(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, "id", 10);
    BatchPredicate pred = SqlWhereConverter.convert(where, SCHEMA);

    assertInstanceOf(ComparisonPredicate.class, pred);
    ComparisonPredicate cp = (ComparisonPredicate) pred;
    assertEquals(ComparisonOperator.GREATER_THAN_OR_EQUAL, cp.getOperator());
  }

  // --- Error cases ---

  @Test
  void unknownColumnThrowsSqlExecutionException() {
    SqlNode where = comparison(SqlStdOperatorTable.EQUALS, "nonexistent", 1);
    SqlExecutionException ex =
        assertThrows(SqlExecutionException.class, () -> SqlWhereConverter.convert(where, SCHEMA));
    assertTrue(ex.getMessage().contains("nonexistent"));
    assertTrue(ex.getMessage().contains("not found"));
  }

  @Test
  void unsupportedOperatorThrowsSqlExecutionException() {
    // Use OR which is not supported in M8
    SqlNode left = comparison(SqlStdOperatorTable.EQUALS, "id", 1);
    SqlNode right = comparison(SqlStdOperatorTable.EQUALS, "id", 2);
    SqlNode orNode =
        new SqlBasicCall(SqlStdOperatorTable.OR, new SqlNode[] {left, right}, SqlParserPos.ZERO);

    SqlExecutionException ex =
        assertThrows(SqlExecutionException.class, () -> SqlWhereConverter.convert(orNode, SCHEMA));
    assertTrue(ex.getMessage().contains("Unsupported WHERE operator"));
  }

  @Test
  void unsupportedNodeTypeThrowsSqlExecutionException() {
    // Pass a plain SqlLiteral as WHERE node (not a SqlBasicCall)
    SqlNode literal = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
    SqlExecutionException ex =
        assertThrows(SqlExecutionException.class, () -> SqlWhereConverter.convert(literal, SCHEMA));
    assertTrue(ex.getMessage().contains("Unsupported WHERE clause node type"));
  }

  @Test
  void unsupportedComparisonOperandsThrowsSqlExecutionException() {
    // column = column (not column = literal)
    SqlNode col1 = new SqlIdentifier("id", SqlParserPos.ZERO);
    SqlNode col2 = new SqlIdentifier("age", SqlParserPos.ZERO);
    SqlNode call =
        new SqlBasicCall(SqlStdOperatorTable.EQUALS, new SqlNode[] {col1, col2}, SqlParserPos.ZERO);

    SqlExecutionException ex =
        assertThrows(SqlExecutionException.class, () -> SqlWhereConverter.convert(call, SCHEMA));
    assertTrue(ex.getMessage().contains("Unsupported comparison operands"));
  }

  // --- Helpers ---

  private SqlNode comparison(SqlOperator op, String column, long value) {
    SqlNode col = new SqlIdentifier(column, SqlParserPos.ZERO);
    SqlNode lit = SqlLiteral.createExactNumeric(String.valueOf(value), SqlParserPos.ZERO);
    return new SqlBasicCall(op, new SqlNode[] {col, lit}, SqlParserPos.ZERO);
  }

  private SqlNode comparison(SqlOperator op, String column, String value) {
    SqlNode col = new SqlIdentifier(column, SqlParserPos.ZERO);
    SqlNode lit = SqlLiteral.createCharString(value, SqlParserPos.ZERO);
    return new SqlBasicCall(op, new SqlNode[] {col, lit}, SqlParserPos.ZERO);
  }
}
