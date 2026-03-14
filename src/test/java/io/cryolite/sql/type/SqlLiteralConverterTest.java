package io.cryolite.sql.type;

import static org.junit.jupiter.api.Assertions.*;

import io.cryolite.sql.SqlExecutionException;
import java.math.BigDecimal;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SqlLiteralConverter}.
 *
 * <p>Verifies type-directed conversion of Calcite SQL literals to Java values compatible with the
 * Iceberg data API.
 */
class SqlLiteralConverterTest {

  // --- NULL ---

  @Test
  void nullLiteralReturnsNull() {
    SqlLiteral literal = SqlLiteral.createNull(SqlParserPos.ZERO);
    assertNull(SqlLiteralConverter.toJavaValue(literal, Types.LongType.get()));
  }

  // --- LONG ---

  @Test
  void numericLiteralToLong() {
    SqlLiteral literal = SqlLiteral.createExactNumeric("42", SqlParserPos.ZERO);
    Object result = SqlLiteralConverter.toJavaValue(literal, Types.LongType.get());
    assertEquals(42L, result);
  }

  // --- INTEGER ---

  @Test
  void numericLiteralToInteger() {
    SqlLiteral literal = SqlLiteral.createExactNumeric("7", SqlParserPos.ZERO);
    Object result = SqlLiteralConverter.toJavaValue(literal, Types.IntegerType.get());
    assertEquals(7, result);
  }

  // --- FLOAT ---

  @Test
  void numericLiteralToFloat() {
    SqlLiteral literal = SqlLiteral.createExactNumeric("3.14", SqlParserPos.ZERO);
    Object result = SqlLiteralConverter.toJavaValue(literal, Types.FloatType.get());
    assertInstanceOf(Float.class, result);
    assertEquals(3.14f, (Float) result, 0.001f);
  }

  // --- DOUBLE ---

  @Test
  void numericLiteralToDouble() {
    SqlLiteral literal = SqlLiteral.createExactNumeric("2.718", SqlParserPos.ZERO);
    Object result = SqlLiteralConverter.toJavaValue(literal, Types.DoubleType.get());
    assertInstanceOf(Double.class, result);
    assertEquals(2.718, (Double) result, 0.0001);
  }

  // --- DECIMAL ---

  @Test
  void numericLiteralToDecimal() {
    SqlLiteral literal = SqlLiteral.createExactNumeric("99.99", SqlParserPos.ZERO);
    Object result = SqlLiteralConverter.toJavaValue(literal, Types.DecimalType.of(10, 2));
    assertInstanceOf(BigDecimal.class, result);
    assertEquals(new BigDecimal("99.99"), result);
  }

  // --- STRING ---

  @Test
  void charLiteralToString() {
    SqlLiteral literal = SqlLiteral.createCharString("hello", SqlParserPos.ZERO);
    Object result = SqlLiteralConverter.toJavaValue(literal, Types.StringType.get());
    assertEquals("hello", result);
  }

  // --- BOOLEAN ---

  @Test
  void booleanLiteralToBoolean() {
    SqlLiteral literalTrue = SqlLiteral.createBoolean(true, SqlParserPos.ZERO);
    SqlLiteral literalFalse = SqlLiteral.createBoolean(false, SqlParserPos.ZERO);

    assertEquals(
        Boolean.TRUE, SqlLiteralConverter.toJavaValue(literalTrue, Types.BooleanType.get()));
    assertEquals(
        Boolean.FALSE, SqlLiteralConverter.toJavaValue(literalFalse, Types.BooleanType.get()));
  }

  // --- Error cases ---

  @Test
  void unsupportedTargetTypeThrowsSqlExecutionException() {
    SqlLiteral literal = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
    SqlExecutionException ex =
        assertThrows(
            SqlExecutionException.class,
            () -> SqlLiteralConverter.toJavaValue(literal, Types.BinaryType.get()));
    assertTrue(ex.getMessage().contains("Unsupported target type"));
  }

  @Test
  void conversionErrorWrapsInSqlExecutionException() {
    // A string literal cannot be cast to BigDecimal for a LONG target
    SqlLiteral literal = SqlLiteral.createCharString("not_a_number", SqlParserPos.ZERO);
    SqlExecutionException ex =
        assertThrows(
            SqlExecutionException.class,
            () -> SqlLiteralConverter.toJavaValue(literal, Types.LongType.get()));
    assertTrue(ex.getMessage().contains("Cannot convert SQL literal"));
    assertNotNull(ex.getCause());
  }
}
