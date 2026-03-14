package io.cryolite.sql.type;

import io.cryolite.sql.SqlExecutionException;
import java.math.BigDecimal;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Converts Calcite {@link SqlLiteral} values to Java objects compatible with the Iceberg data API.
 *
 * <p>The conversion is type-directed: the target Iceberg column type determines how the SQL literal
 * is interpreted. For example, a numeric literal {@code 42} becomes a {@code Long} when the target
 * column is {@code BIGINT}, but an {@code Integer} for {@code INTEGER}.
 *
 * <p>A {@code NULL} literal always produces {@code null}, regardless of the target type.
 *
 * @since 0.1.0
 */
public final class SqlLiteralConverter {

  private SqlLiteralConverter() {
    // utility class
  }

  /**
   * Converts a Calcite SQL literal to the Java value expected by Iceberg for the given column type.
   *
   * @param literal the Calcite SQL literal to convert
   * @param targetType the Iceberg type of the target column
   * @return the Java value, or {@code null} for SQL NULL literals
   * @throws SqlExecutionException if the literal cannot be converted to the target type
   */
  public static Object toJavaValue(SqlLiteral literal, Type targetType) {
    if (literal.getTypeName() == SqlTypeName.NULL) {
      return null;
    }

    try {
      return switch (targetType.typeId()) {
        case LONG -> toLong(literal);
        case INTEGER -> toInteger(literal);
        case FLOAT -> toFloat(literal);
        case DOUBLE -> toDouble(literal);
        case DECIMAL -> toDecimal(literal, (Types.DecimalType) targetType);
        case STRING -> toString(literal);
        case BOOLEAN -> toBoolean(literal);
        default ->
            throw new SqlExecutionException(
                "Unsupported target type for literal conversion: "
                    + targetType.typeId()
                    + ". Literal was: '"
                    + literal
                    + "'");
      };
    } catch (SqlExecutionException e) {
      throw e;
    } catch (Exception e) {
      throw new SqlExecutionException(
          "Cannot convert SQL literal '"
              + literal
              + "' to target type "
              + targetType.typeId()
              + ": "
              + e.getMessage(),
          e);
    }
  }

  // --- private converters ---

  private static Long toLong(SqlLiteral literal) {
    BigDecimal bd = (BigDecimal) literal.getValue();
    return bd.longValueExact();
  }

  private static Integer toInteger(SqlLiteral literal) {
    BigDecimal bd = (BigDecimal) literal.getValue();
    return bd.intValueExact();
  }

  private static Float toFloat(SqlLiteral literal) {
    BigDecimal bd = (BigDecimal) literal.getValue();
    return bd.floatValue();
  }

  private static Double toDouble(SqlLiteral literal) {
    BigDecimal bd = (BigDecimal) literal.getValue();
    return bd.doubleValue();
  }

  private static BigDecimal toDecimal(SqlLiteral literal, Types.DecimalType targetType) {
    BigDecimal bd = (BigDecimal) literal.getValue();
    return bd.setScale(targetType.scale(), java.math.RoundingMode.UNNECESSARY);
  }

  private static String toString(SqlLiteral literal) {
    // SqlCharStringLiteral.toValue() strips surrounding quotes and unescapes
    return literal.getValueAs(String.class);
  }

  private static Boolean toBoolean(SqlLiteral literal) {
    return (Boolean) literal.getValue();
  }
}
