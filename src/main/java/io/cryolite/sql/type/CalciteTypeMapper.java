package io.cryolite.sql.type;

import io.cryolite.sql.SqlExecutionException;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Maps Calcite SQL data types to Apache Iceberg types.
 *
 * <p>This mapper converts the SQL type information parsed by Apache Calcite into the corresponding
 * Iceberg {@link Type} instances. It is the central translation layer between the SQL world and the
 * Iceberg storage world.
 *
 * <p>Supported SQL types and their Iceberg equivalents:
 *
 * <ul>
 *   <li>BOOLEAN → {@link Types.BooleanType}
 *   <li>TINYINT, SMALLINT, INTEGER, INT → {@link Types.IntegerType}
 *   <li>BIGINT → {@link Types.LongType}
 *   <li>FLOAT, REAL → {@link Types.FloatType}
 *   <li>DOUBLE, DOUBLE PRECISION → {@link Types.DoubleType}
 *   <li>DECIMAL(p,s) → {@link Types.DecimalType}
 *   <li>VARCHAR, CHAR, STRING → {@link Types.StringType}
 *   <li>BINARY, VARBINARY → {@link Types.BinaryType}
 *   <li>DATE → {@link Types.DateType}
 *   <li>TIME → {@link Types.TimeType}
 *   <li>TIMESTAMP → {@link Types.TimestampType} (without timezone)
 *   <li>TIMESTAMP WITH LOCAL TIME ZONE → {@link Types.TimestampType} (with timezone)
 * </ul>
 *
 * @since 0.1.0
 */
public final class CalciteTypeMapper {

  private CalciteTypeMapper() {
    // utility class
  }

  /**
   * Converts a Calcite {@link SqlDataTypeSpec} to an Iceberg {@link Type}.
   *
   * @param dataTypeSpec the Calcite data type specification
   * @return the corresponding Iceberg type
   * @throws SqlExecutionException if the type is not supported
   */
  public static Type toIcebergType(SqlDataTypeSpec dataTypeSpec) {
    // Extract the type name string by joining all identifier name parts.
    // For simple types like INTEGER this yields "INTEGER".
    // For compound types like TIMESTAMP WITH LOCAL TIME ZONE this yields "TIMESTAMP WITH LOCAL TIME
    // ZONE".
    String typeName = String.join(" ", dataTypeSpec.getTypeName().names).toUpperCase();

    return switch (typeName) {
      case "BOOLEAN" -> Types.BooleanType.get();

      // Calcite SqlTypeName enum names (used internally as identifier names)
      case "TINYINT", "SMALLINT", "INTEGER" -> Types.IntegerType.get();

      case "BIGINT" -> Types.LongType.get();

      case "FLOAT", "REAL" -> Types.FloatType.get();

      case "DOUBLE" -> Types.DoubleType.get();

      case "DECIMAL" -> toDecimalType(dataTypeSpec);

      case "VARCHAR", "CHAR" -> Types.StringType.get();

      case "BINARY", "VARBINARY" -> Types.BinaryType.get();

      case "DATE" -> Types.DateType.get();

      case "TIME" -> Types.TimeType.get();

      case "TIMESTAMP" -> Types.TimestampType.withoutZone();

      // SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE → enum name as single identifier
      case "TIMESTAMP_WITH_LOCAL_TIME_ZONE" -> Types.TimestampType.withZone();

      default ->
          throw new SqlExecutionException(
              "Unsupported SQL type: '"
                  + typeName
                  + "'. Supported types: BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, "
                  + "FLOAT, REAL, DOUBLE, DECIMAL, VARCHAR, CHAR, BINARY, DATE, TIME, TIMESTAMP, "
                  + "TIMESTAMP WITH LOCAL TIME ZONE");
    };
  }

  private static Types.DecimalType toDecimalType(SqlDataTypeSpec dataTypeSpec) {
    if (dataTypeSpec.getTypeNameSpec() instanceof SqlBasicTypeNameSpec basicSpec) {
      int precision = basicSpec.getPrecision();
      int scale = basicSpec.getScale();
      // Default precision/scale when not specified (-1 in Calcite)
      int resolvedPrecision = (precision < 0) ? 38 : precision;
      int resolvedScale = (scale < 0) ? 0 : scale;
      return Types.DecimalType.of(resolvedPrecision, resolvedScale);
    }
    // Fallback: use default precision and scale
    return Types.DecimalType.of(38, 0);
  }
}
