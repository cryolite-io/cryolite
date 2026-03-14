package io.cryolite.sql.type;

import static org.junit.jupiter.api.Assertions.*;

import io.cryolite.sql.SqlExecutionException;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link CalciteTypeMapper}.
 *
 * <p>Verifies that every supported SQL type is correctly mapped to its Iceberg equivalent, and that
 * unsupported types produce a clear error message.
 */
class CalciteTypeMapperTest {

  // Helper to build a simple SqlDataTypeSpec without precision/scale
  private static SqlDataTypeSpec spec(SqlTypeName typeName) {
    return new SqlDataTypeSpec(
        new SqlBasicTypeNameSpec(typeName, SqlParserPos.ZERO), SqlParserPos.ZERO);
  }

  // Helper to build a SqlDataTypeSpec with precision and scale (for DECIMAL)
  private static SqlDataTypeSpec decimalSpec(int precision, int scale) {
    return new SqlDataTypeSpec(
        new SqlBasicTypeNameSpec(SqlTypeName.DECIMAL, precision, scale, SqlParserPos.ZERO),
        SqlParserPos.ZERO);
  }

  @Test
  void booleanMapsToIcebergBoolean() {
    assertEquals(
        Types.BooleanType.get(), CalciteTypeMapper.toIcebergType(spec(SqlTypeName.BOOLEAN)));
  }

  @Test
  void tinyintMapsToIcebergInteger() {
    assertEquals(
        Types.IntegerType.get(), CalciteTypeMapper.toIcebergType(spec(SqlTypeName.TINYINT)));
  }

  @Test
  void smallintMapsToIcebergInteger() {
    assertEquals(
        Types.IntegerType.get(), CalciteTypeMapper.toIcebergType(spec(SqlTypeName.SMALLINT)));
  }

  @Test
  void integerMapsToIcebergInteger() {
    assertEquals(
        Types.IntegerType.get(), CalciteTypeMapper.toIcebergType(spec(SqlTypeName.INTEGER)));
  }

  @Test
  void bigintMapsToIcebergLong() {
    assertEquals(Types.LongType.get(), CalciteTypeMapper.toIcebergType(spec(SqlTypeName.BIGINT)));
  }

  @Test
  void floatMapsToIcebergFloat() {
    assertEquals(Types.FloatType.get(), CalciteTypeMapper.toIcebergType(spec(SqlTypeName.FLOAT)));
  }

  @Test
  void realMapsToIcebergFloat() {
    assertEquals(Types.FloatType.get(), CalciteTypeMapper.toIcebergType(spec(SqlTypeName.REAL)));
  }

  @Test
  void doubleMapsToIcebergDouble() {
    assertEquals(Types.DoubleType.get(), CalciteTypeMapper.toIcebergType(spec(SqlTypeName.DOUBLE)));
  }

  @Test
  void decimalWithPrecisionAndScaleMapsCorrectly() {
    Types.DecimalType result =
        (Types.DecimalType) CalciteTypeMapper.toIcebergType(decimalSpec(10, 2));
    assertEquals(10, result.precision());
    assertEquals(2, result.scale());
  }

  @Test
  void decimalWithDefaultPrecisionAndScale() {
    Types.DecimalType result =
        (Types.DecimalType) CalciteTypeMapper.toIcebergType(decimalSpec(-1, -1));
    assertEquals(38, result.precision());
    assertEquals(0, result.scale());
  }

  @Test
  void varcharMapsToIcebergString() {
    assertEquals(
        Types.StringType.get(), CalciteTypeMapper.toIcebergType(spec(SqlTypeName.VARCHAR)));
  }

  @Test
  void charMapsToIcebergString() {
    assertEquals(Types.StringType.get(), CalciteTypeMapper.toIcebergType(spec(SqlTypeName.CHAR)));
  }

  @Test
  void binaryMapsToIcebergBinary() {
    assertEquals(Types.BinaryType.get(), CalciteTypeMapper.toIcebergType(spec(SqlTypeName.BINARY)));
  }

  @Test
  void varbinaryMapsToIcebergBinary() {
    assertEquals(
        Types.BinaryType.get(), CalciteTypeMapper.toIcebergType(spec(SqlTypeName.VARBINARY)));
  }

  @Test
  void dateMapsToIcebergDate() {
    assertEquals(Types.DateType.get(), CalciteTypeMapper.toIcebergType(spec(SqlTypeName.DATE)));
  }

  @Test
  void timeMapsToIcebergTime() {
    assertEquals(Types.TimeType.get(), CalciteTypeMapper.toIcebergType(spec(SqlTypeName.TIME)));
  }

  @Test
  void timestampMapsToIcebergTimestampWithoutZone() {
    assertEquals(
        Types.TimestampType.withoutZone(),
        CalciteTypeMapper.toIcebergType(spec(SqlTypeName.TIMESTAMP)));
  }

  @Test
  void timestampWithLocalTimeZoneMapsToIcebergTimestampWithZone() {
    // The Calcite parser encodes this as SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE.
    // Internally the identifier name is the enum constant name "TIMESTAMP_WITH_LOCAL_TIME_ZONE".
    assertEquals(
        Types.TimestampType.withZone(),
        CalciteTypeMapper.toIcebergType(spec(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)));
  }

  @Test
  void unsupportedTypeThrowsSqlExecutionException() {
    SqlExecutionException ex =
        assertThrows(
            SqlExecutionException.class,
            () -> CalciteTypeMapper.toIcebergType(spec(SqlTypeName.INTERVAL_DAY)));
    assertTrue(ex.getMessage().contains("Unsupported SQL type"));
  }
}
