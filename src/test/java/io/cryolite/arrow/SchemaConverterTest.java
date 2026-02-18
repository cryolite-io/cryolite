package io.cryolite.arrow;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link SchemaConverter}. */
class SchemaConverterTest {

  @Test
  void testConvertBooleanType() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "flag", Types.BooleanType.get()));
    Schema arrowSchema = SchemaConverter.toArrow(schema);

    assertEquals(1, arrowSchema.getFields().size());
    Field field = arrowSchema.getFields().get(0);
    assertEquals("flag", field.getName());
    assertFalse(field.isNullable());
    assertInstanceOf(ArrowType.Bool.class, field.getType());
  }

  @Test
  void testConvertIntegerType() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "count", Types.IntegerType.get()));
    Schema arrowSchema = SchemaConverter.toArrow(schema);

    Field field = arrowSchema.getFields().get(0);
    ArrowType.Int intType = (ArrowType.Int) field.getType();
    assertEquals(Integer.SIZE, intType.getBitWidth());
    assertTrue(intType.getIsSigned());
  }

  @Test
  void testConvertLongType() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    Schema arrowSchema = SchemaConverter.toArrow(schema);

    Field field = arrowSchema.getFields().get(0);
    ArrowType.Int intType = (ArrowType.Int) field.getType();
    assertEquals(Long.SIZE, intType.getBitWidth());
    assertTrue(intType.getIsSigned());
  }

  @Test
  void testConvertFloatType() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "value", Types.FloatType.get()));
    Schema arrowSchema = SchemaConverter.toArrow(schema);

    Field field = arrowSchema.getFields().get(0);
    ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) field.getType();
    assertEquals(FloatingPointPrecision.SINGLE, fpType.getPrecision());
  }

  @Test
  void testConvertDoubleType() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "price", Types.DoubleType.get()));
    Schema arrowSchema = SchemaConverter.toArrow(schema);

    Field field = arrowSchema.getFields().get(0);
    ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) field.getType();
    assertEquals(FloatingPointPrecision.DOUBLE, fpType.getPrecision());
  }

  @Test
  void testConvertStringType() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            Types.NestedField.optional(1, "name", Types.StringType.get()));
    Schema arrowSchema = SchemaConverter.toArrow(schema);

    Field field = arrowSchema.getFields().get(0);
    assertTrue(field.isNullable());
    assertInstanceOf(ArrowType.Utf8.class, field.getType());
  }

  @Test
  void testConvertBinaryType() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "data", Types.BinaryType.get()));
    Schema arrowSchema = SchemaConverter.toArrow(schema);

    assertInstanceOf(ArrowType.Binary.class, arrowSchema.getFields().get(0).getType());
  }

  @Test
  void testConvertDateType() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(Types.NestedField.required(1, "day", Types.DateType.get()));
    Schema arrowSchema = SchemaConverter.toArrow(schema);

    ArrowType.Date dateType = (ArrowType.Date) arrowSchema.getFields().get(0).getType();
    assertEquals(DateUnit.DAY, dateType.getUnit());
  }

  @Test
  void testConvertTimeType() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(Types.NestedField.required(1, "time", Types.TimeType.get()));
    Schema arrowSchema = SchemaConverter.toArrow(schema);

    ArrowType.Time timeType = (ArrowType.Time) arrowSchema.getFields().get(0).getType();
    assertEquals(TimeUnit.MICROSECOND, timeType.getUnit());
    assertEquals(Long.SIZE, timeType.getBitWidth());
  }

  @Test
  void testConvertTimestampWithTimezone() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "ts", Types.TimestampType.withZone()));
    Schema arrowSchema = SchemaConverter.toArrow(schema);

    ArrowType.Timestamp tsType = (ArrowType.Timestamp) arrowSchema.getFields().get(0).getType();
    assertEquals(TimeUnit.MICROSECOND, tsType.getUnit());
    assertEquals("UTC", tsType.getTimezone());
  }

  @Test
  void testConvertTimestampWithoutTimezone() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "ts", Types.TimestampType.withoutZone()));
    Schema arrowSchema = SchemaConverter.toArrow(schema);

    ArrowType.Timestamp tsType = (ArrowType.Timestamp) arrowSchema.getFields().get(0).getType();
    assertEquals(TimeUnit.MICROSECOND, tsType.getUnit());
    assertNull(tsType.getTimezone());
  }

  @Test
  void testConvertTimestampNanoWithTimezone() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "ts", Types.TimestampNanoType.withZone()));
    Schema arrowSchema = SchemaConverter.toArrow(schema);

    ArrowType.Timestamp tsType = (ArrowType.Timestamp) arrowSchema.getFields().get(0).getType();
    assertEquals(TimeUnit.NANOSECOND, tsType.getUnit());
    assertEquals("UTC", tsType.getTimezone());
  }

  @Test
  void testConvertDecimalType() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "amount", Types.DecimalType.of(10, 2)));
    Schema arrowSchema = SchemaConverter.toArrow(schema);

    ArrowType.Decimal decType = (ArrowType.Decimal) arrowSchema.getFields().get(0).getType();
    assertEquals(10, decType.getPrecision());
    assertEquals(2, decType.getScale());
    assertEquals(128, decType.getBitWidth());
  }

  @Test
  void testConvertUuidType() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(Types.NestedField.required(1, "uuid", Types.UUIDType.get()));
    Schema arrowSchema = SchemaConverter.toArrow(schema);

    ArrowType.FixedSizeBinary fixedType =
        (ArrowType.FixedSizeBinary) arrowSchema.getFields().get(0).getType();
    assertEquals(16, fixedType.getByteWidth());
  }

  @Test
  void testConvertFixedType() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "hash", Types.FixedType.ofLength(32)));
    Schema arrowSchema = SchemaConverter.toArrow(schema);

    ArrowType.FixedSizeBinary fixedType =
        (ArrowType.FixedSizeBinary) arrowSchema.getFields().get(0).getType();
    assertEquals(32, fixedType.getByteWidth());
  }

  @Test
  void testNullableField() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            Types.NestedField.optional(1, "optional_field", Types.StringType.get()));
    Schema arrowSchema = SchemaConverter.toArrow(schema);

    assertTrue(arrowSchema.getFields().get(0).isNullable());
  }

  @Test
  void testRequiredField() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "required_field", Types.LongType.get()));
    Schema arrowSchema = SchemaConverter.toArrow(schema);

    assertFalse(arrowSchema.getFields().get(0).isNullable());
  }

  @Test
  void testMultiColumnSchema() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "active", Types.BooleanType.get()));
    Schema arrowSchema = SchemaConverter.toArrow(schema);

    assertEquals(3, arrowSchema.getFields().size());
    assertEquals("id", arrowSchema.getFields().get(0).getName());
    assertEquals("name", arrowSchema.getFields().get(1).getName());
    assertEquals("active", arrowSchema.getFields().get(2).getName());
    assertFalse(arrowSchema.getFields().get(0).isNullable());
    assertTrue(arrowSchema.getFields().get(1).isNullable());
    assertFalse(arrowSchema.getFields().get(2).isNullable());
  }

  @Test
  void testEmptySchema() {
    org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema();
    Schema arrowSchema = SchemaConverter.toArrow(schema);

    assertTrue(arrowSchema.getFields().isEmpty());
  }
}
