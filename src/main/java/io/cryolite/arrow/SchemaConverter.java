package io.cryolite.arrow;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Converts Apache Iceberg schemas to Apache Arrow schemas.
 *
 * <p>This converter maps Iceberg primitive types to their corresponding Arrow types, preserving
 * nullability semantics. The type mapping follows the same conventions as Iceberg's own
 * ArrowSchemaUtil to ensure compatibility.
 *
 * <p>Supported Iceberg types:
 *
 * <ul>
 *   <li>BOOLEAN → Arrow Bool
 *   <li>INTEGER → Arrow Int(32, signed)
 *   <li>LONG → Arrow Int(64, signed)
 *   <li>FLOAT → Arrow FloatingPoint(SINGLE)
 *   <li>DOUBLE → Arrow FloatingPoint(DOUBLE)
 *   <li>STRING → Arrow Utf8
 *   <li>BINARY → Arrow Binary
 *   <li>DATE → Arrow Date(DAY)
 *   <li>TIME → Arrow Time(MICROSECOND, 64)
 *   <li>TIMESTAMP → Arrow Timestamp(MICROSECOND, timezone)
 *   <li>TIMESTAMP_NANO → Arrow Timestamp(NANOSECOND, timezone)
 *   <li>DECIMAL → Arrow Decimal(precision, scale, 128)
 *   <li>UUID → Arrow FixedSizeBinary(16)
 *   <li>FIXED → Arrow FixedSizeBinary(length)
 * </ul>
 */
public final class SchemaConverter {

  private SchemaConverter() {
    // Utility class - no instantiation
  }

  /**
   * Converts an Iceberg schema to an Arrow schema.
   *
   * @param icebergSchema the Iceberg schema to convert
   * @return the corresponding Arrow schema
   * @throws UnsupportedOperationException if the schema contains unsupported types
   */
  public static Schema toArrow(org.apache.iceberg.Schema icebergSchema) {
    List<Field> arrowFields = new ArrayList<>(icebergSchema.columns().size());

    for (Types.NestedField icebergField : icebergSchema.columns()) {
      arrowFields.add(convertField(icebergField));
    }

    return new Schema(arrowFields);
  }

  /**
   * Converts a single Iceberg field to an Arrow field.
   *
   * @param icebergField the Iceberg field to convert
   * @return the corresponding Arrow field
   * @throws UnsupportedOperationException if the field type is not supported
   */
  static Field convertField(Types.NestedField icebergField) {
    ArrowType arrowType = convertPrimitiveType(icebergField.type().asPrimitiveType());
    FieldType fieldType = new FieldType(icebergField.isOptional(), arrowType, null);
    return new Field(icebergField.name(), fieldType, null);
  }

  /**
   * Maps an Iceberg primitive type to the corresponding Arrow type.
   *
   * @param primitive the Iceberg primitive type
   * @return the corresponding Arrow type
   * @throws UnsupportedOperationException if the type is not supported
   */
  static ArrowType convertPrimitiveType(Type.PrimitiveType primitive) {
    return switch (primitive.typeId()) {
      case BOOLEAN -> ArrowType.Bool.INSTANCE;
      case INTEGER -> new ArrowType.Int(Integer.SIZE, true);
      case LONG -> new ArrowType.Int(Long.SIZE, true);
      case FLOAT -> new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
      case DOUBLE -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
      case STRING -> ArrowType.Utf8.INSTANCE;
      case BINARY -> ArrowType.Binary.INSTANCE;
      case DATE -> new ArrowType.Date(DateUnit.DAY);
      case TIME -> new ArrowType.Time(TimeUnit.MICROSECOND, Long.SIZE);
      case TIMESTAMP ->
          new ArrowType.Timestamp(
              TimeUnit.MICROSECOND,
              ((Types.TimestampType) primitive).shouldAdjustToUTC() ? "UTC" : null);
      case TIMESTAMP_NANO ->
          new ArrowType.Timestamp(
              TimeUnit.NANOSECOND,
              ((Types.TimestampNanoType) primitive).shouldAdjustToUTC() ? "UTC" : null);
      case DECIMAL -> {
        Types.DecimalType decimalType = (Types.DecimalType) primitive;
        yield new ArrowType.Decimal(decimalType.precision(), decimalType.scale(), 128);
      }
      case UUID -> new ArrowType.FixedSizeBinary(16);
      case FIXED -> {
        Types.FixedType fixedType = (Types.FixedType) primitive;
        yield new ArrowType.FixedSizeBinary(fixedType.length());
      }
      default ->
          throw new UnsupportedOperationException(
              "Unsupported Iceberg type: " + primitive.typeId());
    };
  }
}
