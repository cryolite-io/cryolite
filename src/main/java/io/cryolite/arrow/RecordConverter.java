package io.cryolite.arrow;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Converts Arrow {@link VectorSchemaRoot} rows to Iceberg {@link GenericRecord} objects lazily.
 *
 * <p>This is an internal helper used by {@link io.cryolite.data.TableReader#readRecords()} to
 * provide a convenience record-based API on top of the Arrow batch pipeline. All data flows through
 * the Arrow vectorized reader first; this converter creates one record at a time on demand.
 *
 * <p><b>Usage pattern (lazy, per-row):</b>
 *
 * <ol>
 *   <li>When a new batch arrives, call {@link #buildAccessors} once to resolve vector references
 *       and type metadata for all columns. This avoids repeated HashMap lookups inside Arrow's
 *       {@link VectorSchemaRoot}.
 *   <li>For each row the caller needs, call {@link #convertRow} with the cached accessors and the
 *       row index. This creates exactly one {@link GenericRecord} per call – no bulk
 *       materialization, no wasted allocations on early termination.
 * </ol>
 *
 * <p>Supported types match {@link SchemaConverter}: BOOLEAN, INTEGER, LONG, FLOAT, DOUBLE, STRING,
 * BINARY, DATE, TIME, TIMESTAMP, TIMESTAMP_NANO, DECIMAL, UUID, FIXED.
 *
 * @since 0.1.0
 */
public final class RecordConverter {

  private static final long MICROS_PER_SECOND = 1_000_000L;
  private static final long NANOS_PER_MICRO = 1_000L;
  private static final long NANOS_PER_SECOND = 1_000_000_000L;

  private RecordConverter() {
    // Utility class - no instantiation
  }

  /**
   * Pre-resolved field metadata for efficient batch-level conversion.
   *
   * <p>Caches the Arrow vector reference, field name, and Iceberg primitive type so that per-row
   * conversion only needs to extract values without repeated lookups. Build once per batch via
   * {@link #buildAccessors}, then pass to {@link #convertRow} for each row index.
   *
   * @param vector the Arrow vector for this field
   * @param name the field name
   * @param type the Iceberg primitive type
   */
  public record FieldAccessor(FieldVector vector, String name, Type.PrimitiveType type) {}

  /**
   * Builds field accessors for a batch by resolving vectors and types once.
   *
   * <p>Callers should invoke this once when a new batch arrives, then reuse the returned list for
   * all rows in that batch via {@link #convertRow}. This is the key to avoiding repeated HashMap
   * lookups inside the Arrow {@link VectorSchemaRoot}.
   *
   * @param icebergSchema the Iceberg schema
   * @param root the Arrow batch
   * @return list of pre-resolved field accessors, one per schema column
   */
  public static List<FieldAccessor> buildAccessors(Schema icebergSchema, VectorSchemaRoot root) {
    List<Types.NestedField> columns = icebergSchema.columns();
    List<FieldAccessor> accessors = new ArrayList<>(columns.size());

    for (Types.NestedField field : columns) {
      FieldVector vector = root.getVector(field.name());
      Type.PrimitiveType primitiveType = field.type().asPrimitiveType();
      accessors.add(new FieldAccessor(vector, field.name(), primitiveType));
    }

    return accessors;
  }

  /**
   * Converts a single row using pre-resolved field accessors.
   *
   * <p>Creates exactly one {@link GenericRecord} from the given row index. Accessors must have been
   * built for the current batch via {@link #buildAccessors} before calling this method.
   *
   * @param icebergSchema the Iceberg schema for record creation
   * @param accessors the pre-resolved field accessors for the current batch
   * @param rowIndex the zero-based row index within the current batch
   * @return a new GenericRecord with the row's values
   */
  public static GenericRecord convertRow(
      Schema icebergSchema, List<FieldAccessor> accessors, int rowIndex) {
    GenericRecord record = GenericRecord.create(icebergSchema);

    for (FieldAccessor accessor : accessors) {
      if (accessor.vector().isNull(rowIndex)) {
        record.setField(accessor.name(), null);
      } else {
        record.setField(
            accessor.name(), extractValue(accessor.vector(), rowIndex, accessor.type()));
      }
    }

    return record;
  }

  /**
   * Extracts a typed Java value from an Arrow vector at the given row index.
   *
   * <p>The returned Java types match what Iceberg's GenericRecord expects:
   *
   * <ul>
   *   <li>BOOLEAN → {@link Boolean}
   *   <li>INTEGER → {@link Integer}
   *   <li>LONG → {@link Long}
   *   <li>FLOAT → {@link Float}
   *   <li>DOUBLE → {@link Double}
   *   <li>STRING → {@link String}
   *   <li>BINARY → {@link ByteBuffer}
   *   <li>DATE → {@link LocalDate}
   *   <li>TIME → {@link LocalTime}
   *   <li>TIMESTAMP (with TZ) → {@link OffsetDateTime}
   *   <li>TIMESTAMP (without TZ) → {@link LocalDateTime}
   *   <li>DECIMAL → {@link BigDecimal}
   *   <li>UUID → {@link UUID}
   *   <li>FIXED → {@link ByteBuffer}
   * </ul>
   */
  static Object extractValue(FieldVector vector, int rowIndex, Type.PrimitiveType type) {
    return switch (type.typeId()) {
      case BOOLEAN -> ((BitVector) vector).get(rowIndex) != 0;
      case INTEGER -> ((IntVector) vector).get(rowIndex);
      case LONG -> ((BigIntVector) vector).get(rowIndex);
      case FLOAT -> ((Float4Vector) vector).get(rowIndex);
      case DOUBLE -> ((Float8Vector) vector).get(rowIndex);
      case STRING -> new String(((VarCharVector) vector).get(rowIndex));
      case BINARY -> ByteBuffer.wrap(((VarBinaryVector) vector).get(rowIndex));
      case DATE -> LocalDate.ofEpochDay(((DateDayVector) vector).get(rowIndex));
      case TIME ->
          LocalTime.ofNanoOfDay(((TimeMicroVector) vector).get(rowIndex) * NANOS_PER_MICRO);
      case TIMESTAMP -> extractTimestamp((Types.TimestampType) type, vector, rowIndex);
      case TIMESTAMP_NANO -> extractTimestampNano((Types.TimestampNanoType) type, vector, rowIndex);
      case DECIMAL -> ((DecimalVector) vector).getObject(rowIndex);
      case UUID -> extractUuid((FixedSizeBinaryVector) vector, rowIndex);
      case FIXED -> ByteBuffer.wrap(((FixedSizeBinaryVector) vector).get(rowIndex));
      default -> throw new UnsupportedOperationException("Unsupported type: " + type.typeId());
    };
  }

  private static Object extractTimestamp(
      Types.TimestampType tsType, FieldVector vector, int rowIndex) {
    if (tsType.shouldAdjustToUTC()) {
      long micros = ((TimeStampMicroTZVector) vector).get(rowIndex);
      return microsToOffsetDateTime(micros);
    }
    long micros = ((TimeStampMicroVector) vector).get(rowIndex);
    return microsToLocalDateTime(micros);
  }

  private static Object extractTimestampNano(
      Types.TimestampNanoType tsType, FieldVector vector, int rowIndex) {
    if (tsType.shouldAdjustToUTC()) {
      long nanos = ((TimeStampNanoTZVector) vector).get(rowIndex);
      return nanosToOffsetDateTime(nanos);
    }
    long nanos = ((TimeStampNanoVector) vector).get(rowIndex);
    return nanosToLocalDateTime(nanos);
  }

  private static OffsetDateTime microsToOffsetDateTime(long micros) {
    long epochSecond = Math.floorDiv(micros, MICROS_PER_SECOND);
    long nanoAdjustment = Math.floorMod(micros, MICROS_PER_SECOND) * NANOS_PER_MICRO;
    return Instant.ofEpochSecond(epochSecond, nanoAdjustment).atOffset(ZoneOffset.UTC);
  }

  private static LocalDateTime microsToLocalDateTime(long micros) {
    return microsToOffsetDateTime(micros).toLocalDateTime();
  }

  private static OffsetDateTime nanosToOffsetDateTime(long nanos) {
    long epochSecond = Math.floorDiv(nanos, NANOS_PER_SECOND);
    long nanoAdjustment = Math.floorMod(nanos, NANOS_PER_SECOND);
    return Instant.ofEpochSecond(epochSecond, nanoAdjustment).atOffset(ZoneOffset.UTC);
  }

  private static LocalDateTime nanosToLocalDateTime(long nanos) {
    return nanosToOffsetDateTime(nanos).toLocalDateTime();
  }

  private static UUID extractUuid(FixedSizeBinaryVector vector, int rowIndex) {
    byte[] bytes = vector.get(rowIndex);
    ByteBuffer bb = ByteBuffer.wrap(bytes);
    return new UUID(bb.getLong(), bb.getLong());
  }
}
