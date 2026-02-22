package io.cryolite.arrow;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
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
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link RecordConverter}. */
class RecordConverterTest {

  @Test
  void testConvertBoolean() {
    org.apache.iceberg.Schema iceSchema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "flag", Types.BooleanType.get()));
    Schema arrowSchema = SchemaConverter.toArrow(iceSchema);

    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, alloc)) {
      BitVector vec = (BitVector) root.getVector("flag");
      vec.allocateNew(2);
      vec.set(0, 1);
      vec.set(1, 0);
      vec.setValueCount(2);
      root.setRowCount(2);

      var accessors = RecordConverter.buildAccessors(iceSchema, root);
      GenericRecord row0 = RecordConverter.convertRow(iceSchema, accessors, 0);
      GenericRecord row1 = RecordConverter.convertRow(iceSchema, accessors, 1);
      assertEquals(true, row0.getField("flag"));
      assertEquals(false, row1.getField("flag"));
    }
  }

  @Test
  void testConvertInteger() {
    org.apache.iceberg.Schema iceSchema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "val", Types.IntegerType.get()));
    Schema arrowSchema = SchemaConverter.toArrow(iceSchema);

    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, alloc)) {
      IntVector vec = (IntVector) root.getVector("val");
      vec.allocateNew(1);
      vec.set(0, 42);
      vec.setValueCount(1);
      root.setRowCount(1);

      var accessors = RecordConverter.buildAccessors(iceSchema, root);
      GenericRecord record = RecordConverter.convertRow(iceSchema, accessors, 0);
      assertEquals(42, record.getField("val"));
    }
  }

  @Test
  void testConvertLong() {
    org.apache.iceberg.Schema iceSchema =
        new org.apache.iceberg.Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    Schema arrowSchema = SchemaConverter.toArrow(iceSchema);

    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, alloc)) {
      BigIntVector vec = (BigIntVector) root.getVector("id");
      vec.allocateNew(1);
      vec.set(0, 123456789L);
      vec.setValueCount(1);
      root.setRowCount(1);

      var accessors = RecordConverter.buildAccessors(iceSchema, root);
      GenericRecord record = RecordConverter.convertRow(iceSchema, accessors, 0);
      assertEquals(123456789L, record.getField("id"));
    }
  }

  @Test
  void testConvertFloat() {
    org.apache.iceberg.Schema iceSchema =
        new org.apache.iceberg.Schema(Types.NestedField.required(1, "val", Types.FloatType.get()));
    Schema arrowSchema = SchemaConverter.toArrow(iceSchema);

    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, alloc)) {
      Float4Vector vec = (Float4Vector) root.getVector("val");
      vec.allocateNew(1);
      vec.set(0, 3.14f);
      vec.setValueCount(1);
      root.setRowCount(1);

      var accessors = RecordConverter.buildAccessors(iceSchema, root);
      GenericRecord record = RecordConverter.convertRow(iceSchema, accessors, 0);
      assertEquals(3.14f, (float) record.getField("val"), 0.001f);
    }
  }

  @Test
  void testConvertDouble() {
    org.apache.iceberg.Schema iceSchema =
        new org.apache.iceberg.Schema(Types.NestedField.required(1, "val", Types.DoubleType.get()));
    Schema arrowSchema = SchemaConverter.toArrow(iceSchema);

    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, alloc)) {
      Float8Vector vec = (Float8Vector) root.getVector("val");
      vec.allocateNew(1);
      vec.set(0, 2.718281828);
      vec.setValueCount(1);
      root.setRowCount(1);

      var accessors = RecordConverter.buildAccessors(iceSchema, root);
      GenericRecord record = RecordConverter.convertRow(iceSchema, accessors, 0);
      assertEquals(2.718281828, (double) record.getField("val"), 0.0000001);
    }
  }

  @Test
  void testConvertString() {
    org.apache.iceberg.Schema iceSchema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "name", Types.StringType.get()));
    Schema arrowSchema = SchemaConverter.toArrow(iceSchema);

    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, alloc)) {
      VarCharVector vec = (VarCharVector) root.getVector("name");
      vec.allocateNew();
      vec.set(0, "hello".getBytes());
      vec.setValueCount(1);
      root.setRowCount(1);

      var accessors = RecordConverter.buildAccessors(iceSchema, root);
      GenericRecord record = RecordConverter.convertRow(iceSchema, accessors, 0);
      assertEquals("hello", record.getField("name"));
    }
  }

  @Test
  void testConvertBinary() {
    org.apache.iceberg.Schema iceSchema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "data", Types.BinaryType.get()));
    Schema arrowSchema = SchemaConverter.toArrow(iceSchema);

    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, alloc)) {
      VarBinaryVector vec = (VarBinaryVector) root.getVector("data");
      vec.allocateNew();
      vec.set(0, new byte[] {1, 2, 3});
      vec.setValueCount(1);
      root.setRowCount(1);

      var accessors = RecordConverter.buildAccessors(iceSchema, root);
      GenericRecord record = RecordConverter.convertRow(iceSchema, accessors, 0);
      assertEquals(ByteBuffer.wrap(new byte[] {1, 2, 3}), record.getField("data"));
    }
  }

  @Test
  void testConvertDate() {
    org.apache.iceberg.Schema iceSchema =
        new org.apache.iceberg.Schema(Types.NestedField.required(1, "day", Types.DateType.get()));
    Schema arrowSchema = SchemaConverter.toArrow(iceSchema);

    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, alloc)) {
      DateDayVector vec = (DateDayVector) root.getVector("day");
      vec.allocateNew(1);
      int epochDay = (int) LocalDate.of(2025, 6, 15).toEpochDay();
      vec.set(0, epochDay);
      vec.setValueCount(1);
      root.setRowCount(1);

      var accessors = RecordConverter.buildAccessors(iceSchema, root);
      GenericRecord record = RecordConverter.convertRow(iceSchema, accessors, 0);
      assertEquals(LocalDate.of(2025, 6, 15), record.getField("day"));
    }
  }

  @Test
  void testConvertTime() {
    org.apache.iceberg.Schema iceSchema =
        new org.apache.iceberg.Schema(Types.NestedField.required(1, "time", Types.TimeType.get()));
    Schema arrowSchema = SchemaConverter.toArrow(iceSchema);

    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, alloc)) {
      TimeMicroVector vec = (TimeMicroVector) root.getVector("time");
      vec.allocateNew(1);
      long micros = 13L * 3_600_000_000L + 45L * 60_000_000L; // 13:45:00
      vec.set(0, micros);
      vec.setValueCount(1);
      root.setRowCount(1);

      var accessors = RecordConverter.buildAccessors(iceSchema, root);
      GenericRecord record = RecordConverter.convertRow(iceSchema, accessors, 0);
      assertEquals(LocalTime.of(13, 45, 0), record.getField("time"));
    }
  }

  @Test
  void testConvertTimestampWithTimezone() {
    org.apache.iceberg.Schema iceSchema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "ts", Types.TimestampType.withZone()));
    Schema arrowSchema = SchemaConverter.toArrow(iceSchema);

    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, alloc)) {
      TimeStampMicroTZVector vec = (TimeStampMicroTZVector) root.getVector("ts");
      vec.allocateNew(1);
      // 2025-01-15T10:30:00Z = 1736937000 epoch seconds
      long micros = 1_736_937_000_000_000L;
      vec.set(0, micros);
      vec.setValueCount(1);
      root.setRowCount(1);

      var accessors = RecordConverter.buildAccessors(iceSchema, root);
      GenericRecord record = RecordConverter.convertRow(iceSchema, accessors, 0);
      OffsetDateTime expected = OffsetDateTime.of(2025, 1, 15, 10, 30, 0, 0, ZoneOffset.UTC);
      assertEquals(expected, record.getField("ts"));
    }
  }

  @Test
  void testConvertTimestampWithoutTimezone() {
    org.apache.iceberg.Schema iceSchema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "ts", Types.TimestampType.withoutZone()));
    Schema arrowSchema = SchemaConverter.toArrow(iceSchema);

    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, alloc)) {
      TimeStampMicroVector vec = (TimeStampMicroVector) root.getVector("ts");
      vec.allocateNew(1);
      long micros = 1_736_937_000_000_000L;
      vec.set(0, micros);
      vec.setValueCount(1);
      root.setRowCount(1);

      var accessors = RecordConverter.buildAccessors(iceSchema, root);
      GenericRecord record = RecordConverter.convertRow(iceSchema, accessors, 0);
      LocalDateTime expected = LocalDateTime.of(2025, 1, 15, 10, 30, 0);
      assertEquals(expected, record.getField("ts"));
    }
  }

  @Test
  void testConvertTimestampNanoWithTimezone() {
    org.apache.iceberg.Schema iceSchema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "ts", Types.TimestampNanoType.withZone()));
    Schema arrowSchema = SchemaConverter.toArrow(iceSchema);

    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, alloc)) {
      TimeStampNanoTZVector vec = (TimeStampNanoTZVector) root.getVector("ts");
      vec.allocateNew(1);
      long nanos = 1_736_937_000_000_000_000L;
      vec.set(0, nanos);
      vec.setValueCount(1);
      root.setRowCount(1);

      var accessors = RecordConverter.buildAccessors(iceSchema, root);
      GenericRecord record = RecordConverter.convertRow(iceSchema, accessors, 0);
      OffsetDateTime expected = OffsetDateTime.of(2025, 1, 15, 10, 30, 0, 0, ZoneOffset.UTC);
      assertEquals(expected, record.getField("ts"));
    }
  }

  @Test
  void testConvertTimestampNanoWithoutTimezone() {
    org.apache.iceberg.Schema iceSchema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "ts", Types.TimestampNanoType.withoutZone()));
    Schema arrowSchema = SchemaConverter.toArrow(iceSchema);

    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, alloc)) {
      TimeStampNanoVector vec = (TimeStampNanoVector) root.getVector("ts");
      vec.allocateNew(1);
      long nanos = 1_736_937_000_000_000_000L;
      vec.set(0, nanos);
      vec.setValueCount(1);
      root.setRowCount(1);

      var accessors = RecordConverter.buildAccessors(iceSchema, root);
      GenericRecord record = RecordConverter.convertRow(iceSchema, accessors, 0);
      LocalDateTime expected = LocalDateTime.of(2025, 1, 15, 10, 30, 0);
      assertEquals(expected, record.getField("ts"));
    }
  }

  @Test
  void testConvertDecimal() {
    org.apache.iceberg.Schema iceSchema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "amount", Types.DecimalType.of(10, 2)));
    Schema arrowSchema = SchemaConverter.toArrow(iceSchema);

    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, alloc)) {
      DecimalVector vec = (DecimalVector) root.getVector("amount");
      vec.allocateNew(1);
      vec.set(0, new BigDecimal("123.45"));
      vec.setValueCount(1);
      root.setRowCount(1);

      var accessors = RecordConverter.buildAccessors(iceSchema, root);
      GenericRecord record = RecordConverter.convertRow(iceSchema, accessors, 0);
      assertEquals(new BigDecimal("123.45"), record.getField("amount"));
    }
  }

  @Test
  void testConvertUuid() {
    org.apache.iceberg.Schema iceSchema =
        new org.apache.iceberg.Schema(Types.NestedField.required(1, "uid", Types.UUIDType.get()));
    Schema arrowSchema = SchemaConverter.toArrow(iceSchema);

    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, alloc)) {
      FixedSizeBinaryVector vec = (FixedSizeBinaryVector) root.getVector("uid");
      vec.allocateNew(1);
      UUID expected = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
      ByteBuffer bb = ByteBuffer.allocate(16);
      bb.putLong(expected.getMostSignificantBits());
      bb.putLong(expected.getLeastSignificantBits());
      vec.set(0, bb.array());
      vec.setValueCount(1);
      root.setRowCount(1);

      var accessors = RecordConverter.buildAccessors(iceSchema, root);
      GenericRecord record = RecordConverter.convertRow(iceSchema, accessors, 0);
      assertEquals(expected, record.getField("uid"));
    }
  }

  @Test
  void testConvertFixed() {
    org.apache.iceberg.Schema iceSchema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "hash", Types.FixedType.ofLength(4)));
    Schema arrowSchema = SchemaConverter.toArrow(iceSchema);

    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, alloc)) {
      FixedSizeBinaryVector vec = (FixedSizeBinaryVector) root.getVector("hash");
      vec.allocateNew(1);
      vec.set(0, new byte[] {0x0A, 0x0B, 0x0C, 0x0D});
      vec.setValueCount(1);
      root.setRowCount(1);

      var accessors = RecordConverter.buildAccessors(iceSchema, root);
      GenericRecord record = RecordConverter.convertRow(iceSchema, accessors, 0);
      assertEquals(ByteBuffer.wrap(new byte[] {0x0A, 0x0B, 0x0C, 0x0D}), record.getField("hash"));
    }
  }

  @Test
  void testConvertNullValues() {
    org.apache.iceberg.Schema iceSchema =
        new org.apache.iceberg.Schema(
            Types.NestedField.optional(1, "name", Types.StringType.get()),
            Types.NestedField.optional(2, "id", Types.LongType.get()));
    Schema arrowSchema = SchemaConverter.toArrow(iceSchema);

    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, alloc)) {
      VarCharVector nameVec = (VarCharVector) root.getVector("name");
      BigIntVector idVec = (BigIntVector) root.getVector("id");
      nameVec.allocateNew();
      idVec.allocateNew(2);
      // Row 0: name=null, id=1
      nameVec.setNull(0);
      idVec.set(0, 1L);
      // Row 1: name="Alice", id=null
      nameVec.set(1, "Alice".getBytes());
      idVec.setNull(1);
      nameVec.setValueCount(2);
      idVec.setValueCount(2);
      root.setRowCount(2);

      var accessors = RecordConverter.buildAccessors(iceSchema, root);
      GenericRecord row0 = RecordConverter.convertRow(iceSchema, accessors, 0);
      GenericRecord row1 = RecordConverter.convertRow(iceSchema, accessors, 1);
      assertNull(row0.getField("name"));
      assertEquals(1L, row0.getField("id"));
      assertEquals("Alice", row1.getField("name"));
      assertNull(row1.getField("id"));
    }
  }

  @Test
  void testBatchConversionMultipleRows() {
    org.apache.iceberg.Schema iceSchema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));
    Schema arrowSchema = SchemaConverter.toArrow(iceSchema);

    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, alloc)) {
      BigIntVector idVec = (BigIntVector) root.getVector("id");
      VarCharVector nameVec = (VarCharVector) root.getVector("name");
      idVec.allocateNew(3);
      nameVec.allocateNew();
      for (int i = 0; i < 3; i++) {
        idVec.set(i, (long) i);
        nameVec.set(i, ("row-" + i).getBytes());
      }
      idVec.setValueCount(3);
      nameVec.setValueCount(3);
      root.setRowCount(3);

      var accessors = RecordConverter.buildAccessors(iceSchema, root);
      for (int i = 0; i < 3; i++) {
        GenericRecord record = RecordConverter.convertRow(iceSchema, accessors, i);
        assertEquals((long) i, record.getField("id"));
        assertEquals("row-" + i, record.getField("name"));
      }
    }
  }

  @Test
  void testEmptyBatchBuildAccessorsWithZeroRows() {
    org.apache.iceberg.Schema iceSchema =
        new org.apache.iceberg.Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    Schema arrowSchema = SchemaConverter.toArrow(iceSchema);

    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, alloc)) {
      root.setRowCount(0);

      var accessors = RecordConverter.buildAccessors(iceSchema, root);
      assertEquals(1, accessors.size());
      assertEquals(0, root.getRowCount());
    }
  }

  @Test
  void testBuildAccessorsResolvesAllFields() {
    org.apache.iceberg.Schema iceSchema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "flag", Types.BooleanType.get()));
    Schema arrowSchema = SchemaConverter.toArrow(iceSchema);

    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, alloc)) {
      root.allocateNew();
      root.setRowCount(0);

      var accessors = RecordConverter.buildAccessors(iceSchema, root);
      assertEquals(3, accessors.size());
      assertEquals("id", accessors.get(0).name());
      assertEquals("name", accessors.get(1).name());
      assertEquals("flag", accessors.get(2).name());
    }
  }
}
