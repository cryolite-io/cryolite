package io.cryolite.filter;

import static org.junit.jupiter.api.Assertions.*;

import io.cryolite.arrow.SchemaConverter;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ArrowBatchFilter}.
 *
 * <p>Verifies that filtering produces correct output batches with matching rows only.
 */
class ArrowBatchFilterTest {

  private static final org.apache.iceberg.Schema ICE_SCHEMA =
      new org.apache.iceberg.Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required(2, "name", Types.StringType.get()));

  private static final Schema ARROW_SCHEMA = SchemaConverter.toArrow(ICE_SCHEMA);

  @Test
  void filtersRowsMatchingPredicate() {
    ComparisonPredicate pred = new ComparisonPredicate("id", ComparisonOperator.GREATER_THAN, 15L);
    ArrowBatchFilter filter = new ArrowBatchFilter(pred);

    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot source = VectorSchemaRoot.create(ARROW_SCHEMA, alloc)) {
      BigIntVector idVec = (BigIntVector) source.getVector("id");
      VarCharVector nameVec = (VarCharVector) source.getVector("name");
      idVec.allocateNew(4);
      nameVec.allocateNew();

      idVec.set(0, 10L);
      nameVec.set(0, "Alice".getBytes());
      idVec.set(1, 20L);
      nameVec.set(1, "Bob".getBytes());
      idVec.set(2, 5L);
      nameVec.set(2, "Charlie".getBytes());
      idVec.set(3, 30L);
      nameVec.set(3, "Diana".getBytes());
      idVec.setValueCount(4);
      nameVec.setValueCount(4);
      source.setRowCount(4);

      try (VectorSchemaRoot result = filter.filter(source, alloc)) {
        assertEquals(2, result.getRowCount(), "Only Bob (20) and Diana (30) match id > 15");

        BigIntVector resultId = (BigIntVector) result.getVector("id");
        VarCharVector resultName = (VarCharVector) result.getVector("name");
        assertEquals(20L, resultId.get(0));
        assertEquals(30L, resultId.get(1));
        assertEquals("Bob", resultName.getObject(0).toString());
        assertEquals("Diana", resultName.getObject(1).toString());
      }
    }
  }

  @Test
  void noMatchesReturnsEmptyBatch() {
    ComparisonPredicate pred = new ComparisonPredicate("id", ComparisonOperator.GREATER_THAN, 100L);
    ArrowBatchFilter filter = new ArrowBatchFilter(pred);

    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot source = VectorSchemaRoot.create(ARROW_SCHEMA, alloc)) {
      BigIntVector idVec = (BigIntVector) source.getVector("id");
      VarCharVector nameVec = (VarCharVector) source.getVector("name");
      idVec.allocateNew(2);
      nameVec.allocateNew();

      idVec.set(0, 10L);
      nameVec.set(0, "A".getBytes());
      idVec.set(1, 20L);
      nameVec.set(1, "B".getBytes());
      idVec.setValueCount(2);
      nameVec.setValueCount(2);
      source.setRowCount(2);

      try (VectorSchemaRoot result = filter.filter(source, alloc)) {
        assertEquals(0, result.getRowCount(), "No rows should match id > 100");
      }
    }
  }

  @Test
  void allRowsMatchReturnsCopy() {
    ComparisonPredicate pred = new ComparisonPredicate("id", ComparisonOperator.GREATER_THAN, 0L);
    ArrowBatchFilter filter = new ArrowBatchFilter(pred);

    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot source = VectorSchemaRoot.create(ARROW_SCHEMA, alloc)) {
      BigIntVector idVec = (BigIntVector) source.getVector("id");
      VarCharVector nameVec = (VarCharVector) source.getVector("name");
      idVec.allocateNew(2);
      nameVec.allocateNew();

      idVec.set(0, 10L);
      nameVec.set(0, "A".getBytes());
      idVec.set(1, 20L);
      nameVec.set(1, "B".getBytes());
      idVec.setValueCount(2);
      nameVec.setValueCount(2);
      source.setRowCount(2);

      try (VectorSchemaRoot result = filter.filter(source, alloc)) {
        assertEquals(2, result.getRowCount(), "All rows should match id > 0");
        assertNotSame(source, result, "Result should be a copy, not the same object");
      }
    }
  }

  @Test
  void constructorRejectsNullPredicate() {
    assertThrows(NullPointerException.class, () -> new ArrowBatchFilter(null));
  }
}
