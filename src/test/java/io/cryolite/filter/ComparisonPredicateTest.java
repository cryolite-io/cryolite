package io.cryolite.filter;

import static org.junit.jupiter.api.Assertions.*;

import io.cryolite.arrow.SchemaConverter;
import java.util.BitSet;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ComparisonPredicate}.
 *
 * <p>Tests predicate evaluation against Arrow batches with BIGINT and VARCHAR columns, including
 * NULL handling.
 */
class ComparisonPredicateTest {

  private static final org.apache.iceberg.Schema ICE_SCHEMA =
      new org.apache.iceberg.Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "name", Types.StringType.get()));

  private static final Schema ARROW_SCHEMA = SchemaConverter.toArrow(ICE_SCHEMA);

  @Test
  void equalsMatchesCorrectRow() {
    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(ARROW_SCHEMA, alloc)) {
      BigIntVector idVec = (BigIntVector) root.getVector("id");
      VarCharVector nameVec = (VarCharVector) root.getVector("name");
      idVec.allocateNew(3);
      nameVec.allocateNew();

      idVec.set(0, 10L);
      idVec.set(1, 20L);
      idVec.set(2, 30L);
      nameVec.set(0, "Alice".getBytes());
      nameVec.set(1, "Bob".getBytes());
      nameVec.set(2, "Charlie".getBytes());
      idVec.setValueCount(3);
      nameVec.setValueCount(3);
      root.setRowCount(3);

      ComparisonPredicate pred = new ComparisonPredicate("id", ComparisonOperator.EQUALS, 20L);

      BitSet result = pred.evaluate(root);
      assertFalse(result.get(0));
      assertTrue(result.get(1));
      assertFalse(result.get(2));
    }
  }

  @Test
  void greaterThanOnStrings() {
    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(ARROW_SCHEMA, alloc)) {
      BigIntVector idVec = (BigIntVector) root.getVector("id");
      VarCharVector nameVec = (VarCharVector) root.getVector("name");
      idVec.allocateNew(2);
      nameVec.allocateNew();

      idVec.set(0, 1L);
      idVec.set(1, 2L);
      nameVec.set(0, "Alice".getBytes());
      nameVec.set(1, "Zara".getBytes());
      idVec.setValueCount(2);
      nameVec.setValueCount(2);
      root.setRowCount(2);

      ComparisonPredicate pred =
          new ComparisonPredicate("name", ComparisonOperator.GREATER_THAN, "M");

      BitSet result = pred.evaluate(root);
      assertFalse(result.get(0)); // "Alice" < "M"
      assertTrue(result.get(1)); // "Zara" > "M"
    }
  }

  @Test
  void nullColumnValueReturnsFalse() {
    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(ARROW_SCHEMA, alloc)) {
      BigIntVector idVec = (BigIntVector) root.getVector("id");
      VarCharVector nameVec = (VarCharVector) root.getVector("name");
      idVec.allocateNew(1);
      nameVec.allocateNew();

      idVec.set(0, 1L);
      // name is NULL at index 0 (not set on optional vector)
      nameVec.setNull(0);
      idVec.setValueCount(1);
      nameVec.setValueCount(1);
      root.setRowCount(1);

      ComparisonPredicate pred =
          new ComparisonPredicate("name", ComparisonOperator.EQUALS, "Alice");
      BitSet result = pred.evaluate(root);
      assertFalse(result.get(0), "NULL comparison should return false");
    }
  }

  @Test
  void nonExistentColumnReturnsFalse() {
    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(ARROW_SCHEMA, alloc)) {
      BigIntVector idVec = (BigIntVector) root.getVector("id");
      idVec.allocateNew(1);
      idVec.set(0, 1L);
      idVec.setValueCount(1);
      root.setRowCount(1);

      ComparisonPredicate pred =
          new ComparisonPredicate("missing_col", ComparisonOperator.EQUALS, 1L);
      BitSet result = pred.evaluate(root);
      assertFalse(result.get(0), "Missing column should return false");
    }
  }

  @Test
  void constructorRejectsNullColumnName() {
    assertThrows(
        NullPointerException.class,
        () -> new ComparisonPredicate(null, ComparisonOperator.EQUALS, 1L));
  }

  @Test
  void constructorRejectsNullOperator() {
    assertThrows(NullPointerException.class, () -> new ComparisonPredicate("id", null, 1L));
  }

  @Test
  void gettersReturnConstructorValues() {
    ComparisonPredicate pred = new ComparisonPredicate("age", ComparisonOperator.LESS_THAN, 30L);
    assertEquals("age", pred.getColumnName());
    assertEquals(ComparisonOperator.LESS_THAN, pred.getOperator());
    assertEquals(30L, pred.getLiteral());
  }
}
