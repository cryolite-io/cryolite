package io.cryolite.filter;

import static org.junit.jupiter.api.Assertions.*;

import io.cryolite.arrow.SchemaConverter;
import java.util.BitSet;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
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
  void intVectorLessThanMatchesCorrectRows() {
    org.apache.iceberg.Schema iceSchema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "age", Types.IntegerType.get()));
    Schema arrowSchema = SchemaConverter.toArrow(iceSchema);

    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, alloc)) {
      IntVector ageVec = (IntVector) root.getVector("age");
      ageVec.allocateNew(3);
      ageVec.set(0, 10);
      ageVec.set(1, 20);
      ageVec.set(2, 30);
      ageVec.setValueCount(3);
      root.setRowCount(3);

      ComparisonPredicate pred = new ComparisonPredicate("age", ComparisonOperator.LESS_THAN, 25);
      BitSet result = pred.evaluate(root);
      assertTrue(result.get(0));  // 10 < 25
      assertTrue(result.get(1));  // 20 < 25
      assertFalse(result.get(2)); // 30 >= 25
    }
  }

  @Test
  void float8VectorGreaterThanMatchesCorrectRows() {
    org.apache.iceberg.Schema iceSchema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "score", Types.DoubleType.get()));
    Schema arrowSchema = SchemaConverter.toArrow(iceSchema);

    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, alloc)) {
      Float8Vector scoreVec = (Float8Vector) root.getVector("score");
      scoreVec.allocateNew(3);
      scoreVec.set(0, 1.5);
      scoreVec.set(1, 3.0);
      scoreVec.set(2, 4.5);
      scoreVec.setValueCount(3);
      root.setRowCount(3);

      ComparisonPredicate pred =
          new ComparisonPredicate("score", ComparisonOperator.GREATER_THAN, 2.0);
      BitSet result = pred.evaluate(root);
      assertFalse(result.get(0)); // 1.5 <= 2.0
      assertTrue(result.get(1));  // 3.0 > 2.0
      assertTrue(result.get(2));  // 4.5 > 2.0
    }
  }

  @Test
  void longColumnRemainingOperatorsEvaluateCorrectly() {
    // Covers NOT_EQUALS, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL branches
    // in buildLongComparator using id=10 as the column value.
    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(ARROW_SCHEMA, alloc)) {
      BigIntVector idVec = (BigIntVector) root.getVector("id");
      VarCharVector nameVec = (VarCharVector) root.getVector("name");
      idVec.allocateNew(1);
      nameVec.allocateNew();
      idVec.set(0, 10L);
      nameVec.setNull(0);
      idVec.setValueCount(1);
      nameVec.setValueCount(1);
      root.setRowCount(1);

      assertFalse(new ComparisonPredicate("id", ComparisonOperator.NOT_EQUALS, 10L).evaluate(root).get(0));
      assertTrue(new ComparisonPredicate("id", ComparisonOperator.NOT_EQUALS, 5L).evaluate(root).get(0));
      assertTrue(new ComparisonPredicate("id", ComparisonOperator.LESS_THAN_OR_EQUAL, 10L).evaluate(root).get(0));
      assertFalse(new ComparisonPredicate("id", ComparisonOperator.LESS_THAN_OR_EQUAL, 9L).evaluate(root).get(0));
      assertTrue(new ComparisonPredicate("id", ComparisonOperator.GREATER_THAN, 9L).evaluate(root).get(0));
      assertFalse(new ComparisonPredicate("id", ComparisonOperator.GREATER_THAN, 10L).evaluate(root).get(0));
      assertTrue(new ComparisonPredicate("id", ComparisonOperator.GREATER_THAN_OR_EQUAL, 10L).evaluate(root).get(0));
      assertFalse(new ComparisonPredicate("id", ComparisonOperator.GREATER_THAN_OR_EQUAL, 11L).evaluate(root).get(0));
    }
  }

  @Test
  void doubleColumnAllOperatorsEvaluateCorrectly() {
    // Covers all buildDoubleComparator branches using score=5.0 as the column value.
    org.apache.iceberg.Schema iceSchema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "score", Types.DoubleType.get()));
    Schema arrowSchema = SchemaConverter.toArrow(iceSchema);

    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, alloc)) {
      Float8Vector scoreVec = (Float8Vector) root.getVector("score");
      scoreVec.allocateNew(1);
      scoreVec.set(0, 5.0);
      scoreVec.setValueCount(1);
      root.setRowCount(1);

      assertTrue(new ComparisonPredicate("score", ComparisonOperator.EQUALS, 5.0).evaluate(root).get(0));
      assertFalse(new ComparisonPredicate("score", ComparisonOperator.NOT_EQUALS, 5.0).evaluate(root).get(0));
      assertTrue(new ComparisonPredicate("score", ComparisonOperator.NOT_EQUALS, 3.0).evaluate(root).get(0));
      assertTrue(new ComparisonPredicate("score", ComparisonOperator.LESS_THAN, 6.0).evaluate(root).get(0));
      assertFalse(new ComparisonPredicate("score", ComparisonOperator.LESS_THAN, 5.0).evaluate(root).get(0));
      assertTrue(new ComparisonPredicate("score", ComparisonOperator.LESS_THAN_OR_EQUAL, 5.0).evaluate(root).get(0));
      assertFalse(new ComparisonPredicate("score", ComparisonOperator.LESS_THAN_OR_EQUAL, 4.0).evaluate(root).get(0));
      assertTrue(new ComparisonPredicate("score", ComparisonOperator.GREATER_THAN_OR_EQUAL, 5.0).evaluate(root).get(0));
      assertFalse(new ComparisonPredicate("score", ComparisonOperator.GREATER_THAN_OR_EQUAL, 6.0).evaluate(root).get(0));
    }
  }

  @Test
  void varCharColumnRemainingOperatorsEvaluateCorrectly() {
    // Covers EQUALS, NOT_EQUALS, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN_OR_EQUAL
    // in buildStringComparator using name="Charlie" as the column value.
    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(ARROW_SCHEMA, alloc)) {
      BigIntVector idVec = (BigIntVector) root.getVector("id");
      VarCharVector nameVec = (VarCharVector) root.getVector("name");
      idVec.allocateNew(1);
      nameVec.allocateNew();
      idVec.set(0, 1L);
      nameVec.set(0, "Charlie".getBytes());
      idVec.setValueCount(1);
      nameVec.setValueCount(1);
      root.setRowCount(1);

      assertTrue(new ComparisonPredicate("name", ComparisonOperator.EQUALS, "Charlie").evaluate(root).get(0));
      assertFalse(new ComparisonPredicate("name", ComparisonOperator.NOT_EQUALS, "Charlie").evaluate(root).get(0));
      assertTrue(new ComparisonPredicate("name", ComparisonOperator.NOT_EQUALS, "Alice").evaluate(root).get(0));
      assertTrue(new ComparisonPredicate("name", ComparisonOperator.LESS_THAN, "Zara").evaluate(root).get(0));
      assertFalse(new ComparisonPredicate("name", ComparisonOperator.LESS_THAN, "Alice").evaluate(root).get(0));
      assertTrue(new ComparisonPredicate("name", ComparisonOperator.LESS_THAN_OR_EQUAL, "Charlie").evaluate(root).get(0));
      assertFalse(new ComparisonPredicate("name", ComparisonOperator.LESS_THAN_OR_EQUAL, "Bob").evaluate(root).get(0));
      assertTrue(new ComparisonPredicate("name", ComparisonOperator.GREATER_THAN_OR_EQUAL, "Charlie").evaluate(root).get(0));
      assertFalse(new ComparisonPredicate("name", ComparisonOperator.GREATER_THAN_OR_EQUAL, "Zara").evaluate(root).get(0));
    }
  }

  @Test
  void float4VectorEvaluatesWithPromotion() {
    // Covers the evaluateFloat dispatch path (float → double promotion).
    org.apache.iceberg.Schema iceSchema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "ratio", Types.FloatType.get()));
    Schema arrowSchema = SchemaConverter.toArrow(iceSchema);

    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, alloc)) {
      Float4Vector ratioVec = (Float4Vector) root.getVector("ratio");
      ratioVec.allocateNew(2);
      ratioVec.set(0, 0.5f);
      ratioVec.set(1, 1.5f);
      ratioVec.setValueCount(2);
      root.setRowCount(2);

      ComparisonPredicate pred = new ComparisonPredicate("ratio", ComparisonOperator.GREATER_THAN, 1.0);
      BitSet result = pred.evaluate(root);
      assertFalse(result.get(0)); // 0.5 <= 1.0
      assertTrue(result.get(1));  // 1.5 > 1.0
    }
  }

  @Test
  void genericFallbackEvaluatesForUnsupportedVectorType() {
    // Covers evaluateGeneric via a BOOLEAN (BitVector) column which has no dedicated dispatch path.
    org.apache.iceberg.Schema iceSchema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "active", Types.BooleanType.get()));
    Schema arrowSchema = SchemaConverter.toArrow(iceSchema);

    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, alloc)) {
      BitVector activeVec = (BitVector) root.getVector("active");
      activeVec.allocateNew(2);
      activeVec.set(0, 1); // true
      activeVec.set(1, 0); // false
      activeVec.setValueCount(2);
      root.setRowCount(2);

      // BitVector.getObject() returns Boolean; operator.apply() handles it via Comparable fallback
      ComparisonPredicate pred = new ComparisonPredicate("active", ComparisonOperator.EQUALS, Boolean.TRUE);
      BitSet result = pred.evaluate(root);
      assertTrue(result.get(0));  // true == true
      assertFalse(result.get(1)); // false != true
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
