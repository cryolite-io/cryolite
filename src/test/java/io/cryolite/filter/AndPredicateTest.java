package io.cryolite.filter;

import static org.junit.jupiter.api.Assertions.*;

import io.cryolite.arrow.SchemaConverter;
import java.util.BitSet;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link AndPredicate}.
 *
 * <p>Verifies BitSet AND semantics, empty operand list (vacuous truth), and null rejection.
 */
class AndPredicateTest {

  private static final org.apache.iceberg.Schema ICE_SCHEMA =
      new org.apache.iceberg.Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required(2, "name", Types.StringType.get()));

  private static final Schema ARROW_SCHEMA = SchemaConverter.toArrow(ICE_SCHEMA);

  @Test
  void allPredicatesTrueReturnsTrue() {
    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(ARROW_SCHEMA, alloc)) {
      BigIntVector idVec = (BigIntVector) root.getVector("id");
      VarCharVector nameVec = (VarCharVector) root.getVector("name");
      idVec.allocateNew(1);
      nameVec.allocateNew();
      idVec.set(0, 25L);
      nameVec.set(0, "Alice".getBytes());
      idVec.setValueCount(1);
      nameVec.setValueCount(1);
      root.setRowCount(1);

      AndPredicate and =
          new AndPredicate(
              List.of(
                  new ComparisonPredicate("id", ComparisonOperator.GREATER_THAN, 10L),
                  new ComparisonPredicate("name", ComparisonOperator.EQUALS, "Alice")));

      BitSet result = and.evaluate(root);
      assertTrue(result.get(0));
    }
  }

  @Test
  void onePredicateFalseReturnsFalse() {
    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(ARROW_SCHEMA, alloc)) {
      BigIntVector idVec = (BigIntVector) root.getVector("id");
      VarCharVector nameVec = (VarCharVector) root.getVector("name");
      idVec.allocateNew(1);
      nameVec.allocateNew();
      idVec.set(0, 5L); // id = 5, which is NOT > 10
      nameVec.set(0, "Alice".getBytes());
      idVec.setValueCount(1);
      nameVec.setValueCount(1);
      root.setRowCount(1);

      AndPredicate and =
          new AndPredicate(
              List.of(
                  new ComparisonPredicate("id", ComparisonOperator.GREATER_THAN, 10L),
                  new ComparisonPredicate("name", ComparisonOperator.EQUALS, "Alice")));

      BitSet result = and.evaluate(root);
      assertFalse(result.get(0));
    }
  }

  @Test
  void emptyOperandListMatchesEveryRow() {
    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(ARROW_SCHEMA, alloc)) {
      BigIntVector idVec = (BigIntVector) root.getVector("id");
      VarCharVector nameVec = (VarCharVector) root.getVector("name");
      idVec.allocateNew(1);
      nameVec.allocateNew();
      idVec.set(0, 1L);
      nameVec.set(0, "x".getBytes());
      idVec.setValueCount(1);
      nameVec.setValueCount(1);
      root.setRowCount(1);

      AndPredicate and = new AndPredicate(List.of());
      BitSet result = and.evaluate(root);
      assertTrue(result.get(0), "Empty AND = vacuous truth");
    }
  }

  @Test
  void constructorRejectsNull() {
    assertThrows(NullPointerException.class, () -> new AndPredicate(null));
  }

  @Test
  void getOperandsReturnsImmutableCopy() {
    AndPredicate and =
        new AndPredicate(List.of(new ComparisonPredicate("id", ComparisonOperator.EQUALS, 1L)));
    assertEquals(1, and.getOperands().size());
    assertThrows(UnsupportedOperationException.class, () -> and.getOperands().clear());
  }
}
