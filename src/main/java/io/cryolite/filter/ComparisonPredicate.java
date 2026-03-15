package io.cryolite.filter;

import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.Objects;
import java.util.function.DoublePredicate;
import java.util.function.LongPredicate;
import java.util.function.Predicate;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * A batch predicate that compares a column value against a literal using a comparison operator.
 *
 * <p>Evaluates the entire column in a single pass, producing a {@link BitSet} selection vector.
 * This is cache-friendly because it iterates through contiguous column memory rather than jumping
 * across columns per row.
 *
 * <p>Supports the standard SQL comparison operators: {@code =}, {@code <>}, {@code <}, {@code <=},
 * {@code >}, {@code >=}.
 *
 * <p>Type dispatch: the vector type is examined once per {@code evaluate()} call and a
 * type-specific primitive loop is used. This avoids per-row boxing (e.g., {@code long → Long})
 * that occurs with {@link FieldVector#getObject}. For example, {@link BigIntVector#get} returns a
 * primitive {@code long} that is compared directly via a {@link LongPredicate}. Unsupported vector
 * types fall back to {@code getObject()}.
 *
 * <p>NULL handling follows SQL three-valued logic: any comparison involving NULL yields {@code
 * false} (the row does not match).
 *
 * @since 0.1.0
 */
public class ComparisonPredicate implements BatchPredicate {

  private final String columnName;
  private final ComparisonOperator operator;
  private final Comparable<?> literal;

  /**
   * Creates a comparison predicate.
   *
   * @param columnName the name of the column to compare
   * @param operator the comparison operator
   * @param literal the literal value to compare against
   */
  public ComparisonPredicate(
      String columnName, ComparisonOperator operator, Comparable<?> literal) {
    this.columnName = Objects.requireNonNull(columnName, "columnName must not be null");
    this.operator = Objects.requireNonNull(operator, "operator must not be null");
    this.literal = literal;
  }

  @Override
  public BitSet evaluate(VectorSchemaRoot batch) {
    int rowCount = batch.getRowCount();
    BitSet selection = new BitSet(rowCount);

    FieldVector vector = batch.getVector(columnName);
    if (vector == null) {
      return selection; // all bits clear → no matches
    }

    // Dispatch once on vector type; each branch runs a tight primitive loop.
    if (vector instanceof BigIntVector v) {
      return evaluateLong(v, rowCount, selection);
    } else if (vector instanceof IntVector v) {
      return evaluateInt(v, rowCount, selection);
    } else if (vector instanceof Float8Vector v) {
      return evaluateDouble(v, rowCount, selection);
    } else if (vector instanceof Float4Vector v) {
      return evaluateFloat(v, rowCount, selection);
    } else if (vector instanceof VarCharVector v) {
      return evaluateVarChar(v, rowCount, selection);
    } else {
      return evaluateGeneric(vector, rowCount, selection);
    }
  }

  // --- Type-specific primitive loops (no boxing) ---

  private BitSet evaluateLong(BigIntVector vec, int rowCount, BitSet selection) {
    LongPredicate test = buildLongComparator(((Number) literal).longValue());
    for (int i = 0; i < rowCount; i++) {
      if (!vec.isNull(i) && test.test(vec.get(i))) {
        selection.set(i);
      }
    }
    return selection;
  }

  private BitSet evaluateInt(IntVector vec, int rowCount, BitSet selection) {
    // Promote int literal to long so the same comparator works for both int and long columns.
    LongPredicate test = buildLongComparator(((Number) literal).longValue());
    for (int i = 0; i < rowCount; i++) {
      if (!vec.isNull(i) && test.test(vec.get(i))) {
        selection.set(i);
      }
    }
    return selection;
  }

  private BitSet evaluateDouble(Float8Vector vec, int rowCount, BitSet selection) {
    DoublePredicate test = buildDoubleComparator(((Number) literal).doubleValue());
    for (int i = 0; i < rowCount; i++) {
      if (!vec.isNull(i) && test.test(vec.get(i))) {
        selection.set(i);
      }
    }
    return selection;
  }

  private BitSet evaluateFloat(Float4Vector vec, int rowCount, BitSet selection) {
    // Promote float to double so the same comparator works for both float and double columns.
    DoublePredicate test = buildDoubleComparator(((Number) literal).doubleValue());
    for (int i = 0; i < rowCount; i++) {
      if (!vec.isNull(i) && test.test(vec.get(i))) {
        selection.set(i);
      }
    }
    return selection;
  }

  private BitSet evaluateVarChar(VarCharVector vec, int rowCount, BitSet selection) {
    String litStr = literal.toString();
    Predicate<String> test = buildStringComparator(litStr);
    for (int i = 0; i < rowCount; i++) {
      if (!vec.isNull(i)) {
        // Use get(i) → byte[] to skip Arrow's Text wrapper allocation
        String value = new String(vec.get(i), StandardCharsets.UTF_8);
        if (test.test(value)) {
          selection.set(i);
        }
      }
    }
    return selection;
  }

  /** Fallback for unsupported vector types (Decimal, Binary, UUID, etc.). */
  private BitSet evaluateGeneric(FieldVector vector, int rowCount, BitSet selection) {
    for (int i = 0; i < rowCount; i++) {
      if (!vector.isNull(i)) {
        Object value = vector.getObject(i);
        if (value != null && operator.apply(value, literal)) {
          selection.set(i);
        }
      }
    }
    return selection;
  }

  // --- Comparator builders: dispatch operator once, return a primitive functional interface ---

  private LongPredicate buildLongComparator(long litVal) {
    return switch (operator) {
      case EQUALS -> v -> v == litVal;
      case NOT_EQUALS -> v -> v != litVal;
      case LESS_THAN -> v -> v < litVal;
      case LESS_THAN_OR_EQUAL -> v -> v <= litVal;
      case GREATER_THAN -> v -> v > litVal;
      case GREATER_THAN_OR_EQUAL -> v -> v >= litVal;
    };
  }

  private DoublePredicate buildDoubleComparator(double litVal) {
    return switch (operator) {
      case EQUALS -> v -> v == litVal;
      case NOT_EQUALS -> v -> v != litVal;
      case LESS_THAN -> v -> v < litVal;
      case LESS_THAN_OR_EQUAL -> v -> v <= litVal;
      case GREATER_THAN -> v -> v > litVal;
      case GREATER_THAN_OR_EQUAL -> v -> v >= litVal;
    };
  }

  private Predicate<String> buildStringComparator(String litStr) {
    return switch (operator) {
      case EQUALS -> v -> v.equals(litStr);
      case NOT_EQUALS -> v -> !v.equals(litStr);
      case LESS_THAN -> v -> v.compareTo(litStr) < 0;
      case LESS_THAN_OR_EQUAL -> v -> v.compareTo(litStr) <= 0;
      case GREATER_THAN -> v -> v.compareTo(litStr) > 0;
      case GREATER_THAN_OR_EQUAL -> v -> v.compareTo(litStr) >= 0;
    };
  }

  /** Returns the column name this predicate operates on. */
  public String getColumnName() {
    return columnName;
  }

  /** Returns the comparison operator. */
  public ComparisonOperator getOperator() {
    return operator;
  }

  /** Returns the literal value. */
  public Comparable<?> getLiteral() {
    return literal;
  }
}
