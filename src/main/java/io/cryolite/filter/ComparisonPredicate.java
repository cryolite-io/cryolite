package io.cryolite.filter;

import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.Objects;
import java.util.Optional;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.iceberg.expressions.Expression;

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
 * <p>Type dispatch: the vector type is examined once per {@code evaluate()} call. A type-specific
 * predicate is obtained from {@link ComparisonOperator} (e.g., {@link
 * ComparisonOperator#asLongPredicate}) and reused for every row. This avoids per-row boxing and
 * keeps the operator switch out of the hot loop. Unsupported vector types fall back to {@code
 * getObject()}.
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

    // Dispatch once on vector type. The predicate is built once and reused per row,
    // keeping the operator switch out of the hot loop.
    if (vector instanceof BigIntVector v) {
      var test = operator.asLongPredicate(((Number) literal).longValue());
      for (int i = 0; i < rowCount; i++) {
        if (!v.isNull(i) && test.test(v.get(i))) selection.set(i);
      }
    } else if (vector instanceof IntVector v) {
      // Promote int to long – reuses the same long comparator.
      var test = operator.asLongPredicate(((Number) literal).longValue());
      for (int i = 0; i < rowCount; i++) {
        if (!v.isNull(i) && test.test(v.get(i))) selection.set(i);
      }
    } else if (vector instanceof Float8Vector v) {
      var test = operator.asDoublePredicate(((Number) literal).doubleValue());
      for (int i = 0; i < rowCount; i++) {
        if (!v.isNull(i) && test.test(v.get(i))) selection.set(i);
      }
    } else if (vector instanceof Float4Vector v) {
      // Promote float to double – reuses the same double comparator.
      var test = operator.asDoublePredicate(((Number) literal).doubleValue());
      for (int i = 0; i < rowCount; i++) {
        if (!v.isNull(i) && test.test(v.get(i))) selection.set(i);
      }
    } else if (vector instanceof VarCharVector v) {
      var test = operator.asStringPredicate(literal.toString());
      for (int i = 0; i < rowCount; i++) {
        if (!v.isNull(i) && test.test(new String(v.get(i), StandardCharsets.UTF_8))) {
          selection.set(i);
        }
      }
    } else {
      // Fallback for unsupported types (Decimal, Binary, UUID, Boolean, …).
      for (int i = 0; i < rowCount; i++) {
        if (!vector.isNull(i)) {
          Object value = vector.getObject(i);
          if (value != null && operator.apply(value, literal)) selection.set(i);
        }
      }
    }
    return selection;
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

  /**
   * Returns an Iceberg expression equivalent to this comparison, enabling pushdown.
   *
   * <p>All six supported operators map directly to {@link
   * org.apache.iceberg.expressions.Expressions}, so the result fully covers the predicate and the
   * residual Arrow evaluation can be skipped.
   */
  @Override
  public Optional<Expression> toIcebergExpression() {
    return Optional.of(operator.asIcebergExpression(columnName, literal));
  }
}
