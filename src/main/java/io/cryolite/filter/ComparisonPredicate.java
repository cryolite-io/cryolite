package io.cryolite.filter;

import java.util.BitSet;
import java.util.Objects;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * A batch predicate that compares a column value against a literal using a comparison operator.
 *
 * <p>Evaluates the entire column in a single pass, producing a {@link BitSet} selection vector.
 * This is cache-friendly because it iterates through contiguous column memory rather than jumping
 * across columns per row.
 *
 * <p>Supports the standard SQL comparison operators: {@code =}, {@code <>}, {@code <}, {@code <=},
 * {@code >}, {@code >=}. Values are extracted from the Arrow vector and compared using {@link
 * ComparisonOperator} semantics with numeric type promotion.
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

    // Single pass through the column: cache-friendly, contiguous memory access
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
