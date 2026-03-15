package io.cryolite.filter;

import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * A composite predicate that combines multiple predicates with logical AND semantics.
 *
 * <p>Each child predicate produces a {@link BitSet} selection vector, and the results are combined
 * using {@link BitSet#and}. This is a single bulk bitwise operation per child, replacing per-row
 * short-circuit evaluation with columnar set intersection.
 *
 * <p>If the list of operands is empty, the predicate matches every row (vacuous truth).
 *
 * @since 0.1.0
 */
public class AndPredicate implements BatchPredicate {

  private final List<BatchPredicate> operands;

  /**
   * Creates an AND predicate from the given operands.
   *
   * @param operands the predicates to combine; must not be null
   * @throws NullPointerException if operands is null
   */
  public AndPredicate(List<BatchPredicate> operands) {
    this.operands = List.copyOf(Objects.requireNonNull(operands, "operands must not be null"));
  }

  @Override
  public BitSet evaluate(VectorSchemaRoot batch) {
    int rowCount = batch.getRowCount();

    // Vacuous truth: empty AND matches all rows
    BitSet result = new BitSet(rowCount);
    result.set(0, rowCount);

    for (BatchPredicate operand : operands) {
      result.and(operand.evaluate(batch));
    }

    return result;
  }

  /** Returns the list of operands. */
  public List<BatchPredicate> getOperands() {
    return operands;
  }
}
