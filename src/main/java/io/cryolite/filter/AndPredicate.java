package io.cryolite.filter;

import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;

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

  /**
   * Returns an Iceberg expression equivalent to this AND predicate, enabling pushdown.
   *
   * <p>Present only when <em>all</em> operands are individually pushable (i.e., all return a
   * non-empty {@link Optional} from {@link BatchPredicate#toIcebergExpression()}). If any operand
   * is not pushable, the whole AND is not pushed down and the residual Arrow filter runs.
   *
   * <p>An empty operand list is vacuously true and maps to {@link Expressions#alwaysTrue()}.
   */
  @Override
  public Optional<Expression> toIcebergExpression() {
    if (operands.isEmpty()) {
      return Optional.of(Expressions.alwaysTrue());
    }

    Expression combined = null;
    for (BatchPredicate operand : operands) {
      Optional<Expression> expr = operand.toIcebergExpression();
      if (expr.isEmpty()) {
        return Optional.empty(); // not fully pushable
      }
      combined = (combined == null) ? expr.get() : Expressions.and(combined, expr.get());
    }
    return Optional.ofNullable(combined);
  }
}
