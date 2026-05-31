package io.cryolite.filter;

import java.util.BitSet;
import java.util.Optional;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.iceberg.expressions.Expression;

/**
 * A predicate that evaluates an entire Arrow batch column-wise and returns a selection vector.
 *
 * <p>Unlike a row-oriented predicate that checks one row at a time, a batch predicate evaluates the
 * entire column in one pass and produces a {@link BitSet} indicating which rows match. This
 * approach is more efficient because:
 *
 * <ul>
 *   <li><b>Cache-friendly:</b> Iterates through contiguous column memory instead of jumping across
 *       columns per row.
 *   <li><b>Composable via bitwise ops:</b> {@link AndPredicate} combines child results with {@link
 *       BitSet#and}, which is a single bulk operation instead of per-row short-circuit evaluation.
 *   <li><b>Bulk filtering:</b> {@link ArrowBatchFilter} copies matching rows using the selection
 *       vector, enabling potential future SIMD/vectorized optimizations.
 * </ul>
 *
 * <p>Predicates are composable: use {@link AndPredicate} to combine multiple predicates with
 * logical AND semantics.
 *
 * @since 0.1.0
 */
@FunctionalInterface
public interface BatchPredicate {

  /**
   * Evaluates this predicate against all rows in the batch and returns a selection vector.
   *
   * <p>The returned {@link BitSet} has bit {@code i} set if row {@code i} matches the predicate.
   * The BitSet size is at least {@code batch.getRowCount()}.
   *
   * @param batch the Arrow batch to evaluate
   * @return a selection vector where set bits indicate matching rows
   */
  BitSet evaluate(VectorSchemaRoot batch);

  /**
   * Returns an Iceberg {@link Expression} that fully covers this predicate, enabling pushdown to
   * the Iceberg scan layer (manifest, file, and row-group pruning).
   *
   * <p><b>Coverage contract:</b> if the returned {@link Optional} is present, the expression is
   * semantically equivalent to this predicate and the residual Arrow-level filter can be skipped.
   * If absent, the predicate is not (fully) pushable and the residual Arrow-level filter must run
   * for correctness.
   *
   * <p>The default returns {@link Optional#empty()} so any new predicate type is safe-by-default
   * (correctness via residual evaluation) until it opts in to pushdown.
   *
   * @return the pushable expression, or empty if pushdown is not possible
   */
  default Optional<Expression> toIcebergExpression() {
    return Optional.empty();
  }
}
