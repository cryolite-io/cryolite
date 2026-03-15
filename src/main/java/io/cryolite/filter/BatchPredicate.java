package io.cryolite.filter;

import java.util.BitSet;
import org.apache.arrow.vector.VectorSchemaRoot;

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
}
