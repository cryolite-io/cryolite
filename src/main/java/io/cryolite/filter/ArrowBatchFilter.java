package io.cryolite.filter;

import java.util.BitSet;
import java.util.Objects;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Filters Arrow batches using a {@link BatchPredicate} and a selection vector.
 *
 * <p>The filter operates in two phases:
 *
 * <ol>
 *   <li><b>Evaluate:</b> The predicate evaluates the entire batch column-wise and returns a {@link
 *       BitSet} selection vector indicating which rows match.
 *   <li><b>Copy:</b> Only the matching rows (set bits) are copied into the result batch using
 *       Arrow's {@link FieldVector#copyFrom}.
 * </ol>
 *
 * <p>This two-phase approach is more efficient than row-by-row evaluation because the predicate can
 * iterate through contiguous column memory (cache-friendly), and the copy phase is driven by a
 * compact selection vector.
 *
 * <p>The caller owns the returned batch and is responsible for closing it.
 *
 * @since 0.1.0
 */
public class ArrowBatchFilter {

  private final BatchPredicate predicate;

  /**
   * Creates a batch filter with the given predicate.
   *
   * @param predicate the predicate to evaluate against the batch
   */
  public ArrowBatchFilter(BatchPredicate predicate) {
    this.predicate = Objects.requireNonNull(predicate, "predicate must not be null");
  }

  /**
   * Filters the given batch, returning a new batch with only matching rows.
   *
   * <p>If no rows match, returns an empty batch with the same schema. If all rows match, still
   * returns a copy (the original batch's memory is managed by Iceberg's reader).
   *
   * @param source the input batch to filter
   * @param allocator the allocator for the output batch
   * @return a new batch containing only matching rows; caller must close
   */
  public VectorSchemaRoot filter(VectorSchemaRoot source, BufferAllocator allocator) {
    Schema schema = source.getSchema();
    VectorSchemaRoot result = VectorSchemaRoot.create(schema, allocator);

    // Phase 1: Evaluate predicate column-wise → selection vector
    BitSet selection = predicate.evaluate(source);

    // Phase 2: Copy matching rows column-wise using the selection vector.
    // Iterating per column (not per row) keeps access within contiguous Arrow buffers,
    // which is cache-friendly because each column's data sits in a single ArrowBuf.
    result.allocateNew();

    int matchCount = 0;
    for (Field field : schema.getFields()) {
      FieldVector srcVector = source.getVector(field);
      FieldVector destVector = result.getVector(field);
      int destRow = 0;
      for (int srcRow = selection.nextSetBit(0);
          srcRow >= 0;
          srcRow = selection.nextSetBit(srcRow + 1)) {
        destVector.copyFrom(srcRow, destRow++, srcVector);
      }
      destVector.setValueCount(destRow);
      matchCount = destRow;
    }
    result.setRowCount(matchCount);

    return result;
  }
}
