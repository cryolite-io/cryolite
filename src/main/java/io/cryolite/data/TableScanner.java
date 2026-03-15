package io.cryolite.data;

import io.cryolite.filter.ArrowBatchFilter;
import io.cryolite.filter.BatchPredicate;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Objects;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;

/**
 * Orchestrates filtered table reads by combining a {@link TableReader} with residual filtering.
 *
 * <p>The scanner sits between the engine and the low-level reader:
 *
 * <ul>
 *   <li>{@link TableReader} handles pure I/O (Iceberg → Arrow batches)
 *   <li>{@code TableScanner} orchestrates: configure scan → read → apply residual filter → manage
 *       memory
 *   <li>{@link TableWriter} handles pure I/O (records → Iceberg)
 * </ul>
 *
 * <p>In M9+, the scanner will also convert {@link BatchPredicate} to Iceberg expressions for
 * pushdown, applying only the residual (non-pushable) part as an Arrow-level filter.
 *
 * <p><b>Memory Lifecycle:</b> Each filtered batch is valid only until the next call to {@code
 * next()} or until the iterable is closed. The scanner owns the {@link BufferAllocator} used for
 * filtered batches and closes it when the iterable is closed.
 *
 * @since 0.1.0
 */
public class TableScanner {

  private final Table table;

  /**
   * Creates a scanner for the given table.
   *
   * @param table the Iceberg table to scan
   */
  public TableScanner(Table table) {
    this.table = Objects.requireNonNull(table, "table must not be null");
  }

  /**
   * Scans the table with a residual filter applied to the Arrow batches.
   *
   * @param predicate the row predicate for filtering; must not be null
   * @return a closeable iterable of filtered Arrow batches
   * @throws IOException if reading fails
   */
  public CloseableIterable<VectorSchemaRoot> scan(BatchPredicate predicate) throws IOException {
    Objects.requireNonNull(predicate, "predicate must not be null");

    try (TableReader reader = new TableReader(table)) {
      CloseableIterable<VectorSchemaRoot> source = reader.readBatches();
      ArrowBatchFilter filter = new ArrowBatchFilter(predicate);
      BufferAllocator allocator =
          new RootAllocator(); // NOSONAR S2095: closed by FilteredBatchIterable

      return new FilteredBatchIterable(source, filter, allocator);
    }
  }

  /**
   * A closeable iterable that filters Arrow batches and manages their memory lifecycle.
   *
   * <p>Each filtered batch is valid only until the next call to {@code next()} or {@code close()}.
   * This matches the same memory lifecycle contract as Iceberg's vectorized reader.
   *
   * <p>On close, this iterable releases the last filtered batch, closes the source iterable, and
   * closes the allocator in the correct order.
   */
  static class FilteredBatchIterable implements CloseableIterable<VectorSchemaRoot> {

    private final CloseableIterable<VectorSchemaRoot> source;
    private final ArrowBatchFilter filter;
    private final BufferAllocator allocator;
    private FilteredBatchIterator activeIterator;

    FilteredBatchIterable(
        CloseableIterable<VectorSchemaRoot> source,
        ArrowBatchFilter filter,
        BufferAllocator allocator) {
      this.source = source;
      this.filter = filter;
      this.allocator = allocator;
    }

    @Override
    public CloseableIterator<VectorSchemaRoot> iterator() {
      activeIterator = new FilteredBatchIterator(source.iterator(), filter, allocator);
      return activeIterator;
    }

    @Override
    public void close() throws IOException {
      try {
        if (activeIterator != null) {
          activeIterator.close();
        }
      } finally {
        try {
          source.close();
        } finally {
          allocator.close();
        }
      }
    }
  }

  /**
   * Iterator that filters each source batch and auto-closes the previous filtered batch when the
   * next one is requested or when the iterator is closed.
   */
  static class FilteredBatchIterator implements CloseableIterator<VectorSchemaRoot> {

    private final CloseableIterator<VectorSchemaRoot> sourceIterator;
    private final ArrowBatchFilter filter;
    private final BufferAllocator allocator;
    private VectorSchemaRoot currentFiltered;

    FilteredBatchIterator(
        CloseableIterator<VectorSchemaRoot> sourceIterator,
        ArrowBatchFilter filter,
        BufferAllocator allocator) {
      this.sourceIterator = sourceIterator;
      this.filter = filter;
      this.allocator = allocator;
    }

    @Override
    public boolean hasNext() {
      return sourceIterator.hasNext();
    }

    @Override
    public VectorSchemaRoot next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      closeCurrent();
      VectorSchemaRoot sourceBatch = sourceIterator.next();
      currentFiltered = filter.filter(sourceBatch, allocator);
      return currentFiltered;
    }

    @Override
    public void close() throws IOException {
      closeCurrent();
      sourceIterator.close();
    }

    private void closeCurrent() {
      if (currentFiltered != null) {
        currentFiltered.close();
        currentFiltered = null;
      }
    }
  }
}
