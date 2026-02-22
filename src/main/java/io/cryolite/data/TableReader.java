package io.cryolite.data;

import io.cryolite.arrow.RecordConverter;
import io.cryolite.arrow.RecordConverter.FieldAccessor;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.arrow.vectorized.ColumnarBatch;
import org.apache.iceberg.arrow.vectorized.VectorizedTableScanIterable;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;

/**
 * Reads data from an Apache Iceberg table using vectorized Arrow batches.
 *
 * <p>Provides two APIs:
 *
 * <ul>
 *   <li><b>Arrow API</b> ({@link #readBatches()}): Returns columnar batches as {@link
 *       VectorSchemaRoot}. Efficient for vectorized processing, Gandiva expressions, and SQL
 *       execution.
 *   <li><b>Record API</b> ({@link #readRecords()}): Returns row-oriented {@link Record} objects.
 *       Convenience wrapper that internally uses the Arrow batch pipeline.
 * </ul>
 *
 * <p><b>Memory Lifecycle Contract:</b> Each {@link VectorSchemaRoot} batch returned by {@link
 * #readBatches()} is valid only during iteration. The batch memory is owned by Iceberg's vectorized
 * reader and will be freed when the iterator advances or closes. Callers must process each batch
 * within the iteration loop and must not store references for later use.
 *
 * <p>Usage example (Arrow API):
 *
 * <pre>{@code
 * TableReader reader = new TableReader(table);
 * try (CloseableIterable<VectorSchemaRoot> batches = reader.readBatches()) {
 *     for (VectorSchemaRoot batch : batches) {
 *         // Process batch immediately - invalid after next() or close()
 *         System.out.println("Batch rows: " + batch.getRowCount());
 *     }
 * }
 * }</pre>
 *
 * <p>Usage example (Record API):
 *
 * <pre>{@code
 * TableReader reader = new TableReader(table);
 * try (CloseableIterable<Record> records = reader.readRecords()) {
 *     for (Record record : records) {
 *         System.out.println("ID: " + record.getField("id"));
 *     }
 * }
 * }</pre>
 *
 * @since 0.1.0
 */
public class TableReader implements Closeable {

  private final Table table;

  /**
   * Creates a new reader for the given table.
   *
   * @param table the Iceberg table to read from
   */
  public TableReader(Table table) {
    this.table = table;
  }

  /**
   * Reads data as Arrow columnar batches using the table's current snapshot.
   *
   * <p>For custom scan configuration (filters, projections, snapshots), use {@link
   * #readBatches(TableScan)}.
   *
   * @return a closeable iterable of Arrow batches; empty if table has no snapshots
   * @throws IOException if reading fails
   */
  public CloseableIterable<VectorSchemaRoot> readBatches() throws IOException {
    return readBatches(table.newScan());
  }

  /**
   * Reads data as Arrow columnar batches using a custom scan configuration.
   *
   * <p>Accepts a pre-configured {@link TableScan} for filters, projections, and snapshot selection.
   * This is the primary API for SQL execution and vectorized processing.
   *
   * <p><b>Memory Lifecycle:</b> Each batch is valid only during iteration. Do not store batch
   * references for later use.
   *
   * @param scan the configured table scan
   * @return a closeable iterable of Arrow batches; empty if table has no snapshots
   * @throws IOException if reading fails
   */
  public CloseableIterable<VectorSchemaRoot> readBatches(TableScan scan) throws IOException {
    if (table.currentSnapshot() == null) {
      return CloseableIterable.empty();
    }

    VectorizedTableScanIterable iterable = new VectorizedTableScanIterable(scan);
    return CloseableIterable.transform(iterable, ColumnarBatch::createVectorSchemaRootFromVectors);
  }

  /**
   * Reads data as Iceberg Record objects using the table's current snapshot.
   *
   * <p>Convenience wrapper that internally uses {@link #readBatches()} and converts each Arrow
   * batch row to a {@link Record}. For high-performance processing, prefer {@link #readBatches()}.
   *
   * @return a closeable iterable of records; empty if table has no snapshots
   * @throws IOException if reading fails
   */
  public CloseableIterable<Record> readRecords() throws IOException {
    return readRecords(table.newScan());
  }

  /**
   * Reads data as Iceberg Record objects using a custom scan configuration.
   *
   * <p>Internally uses {@link #readBatches(TableScan)} and converts Arrow rows to records using
   * {@link RecordConverter}.
   *
   * @param scan the configured table scan
   * @return a closeable iterable of records; empty if table has no snapshots
   * @throws IOException if reading fails
   */
  public CloseableIterable<Record> readRecords(TableScan scan) throws IOException {
    CloseableIterable<VectorSchemaRoot> batches = readBatches(scan);
    return new BatchToRecordIterable(batches, table.schema());
  }

  @Override
  public void close() {
    // No resources to close - batch iterables manage their own lifecycle
  }

  /**
   * Flattens Arrow batches into individual Iceberg records.
   *
   * <p>Wraps a batch iterable and yields one {@link Record} per row across all batches.
   */
  private static class BatchToRecordIterable implements CloseableIterable<Record> {

    private final CloseableIterable<VectorSchemaRoot> batches;
    private final org.apache.iceberg.Schema schema;

    BatchToRecordIterable(
        CloseableIterable<VectorSchemaRoot> batches, org.apache.iceberg.Schema schema) {
      this.batches = batches;
      this.schema = schema;
    }

    @Override
    public CloseableIterator<Record> iterator() {
      return new BatchToRecordIterator(batches.iterator(), schema);
    }

    @Override
    public void close() throws IOException {
      batches.close();
    }
  }

  /**
   * Iterator that converts Arrow batches to records lazily, one row at a time.
   *
   * <p>When a new batch arrives, {@link RecordConverter#buildAccessors} is called once to resolve
   * vector references and type metadata for all columns. Subsequent {@link #next()} calls each
   * create exactly one {@link Record} via {@link RecordConverter#convertRow}, avoiding bulk
   * materialization of the entire batch upfront.
   *
   * <p>This means:
   *
   * <ul>
   *   <li>HashMap lookups happen once per batch (not once per row)
   *   <li>Record objects are created on demand – early termination wastes no allocations
   *   <li>Memory peak is one Arrow batch + one GenericRecord at a time (not one batch + N records)
   * </ul>
   */
  private static class BatchToRecordIterator implements CloseableIterator<Record> {

    private final CloseableIterator<VectorSchemaRoot> batchIterator;
    private final org.apache.iceberg.Schema schema;

    /** Cached accessors for the current batch – rebuilt when the batch advances. */
    private List<FieldAccessor> currentAccessors;

    /** Total number of rows in the current batch. */
    private int currentBatchRowCount;

    private int currentRow;

    BatchToRecordIterator(
        CloseableIterator<VectorSchemaRoot> batchIterator, org.apache.iceberg.Schema schema) {
      this.batchIterator = batchIterator;
      this.schema = schema;
    }

    @Override
    public boolean hasNext() {
      while (currentAccessors == null || currentRow >= currentBatchRowCount) {
        if (!batchIterator.hasNext()) {
          return false;
        }
        VectorSchemaRoot batch = batchIterator.next();
        // Resolve vector references and types once for the entire batch.
        currentAccessors = RecordConverter.buildAccessors(schema, batch);
        currentBatchRowCount = batch.getRowCount();
        currentRow = 0;
      }
      return true;
    }

    @Override
    public Record next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      // Create exactly one record – lazy, on demand.
      Record record = RecordConverter.convertRow(schema, currentAccessors, currentRow);
      currentRow++;
      return record;
    }

    @Override
    public void close() throws IOException {
      batchIterator.close();
    }
  }
}
