package io.cryolite.data;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitionedFanoutWriter;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;

/**
 * Low-level writer for appending data to Iceberg tables.
 *
 * <p>TableWriter encapsulates the complexity of writing data to Iceberg tables using the native
 * Iceberg Data API. It supports both partitioned and non-partitioned tables, handles the creation
 * of Parquet files, and manages the commit workflow.
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * Table table = catalog.loadTable(tableId);
 * try (TableWriter writer = new TableWriter(table)) {
 *     GenericRecord record = GenericRecord.create(table.schema());
 *     record.setField("id", 1L);
 *     record.setField("name", "Alice");
 *     writer.write(record);
 *
 *     // Commit creates a new snapshot
 *     writer.commit();
 * }
 * }</pre>
 *
 * @since 0.1.0
 */
public class TableWriter implements Closeable {

  /** Default target file size in bytes (128 MB). */
  private static final long DEFAULT_TARGET_FILE_SIZE = 128L * 1024 * 1024;

  private final Table table;
  private final TaskWriter<Record> writer;
  private final List<DataFile> committedFiles;
  private boolean closed = false;

  /**
   * Creates a new TableWriter for the specified table.
   *
   * @param table the Iceberg table to write to
   * @throws IllegalArgumentException if table is null
   */
  public TableWriter(Table table) {
    this(table, DEFAULT_TARGET_FILE_SIZE);
  }

  /**
   * Creates a new TableWriter with a custom target file size.
   *
   * @param table the Iceberg table to write to
   * @param targetFileSizeBytes target size for data files in bytes
   * @throws IllegalArgumentException if table is null or targetFileSizeBytes is not positive
   */
  public TableWriter(Table table, long targetFileSizeBytes) {
    if (table == null) {
      throw new IllegalArgumentException("Table cannot be null");
    }
    if (targetFileSizeBytes <= 0) {
      throw new IllegalArgumentException("Target file size must be positive");
    }

    this.table = table;
    this.committedFiles = new ArrayList<>();

    // Create appender factory for generic records
    GenericAppenderFactory appenderFactory =
        new GenericAppenderFactory(table.schema(), table.spec());

    // Create output file factory with UUID-based operation ID (avoids hostname issues)
    int partitionId = 0;
    long taskId = 0;
    String operationId = UUID.randomUUID().toString();
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, partitionId, taskId)
            .format(FileFormat.PARQUET)
            .operationId(operationId)
            .build();

    // Choose the correct writer based on the table's partitioning.
    // UnpartitionedWriter passes null as partition key, which causes the output file factory
    // to generate paths WITHOUT a partition prefix (avoids double slashes in paths that
    // MinIO and other S3-compatible stores reject).
    // PartitionedFanoutWriter routes records to the correct partition automatically.
    if (table.spec().isUnpartitioned()) {
      this.writer =
          new UnpartitionedWriter<>(
              table.spec(),
              FileFormat.PARQUET,
              appenderFactory,
              fileFactory,
              table.io(),
              targetFileSizeBytes);
    } else {
      PartitionKey partitionKey = new PartitionKey(table.spec(), table.schema());
      InternalRecordWrapper recordWrapper = new InternalRecordWrapper(table.schema().asStruct());
      this.writer =
          new PartitionedFanoutWriter<>(
              table.spec(),
              FileFormat.PARQUET,
              appenderFactory,
              fileFactory,
              table.io(),
              targetFileSizeBytes) {
            @Override
            protected PartitionKey partition(Record record) {
              // Use InternalRecordWrapper to handle type conversions
              // (e.g., OffsetDateTime -> long micros for timestamps)
              partitionKey.partition(recordWrapper.wrap(record));
              return partitionKey;
            }
          };
    }
  }

  /**
   * Writes a record to the table.
   *
   * <p>The record is buffered and will be written to a Parquet file. For partitioned tables, the
   * record is automatically routed to the correct partition.
   *
   * @param record the record to write
   * @throws IOException if an I/O error occurs
   * @throws IllegalStateException if the writer is closed
   */
  public void write(Record record) throws IOException {
    if (closed) {
      throw new IllegalStateException("Writer is closed");
    }
    writer.write(record);
  }

  /**
   * Commits all written records as a new snapshot.
   *
   * <p>This method closes any open data files and appends them to the table as a new snapshot. The
   * commit is atomic - either all files are committed or none.
   *
   * @throws IOException if an I/O error occurs
   * @throws IllegalStateException if the writer is closed
   */
  public void commit() throws IOException {
    if (closed) {
      throw new IllegalStateException("Writer is closed");
    }

    // Complete the writer: closes open files and returns all written data files.
    // TaskWriter.complete() internally calls close() and returns a WriteResult.
    DataFile[] dataFiles = writer.complete().dataFiles();

    if (dataFiles.length > 0) {
      // Append files to table (creates a new snapshot)
      var appendFiles = table.newAppend();
      for (DataFile dataFile : dataFiles) {
        appendFiles.appendFile(dataFile);
        committedFiles.add(dataFile);
      }
      appendFiles.commit();
    }

    closed = true;
  }

  /**
   * Returns the data files written by this writer.
   *
   * <p>This method returns the list of data files that were committed. It can be used to inspect
   * what files were created during the write operation.
   *
   * @return list of committed data files (may be empty if no data was written)
   */
  public List<DataFile> getCommittedFiles() {
    return new ArrayList<>(committedFiles);
  }

  /**
   * Returns the table this writer is writing to.
   *
   * @return the Iceberg table
   */
  public Table getTable() {
    return table;
  }

  /**
   * Checks if this writer is closed.
   *
   * @return true if closed, false otherwise
   */
  public boolean isClosed() {
    return closed;
  }

  /**
   * Closes this writer without committing.
   *
   * <p>Any written but uncommitted data will be lost. Use {@link #commit()} to persist data before
   * closing, or use try-with-resources and call commit within the try block.
   */
  @Override
  public void close() {
    if (!closed) {
      try {
        writer.close();
      } catch (IOException e) {
        // Log and swallow - we're closing anyway
      }
      closed = true;
    }
  }
}
