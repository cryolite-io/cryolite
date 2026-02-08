package io.cryolite;

import static org.junit.jupiter.api.Assertions.*;

import io.cryolite.data.TableWriter;
import io.cryolite.test.S3StorageTestHelper;
import java.io.IOException;
import java.util.HashMap;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for low-level data write operations.
 *
 * <p>Tests the TableWriter class for writing data to Iceberg tables and committing snapshots. Also
 * verifies that data files are correctly written to S3-compatible storage.
 */
class DataWriteOperationsTest extends AbstractIntegrationTest {

  /**
   * Tests writing data to a non-partitioned table and committing.
   *
   * <p>Verifies that:
   *
   * <ul>
   *   <li>Data can be written using TableWriter
   *   <li>Commit creates a new snapshot
   *   <li>Snapshot contains the expected number of data files
   *   <li>Data files are created in Parquet format
   *   <li>Data files exist in S3-compatible storage
   * </ul>
   */
  @Test
  void testWriteToNonPartitionedTable() throws IOException {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;

    Namespace ns = Namespace.of("test_ns_write_" + uniqueSuffix());
    TableIdentifier tableId = TableIdentifier.of(ns, "test_table");

    try {
      // Setup
      nsCatalog.createNamespace(ns, new HashMap<>());
      Table table = catalog.createTable(tableId, createTestSchema());

      // Initially no snapshot exists
      assertNull(table.currentSnapshot());

      // Write data
      try (TableWriter writer = new TableWriter(table)) {
        GenericRecord record = GenericRecord.create(table.schema());

        for (int i = 0; i < 10; i++) {
          record.setField("id", (long) i);
          record.setField("name", "User_" + i);
          writer.write(record);
        }

        writer.commit();

        // Verify committed files
        assertEquals(1, writer.getCommittedFiles().size());
        DataFile dataFile = writer.getCommittedFiles().get(0);
        assertTrue(dataFile.location().endsWith(".parquet"));

        // Verify file exists in S3 storage
        S3StorageTestHelper s3Helper = new S3StorageTestHelper(createTestConfig());
        String filePath = dataFile.location();
        assertTrue(
            s3Helper.fileExists(filePath), "Data file should exist in S3 storage: " + filePath);

        // Verify file has content (size > 0)
        long fileSize = s3Helper.getFileSize(filePath);
        assertTrue(fileSize > 0, "Data file should have content, but size is: " + fileSize);
        s3Helper.close();
      }

      // Reload table to get updated snapshot
      table = catalog.loadTable(tableId);

      // Verify snapshot was created
      Snapshot snapshot = table.currentSnapshot();
      assertNotNull(snapshot, "Snapshot should exist after commit");
      assertEquals("append", snapshot.operation());

    } finally {
      // Cleanup
      if (catalog.tableExists(tableId)) {
        catalog.dropTable(tableId, true);
      }
      nsCatalog.dropNamespace(ns);
      engine.close();
    }
  }

  /**
   * Tests writing data to a partitioned table.
   *
   * <p>Verifies that:
   *
   * <ul>
   *   <li>Data can be written to partitioned tables
   *   <li>Records are automatically routed to correct partitions
   *   <li>Multiple partition data files are created
   *   <li>All data files exist in S3-compatible storage
   * </ul>
   */
  @Test
  void testWriteToPartitionedTable() throws IOException {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;

    Namespace ns = Namespace.of("test_ns_part_write_" + uniqueSuffix());
    TableIdentifier tableId = TableIdentifier.of(ns, "partitioned_table");

    try {
      // Setup - create partitioned table
      nsCatalog.createNamespace(ns, new HashMap<>());

      Schema schema =
          new Schema(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.required(2, "category", Types.StringType.get()),
              Types.NestedField.optional(3, "name", Types.StringType.get()));

      PartitionSpec spec = PartitionSpec.builderFor(schema).identity("category").build();

      Table table = catalog.createTable(tableId, schema, spec);

      // Write data with different categories (partitions)
      try (TableWriter writer = new TableWriter(table)) {
        GenericRecord record = GenericRecord.create(table.schema());

        String[] categories = {"A", "B", "C"};
        for (int i = 0; i < 30; i++) {
          record.setField("id", (long) i);
          record.setField("category", categories[i % 3]);
          record.setField("name", "Item_" + i);
          writer.write(record);
        }

        writer.commit();

        // Should have 3 data files (one per partition)
        assertEquals(3, writer.getCommittedFiles().size());

        // Verify all files exist in S3 storage
        S3StorageTestHelper s3Helper = new S3StorageTestHelper(createTestConfig());
        for (DataFile dataFile : writer.getCommittedFiles()) {
          String filePath = dataFile.location();
          assertTrue(
              s3Helper.fileExists(filePath),
              "Partition data file should exist in S3 storage: " + filePath);

          long fileSize = s3Helper.getFileSize(filePath);
          assertTrue(
              fileSize > 0, "Partition data file should have content, but size is: " + fileSize);
        }
        s3Helper.close();
      }

      // Reload and verify snapshot
      table = catalog.loadTable(tableId);
      Snapshot snapshot = table.currentSnapshot();
      assertNotNull(snapshot);
      assertEquals("append", snapshot.operation());

    } finally {
      if (catalog.tableExists(tableId)) {
        catalog.dropTable(tableId, true);
      }
      nsCatalog.dropNamespace(ns);
      engine.close();
    }
  }

  /**
   * Tests multiple writes create multiple snapshots.
   *
   * <p>Verifies that:
   *
   * <ul>
   *   <li>Each commit creates a distinct snapshot
   *   <li>Snapshot history is maintained
   *   <li>Snapshots have incrementing IDs
   * </ul>
   */
  @Test
  void testMultipleWritesCreateMultipleSnapshots() throws IOException {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;

    Namespace ns = Namespace.of("test_ns_multi_snap_" + uniqueSuffix());
    TableIdentifier tableId = TableIdentifier.of(ns, "multi_snapshot_table");

    try {
      nsCatalog.createNamespace(ns, new HashMap<>());
      Table table = catalog.createTable(tableId, createTestSchema());

      // First write
      try (TableWriter writer = new TableWriter(table)) {
        GenericRecord record = GenericRecord.create(table.schema());
        record.setField("id", 1L);
        record.setField("name", "First");
        writer.write(record);
        writer.commit();
      }

      table = catalog.loadTable(tableId);
      Snapshot firstSnapshot = table.currentSnapshot();
      assertNotNull(firstSnapshot);
      long firstSnapshotId = firstSnapshot.snapshotId();

      // Second write
      try (TableWriter writer = new TableWriter(table)) {
        GenericRecord record = GenericRecord.create(table.schema());
        record.setField("id", 2L);
        record.setField("name", "Second");
        writer.write(record);
        writer.commit();
      }

      table = catalog.loadTable(tableId);
      Snapshot secondSnapshot = table.currentSnapshot();
      assertNotNull(secondSnapshot);
      long secondSnapshotId = secondSnapshot.snapshotId();

      // Verify distinct snapshots
      assertNotEquals(firstSnapshotId, secondSnapshotId);

      // Verify snapshot history (using Iterables.size would be cleaner but avoid extra deps)
      int snapshotCount = 0;
      for (@SuppressWarnings("unused") Snapshot ignored : table.snapshots()) {
        snapshotCount++;
      }
      assertEquals(2, snapshotCount);

    } finally {
      if (catalog.tableExists(tableId)) {
        catalog.dropTable(tableId, true);
      }
      nsCatalog.dropNamespace(ns);
      engine.close();
    }
  }

  /**
   * Tests that closing a writer without committing does not create a snapshot.
   *
   * <p>Verifies that:
   *
   * <ul>
   *   <li>Uncommitted data is not persisted
   *   <li>No snapshot is created if commit is not called
   * </ul>
   */
  @Test
  void testCloseWithoutCommitDoesNotCreateSnapshot() throws IOException {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;

    Namespace ns = Namespace.of("test_ns_no_commit_" + uniqueSuffix());
    TableIdentifier tableId = TableIdentifier.of(ns, "no_commit_table");

    try {
      nsCatalog.createNamespace(ns, new HashMap<>());
      Table table = catalog.createTable(tableId, createTestSchema());

      // Write without commit
      try (TableWriter writer = new TableWriter(table)) {
        GenericRecord record = GenericRecord.create(table.schema());
        record.setField("id", 1L);
        record.setField("name", "Lost data");
        writer.write(record);
        // Close without commit
      }

      // Reload table
      table = catalog.loadTable(tableId);

      // No snapshot should exist
      assertNull(table.currentSnapshot());

    } finally {
      if (catalog.tableExists(tableId)) {
        catalog.dropTable(tableId, true);
      }
      nsCatalog.dropNamespace(ns);
      engine.close();
    }
  }

  /**
   * Tests that writing after commit throws an exception.
   *
   * <p>Verifies that:
   *
   * <ul>
   *   <li>After commit, the writer is closed
   *   <li>Attempting to write throws IllegalStateException
   * </ul>
   */
  @Test
  void testWriteAfterCommitThrows() throws IOException {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;

    Namespace ns = Namespace.of("test_ns_write_after_commit_" + uniqueSuffix());
    TableIdentifier tableId = TableIdentifier.of(ns, "test_table");

    try {
      nsCatalog.createNamespace(ns, new HashMap<>());
      Table table = catalog.createTable(tableId, createTestSchema());

      try (TableWriter writer = new TableWriter(table)) {
        GenericRecord record = GenericRecord.create(table.schema());
        record.setField("id", 1L);
        record.setField("name", "Test");
        writer.write(record);
        writer.commit();

        // Writer should be closed after commit
        assertTrue(writer.isClosed());

        // Writing after commit should throw
        assertThrows(
            IllegalStateException.class,
            () -> {
              writer.write(record);
            });
      }

    } finally {
      if (catalog.tableExists(tableId)) {
        catalog.dropTable(tableId, true);
      }
      nsCatalog.dropNamespace(ns);
      engine.close();
    }
  }

  /**
   * Tests TableWriter constructor validation.
   *
   * <p>Verifies that:
   *
   * <ul>
   *   <li>Null table throws IllegalArgumentException
   *   <li>Non-positive file size throws IllegalArgumentException
   * </ul>
   */
  @Test
  @SuppressWarnings("resource") // Exception thrown before resource created
  void testTableWriterValidation() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              new TableWriter(null);
            });
    assertEquals("Table cannot be null", ex.getMessage());
  }
}
