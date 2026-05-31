package io.cryolite;

import static org.junit.jupiter.api.Assertions.*;

import io.cryolite.data.TableWriter;
import io.cryolite.filter.BatchPredicate;
import io.cryolite.filter.ComparisonOperator;
import io.cryolite.filter.ComparisonPredicate;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Integration test for M10 - Partition Pruning.
 *
 * <p>Verifies that filtering on partition columns correctly prunes files/manifests at the Iceberg
 * level, reducing I/O.
 */
@Tag("integration")
class PartitionPruningIntegrationTest extends AbstractIntegrationTest {

  @Test
  void testPartitionPruning() throws IOException {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;

    Namespace ns = Namespace.of("test_ns_pruning_" + uniqueSuffix());
    TableIdentifier tableId = TableIdentifier.of(ns, "pruning_table");

    try {
      // Setup - create partitioned table
      nsCatalog.createNamespace(ns, new HashMap<>());

      Schema schema =
          new Schema(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.required(2, "category", Types.StringType.get()));

      // Partition by identity(category)
      PartitionSpec spec = PartitionSpec.builderFor(schema).identity("category").build();
      Table table = catalog.createTable(tableId, schema, spec);

      // 1. Write data to 3 distinct partitions
      try (TableWriter writer = new TableWriter(table)) {
        GenericRecord record = GenericRecord.create(table.schema());

        // Partition A
        record.setField("id", 1L);
        record.setField("category", "A");
        writer.write(record);

        // Partition B
        record.setField("id", 2L);
        record.setField("category", "B");
        writer.write(record);

        // Partition C
        record.setField("id", 3L);
        record.setField("category", "C");
        writer.write(record);

        writer.commit();
      }

      // Reload table to ensure metadata is fresh
      table = catalog.loadTable(tableId);

      // 2. Verify all 3 files are present in a full scan (baseline)
      TableScan fullScan = table.newScan();
      List<FileScanTask> allTasks =
          StreamSupport.stream(fullScan.planFiles().spliterator(), false)
              .collect(Collectors.toList());
      assertEquals(3, allTasks.size(), "Should have exactly 3 data files (one per partition)");

      // 3. Execute a scan with a filter that matches only one partition
      ComparisonPredicate predicate =
          new ComparisonPredicate("category", ComparisonOperator.EQUALS, "A");

      // Use CryoliteEngine to perform the scan
      try (CloseableIterable<VectorSchemaRoot> batches =
          engine.scan(tableId, List.of("id", "category"), predicate)) {
        int rowCount = 0;
        for (VectorSchemaRoot root : batches) {
          rowCount += root.getRowCount();
          // Verify content: only category 'A' should be present
          for (int i = 0; i < root.getRowCount(); i++) {
            assertEquals(
                "A",
                root.getVector("category").getObject(i).toString(),
                "Only records from partition A should be returned");
          }
        }
        assertEquals(1, rowCount, "Should find exactly 1 record in partition A");
      }

      // 4. THE PRUNING PROOF:
      // Manually plan files with the same Iceberg expression and verify pruning
      TableScan filteredScan = table.newScan().filter(Expressions.equal("category", "A"));
      List<FileScanTask> filteredTasks =
          StreamSupport.stream(filteredScan.planFiles().spliterator(), false)
              .collect(Collectors.toList());

      assertEquals(
          1,
          filteredTasks.size(),
          "Iceberg should have pruned 2 files, leaving only 1 for partition A");
      assertTrue(
          filteredTasks.get(0).file().location().contains("category=A"),
          "The remaining data file must belong to partition A");

    } finally {
      // Cleanup
      if (catalog.tableExists(tableId)) {
        catalog.dropTable(tableId, true);
      }
      nsCatalog.dropNamespace(ns);
      engine.close();
    }
  }

  @Test
  void testBucketPartitionPruning() throws IOException {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;

    Namespace ns = Namespace.of("test_ns_bucket_" + uniqueSuffix());
    TableIdentifier tableId = TableIdentifier.of(ns, "bucket_table");

    try {
      nsCatalog.createNamespace(ns, new HashMap<>());

      Schema schema =
          new Schema(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.required(2, "name", Types.StringType.get()));

      // Partition by identity(id)
      PartitionSpec spec = PartitionSpec.builderFor(schema).identity("id").build();
      Table table = catalog.createTable(tableId, schema, spec);

      // Write data - id 1 and id 2 will be in different partitions
      try (TableWriter writer = new TableWriter(table)) {
        GenericRecord record = GenericRecord.create(table.schema());

        record.setField("id", 1L);
        record.setField("name", "User 1");
        writer.write(record);

        record.setField("id", 2L);
        record.setField("name", "User 2");
        writer.write(record);

        writer.commit();
      }

      table = catalog.loadTable(tableId);

      // Verify we have 2 files
      assertEquals(
          2, StreamSupport.stream(table.newScan().planFiles().spliterator(), false).count());

      // Filter by id = 1
      ComparisonPredicate predicate = new ComparisonPredicate("id", ComparisonOperator.EQUALS, 1L);

      try (CloseableIterable<VectorSchemaRoot> batches = engine.scan(tableId, predicate)) {
        int rowCount = 0;
        for (VectorSchemaRoot root : batches) {
          rowCount += root.getRowCount();
          for (int i = 0; i < root.getRowCount(); i++) {
            assertEquals(1L, root.getVector("id").getObject(i));
          }
        }
        assertEquals(1, rowCount);
      }

      // Check pruning - identity(id) with id=1 should point to one partition
      TableScan filteredScan = table.newScan().filter(Expressions.equal("id", 1L));
      long fileCount = StreamSupport.stream(filteredScan.planFiles().spliterator(), false).count();

      assertEquals(1, fileCount, "Identity partitioning should have pruned one file");

    } finally {
      if (catalog.tableExists(tableId)) {
        catalog.dropTable(tableId, true);
      }
      nsCatalog.dropNamespace(ns);
      engine.close();
    }
  }

  @Test
  void testResidualOnlyFiltering() throws IOException {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;

    Namespace ns = Namespace.of("test_ns_residual_" + uniqueSuffix());
    TableIdentifier tableId = TableIdentifier.of(ns, "residual_table");

    try {
      nsCatalog.createNamespace(ns, new HashMap<>());
      Table table = catalog.createTable(tableId, createTestSchema());

      // Write data
      try (TableWriter writer = new TableWriter(table)) {
        GenericRecord record = GenericRecord.create(table.schema());
        record.setField("id", 1L);
        record.setField("name", "Match");
        writer.write(record);

        record.setField("id", 2L);
        record.setField("name", "NoMatch");
        writer.write(record);
        writer.commit();
      }

      // A predicate that is NOT pushable (doesn't override toIcebergExpression)
      BatchPredicate residualOnly =
          batch -> {
            java.util.BitSet bs = new java.util.BitSet(batch.getRowCount());
            var idVector = batch.getVector("id");
            for (int i = 0; i < batch.getRowCount(); i++) {
              if (Long.valueOf(1L).equals(idVector.getObject(i))) {
                bs.set(i);
              }
            }
            return bs;
          };

      try (CloseableIterable<VectorSchemaRoot> batches = engine.scan(tableId, residualOnly)) {
        int rowCount = 0;
        for (VectorSchemaRoot root : batches) {
          rowCount += root.getRowCount();
          for (int i = 0; i < root.getRowCount(); i++) {
            assertEquals(1L, root.getVector("id").getObject(i));
          }
        }
        assertEquals(1, rowCount, "Residual filter should have found the record");
      }

      // Verification: Iceberg scan without filter should see 1 file
      table = catalog.loadTable(tableId);
      assertEquals(
          1, StreamSupport.stream(table.newScan().planFiles().spliterator(), false).count());

    } finally {
      if (catalog.tableExists(tableId)) {
        catalog.dropTable(tableId, true);
      }
      nsCatalog.dropNamespace(ns);
      engine.close();
    }
  }
}
