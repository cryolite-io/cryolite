package io.cryolite;

import static org.junit.jupiter.api.Assertions.*;

import io.cryolite.data.TableWriter;
import io.cryolite.filter.ComparisonOperator;
import io.cryolite.filter.ComparisonPredicate;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Integration test for M11 - Parquet Reader Filter / File-level statistics pruning.
 *
 * <p>While M10 proves pruning at the partition level (skipping entire directories), this test
 * proves that Iceberg pushes the filter expression down to the file level, using min/max column
 * statistics cached in the manifest to skip files whose value range cannot match the predicate.
 *
 * <p>This is the foundation for Parquet row-group skipping: Iceberg's vectorized Parquet reader
 * receives the same filter and uses Parquet's own row-group statistics to skip blocks within a
 * file.
 */
@Tag("integration")
class ParquetReaderFilterIntegrationTest extends AbstractIntegrationTest {

  @Test
  void testFileSkippingByColumnStatistics() throws IOException {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;

    Namespace ns = Namespace.of("test_ns_parquet_" + uniqueSuffix());
    TableIdentifier tableId = TableIdentifier.of(ns, "parquet_filter_table");

    try {
      nsCatalog.createNamespace(ns, new HashMap<>());

      // UNPARTITIONED table - pruning will rely purely on column statistics, not directory layout
      Table table = catalog.createTable(tableId, createTestSchema());

      // Write three separate files with disjoint id ranges by committing between writes.
      // Each commit closes the current Parquet file and starts a new one, so Iceberg
      // ends up with three data files - each with its own min/max statistics in the manifest.
      writeBatch(table, 1L, 10L, "low");
      table = catalog.loadTable(tableId);
      writeBatch(table, 100L, 110L, "mid");
      table = catalog.loadTable(tableId);
      writeBatch(table, 200L, 210L, "high");
      table = catalog.loadTable(tableId);

      // Baseline: full scan should see all 3 files
      List<FileScanTask> allTasks =
          StreamSupport.stream(table.newScan().planFiles().spliterator(), false)
              .collect(Collectors.toList());
      assertEquals(3, allTasks.size(), "Should have exactly 3 data files (one per commit)");

      // FILTER: id = 105 - only the mid file (range 100..110) can contain this value.
      // Files for ranges 1..10 and 200..210 must be skipped by column statistics.
      ComparisonPredicate predicate =
          new ComparisonPredicate("id", ComparisonOperator.EQUALS, 105L);

      // End-to-end correctness: engine returns exactly the matching row
      try (CloseableIterable<VectorSchemaRoot> batches =
          engine.scan(tableId, List.of("id", "name"), predicate)) {
        int rowCount = 0;
        for (VectorSchemaRoot root : batches) {
          rowCount += root.getRowCount();
          for (int i = 0; i < root.getRowCount(); i++) {
            assertEquals(105L, root.getVector("id").getObject(i));
          }
        }
        assertEquals(1, rowCount, "Should find exactly 1 record with id = 105");
      }

      // THE PROOF: Iceberg's planner skips the two non-matching files using
      // file-level column statistics - no Parquet file is opened for them.
      TableScan filteredScan = table.newScan().filter(Expressions.equal("id", 105L));
      List<FileScanTask> filteredTasks =
          StreamSupport.stream(filteredScan.planFiles().spliterator(), false)
              .collect(Collectors.toList());

      assertEquals(
          1,
          filteredTasks.size(),
          "Iceberg should have pruned 2 of 3 files using column statistics (min/max)");

      // A predicate that no file can satisfy must prune everything.
      TableScan emptyScan = table.newScan().filter(Expressions.equal("id", 9999L));
      long emptyCount = StreamSupport.stream(emptyScan.planFiles().spliterator(), false).count();
      assertEquals(0, emptyCount, "An out-of-range predicate must prune all files");

    } finally {
      if (catalog.tableExists(tableId)) {
        catalog.dropTable(tableId, true);
      }
      nsCatalog.dropNamespace(ns);
      engine.close();
    }
  }

  /** Writes [minId..maxId] inclusive as a single commit, producing one Parquet file. */
  private void writeBatch(Table table, long minId, long maxId, String label) throws IOException {
    try (TableWriter writer = new TableWriter(table)) {
      GenericRecord record = GenericRecord.create(table.schema());
      for (long id = minId; id <= maxId; id++) {
        record.setField("id", id);
        record.setField("name", label + "-" + id);
        writer.write(record);
      }
      writer.commit();
    }
  }
}
