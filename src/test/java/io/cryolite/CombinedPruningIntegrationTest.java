package io.cryolite;

import static org.junit.jupiter.api.Assertions.*;

import io.cryolite.data.TableWriter;
import io.cryolite.filter.AndPredicate;
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
 * Integration test for M11.5 - Combined Pruning.
 *
 * <p>This test demonstrates the synergy between different pruning layers: 1. Partition Pruning
 * (M10): Skipping entire directories. 2. File-level Statistics Pruning (M11): Skipping files within
 * a partition based on min/max stats.
 *
 * <p>By combining both, a single query can reduce the scan volume significantly before any data
 * file is actually opened.
 */
@Tag("integration")
class CombinedPruningIntegrationTest extends AbstractIntegrationTest {

  @Test
  void testCombinedPartitionAndFilePruning() throws IOException {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;

    Namespace ns = Namespace.of("test_ns_combined_" + uniqueSuffix());
    TableIdentifier tableId = TableIdentifier.of(ns, "combined_pruning_table");

    try {
      nsCatalog.createNamespace(ns, new HashMap<>());

      Schema schema =
          new Schema(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.required(2, "region", Types.StringType.get()));

      // Partition by region
      PartitionSpec spec = PartitionSpec.builderFor(schema).identity("region").build();
      Table table = catalog.createTable(tableId, schema, spec);

      // Write 4 files total: 2 regions, 2 files per region with disjoint ID ranges.
      // EU: [1..10], [100..110]
      // US: [200..210], [300..310]
      writeBatch(table, 1L, 10L, "EU");
      table = catalog.loadTable(tableId);
      writeBatch(table, 100L, 110L, "EU");
      table = catalog.loadTable(tableId);
      writeBatch(table, 200L, 210L, "US");
      table = catalog.loadTable(tableId);
      writeBatch(table, 300L, 310L, "US");
      table = catalog.loadTable(tableId);

      // Verify setup
      List<FileScanTask> allTasks =
          StreamSupport.stream(table.newScan().planFiles().spliterator(), false)
              .collect(Collectors.toList());
      assertEquals(4, allTasks.size(), "Should have 4 data files total");

      // QUERY: region = 'EU' AND id = 105
      // Synergy Effect:
      // 1. Iceberg skips the entire 'US' directory (Partition Pruning).
      // 2. Inside 'EU', Iceberg skips the file with range [1..10] (File Stats Pruning).
      // Result: Exactly 1 file should be planned.
      ComparisonPredicate pRegion =
          new ComparisonPredicate("region", ComparisonOperator.EQUALS, "EU");
      ComparisonPredicate pId = new ComparisonPredicate("id", ComparisonOperator.EQUALS, 105L);
      AndPredicate combinedPredicate = new AndPredicate(List.of(pRegion, pId));

      try (CloseableIterable<VectorSchemaRoot> batches =
          engine.scan(tableId, List.of("id", "region"), combinedPredicate)) {
        int rowCount = 0;
        for (VectorSchemaRoot root : batches) {
          rowCount += root.getRowCount();
          for (int i = 0; i < root.getRowCount(); i++) {
            assertEquals("EU", root.getVector("region").getObject(i).toString());
            assertEquals(105L, root.getVector("id").getObject(i));
          }
        }
        assertEquals(1, rowCount, "Query should find exactly the record with id=105 in EU");
      }

      // THE PROOF: Check the scan plan directly
      TableScan filteredScan =
          table
              .newScan()
              .filter(
                  Expressions.and(
                      Expressions.equal("region", "EU"), Expressions.equal("id", 105L)));

      List<FileScanTask> filteredTasks =
          StreamSupport.stream(filteredScan.planFiles().spliterator(), false)
              .collect(Collectors.toList());

      assertEquals(1, filteredTasks.size(), "Planner should have pruned 3 of 4 files");
      assertTrue(
          filteredTasks.get(0).file().location().contains("region=EU"),
          "The planned file must be in the EU partition");

    } finally {
      if (catalog.tableExists(tableId)) {
        catalog.dropTable(tableId, true);
      }
      nsCatalog.dropNamespace(ns);
      engine.close();
    }
  }

  private void writeBatch(Table table, long minId, long maxId, String region) throws IOException {
    try (TableWriter writer = new TableWriter(table)) {
      GenericRecord record = GenericRecord.create(table.schema());
      for (long id = minId; id <= maxId; id++) {
        record.setField("id", id);
        record.setField("region", region);
        writer.write(record);
      }
      writer.commit();
    }
  }
}
