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
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Integration test for M11.5 - Row Group Pruning.
 *
 * <p>By default, Parquet row groups are 128MB. To verify pruning on small datasets, we force a tiny
 * row group size (4KB). We then write sorted data and verify that Iceberg/Parquet creates multiple
 * row groups (observable via split offsets) and can correctly filter data.
 */
@Tag("integration")
class RowGroupPruningIntegrationTest extends AbstractIntegrationTest {

  @Test
  void testRowGroupCreationAndPruning() throws IOException {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;

    Namespace ns = Namespace.of("test_ns_rowgroup_" + uniqueSuffix());
    TableIdentifier tableId = TableIdentifier.of(ns, "rowgroup_table");

    try {
      nsCatalog.createNamespace(ns, new HashMap<>());

      Schema schema =
          new Schema(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.optional(2, "name", Types.StringType.get()));

      // Create table with a tiny row group size to force multiple row groups within one file
      Table table = catalog.createTable(tableId, schema, PartitionSpec.unpartitioned());
      table
          .updateProperties()
          .set("write.parquet.row-group-size-bytes", "4096") // 4KB row groups
          .commit();

      // Write 2000 rows sorted by ID. This will produce several row groups.
      try (TableWriter writer = new TableWriter(table)) {
        GenericRecord record = GenericRecord.create(table.schema());
        for (long i = 1; i <= 2000; i++) {
          record.setField("id", i);
          record.setField("name", "User-" + i);
          writer.write(record);
        }
        writer.commit();
      }

      table = catalog.loadTable(tableId);
      List<FileScanTask> tasks =
          StreamSupport.stream(table.newScan().planFiles().spliterator(), false)
              .collect(Collectors.toList());

      assertEquals(1, tasks.size(), "Should have exactly 1 file");

      // PROOF: Check for multiple splits/row groups.
      // Iceberg's splitOffsets usually correspond to row group boundaries in Parquet.
      List<Long> offsets = tasks.get(0).file().splitOffsets();
      assertNotNull(offsets, "Split offsets should be populated");
      assertTrue(
          offsets.size() > 1,
          "Should have multiple row group offsets for 4KB target (found " + offsets.size() + ")");

      // VERIFY: Filtered scan works correctly (leveraging row group stats internally)
      ComparisonPredicate predicate =
          new ComparisonPredicate("id", ComparisonOperator.EQUALS, 1000L);
      try (CloseableIterable<VectorSchemaRoot> batches = engine.scan(tableId, predicate)) {
        int totalRows = 0;
        for (VectorSchemaRoot root : batches) {
          totalRows += root.getRowCount();
          for (int i = 0; i < root.getRowCount(); i++) {
            assertEquals(1000L, root.getVector("id").getObject(i));
          }
        }
        assertEquals(1, totalRows, "Should find exactly the record with id=1000");
      }

    } finally {
      if (catalog.tableExists(tableId)) {
        catalog.dropTable(tableId, true);
      }
      nsCatalog.dropNamespace(ns);
      engine.close();
    }
  }
}
