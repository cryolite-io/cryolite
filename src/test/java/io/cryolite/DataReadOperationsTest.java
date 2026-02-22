package io.cryolite;

import static org.junit.jupiter.api.Assertions.*;

import io.cryolite.data.TableReader;
import io.cryolite.data.TableWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for low-level data read operations.
 *
 * <p>Tests the full write-read-back cycle using TableWriter and TableReader. Verifies both the
 * Arrow batch API ({@link TableReader#readBatches()}) and the Record convenience API ({@link
 * TableReader#readRecords()}).
 */
class DataReadOperationsTest extends AbstractIntegrationTest {

  // ---- Arrow Batch API tests ----

  /** Tests readBatches() for a non-partitioned table with 10 records. */
  @Test
  void testReadBatchesFromNonPartitionedTable() throws IOException {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;
    Namespace ns = Namespace.of("test_ns_rb_" + uniqueSuffix());
    TableIdentifier tableId = TableIdentifier.of(ns, "batch_read_table");

    try {
      nsCatalog.createNamespace(ns, new HashMap<>());
      Table table = catalog.createTable(tableId, createTestSchema());
      writeTestRecords(table, 10);
      table = catalog.loadTable(tableId);

      try (TableReader reader = new TableReader(table);
          CloseableIterable<VectorSchemaRoot> batches = reader.readBatches()) {
        int totalRows = 0;
        Set<Long> ids = new HashSet<>();
        for (VectorSchemaRoot batch : batches) {
          BigIntVector idVec = (BigIntVector) batch.getVector("id");
          VarCharVector nameVec = (VarCharVector) batch.getVector("name");
          for (int i = 0; i < batch.getRowCount(); i++) {
            long id = idVec.get(i);
            ids.add(id);
            String name = new String(nameVec.get(i), StandardCharsets.UTF_8);
            assertEquals("User_" + id, name);
          }
          totalRows += batch.getRowCount();
        }
        assertEquals(10, totalRows);
        for (int i = 0; i < 10; i++) {
          assertTrue(ids.contains((long) i), "Missing id: " + i);
        }
      }
    } finally {
      cleanup(catalog, nsCatalog, tableId, ns, engine);
    }
  }

  /** Tests readBatches() returns zero batches for an empty table. */
  @Test
  void testReadBatchesFromEmptyTable() throws IOException {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;
    Namespace ns = Namespace.of("test_ns_rb_empty_" + uniqueSuffix());
    TableIdentifier tableId = TableIdentifier.of(ns, "empty_batch_table");

    try {
      nsCatalog.createNamespace(ns, new HashMap<>());
      catalog.createTable(tableId, createTestSchema());
      Table table = catalog.loadTable(tableId);

      try (TableReader reader = new TableReader(table);
          CloseableIterable<VectorSchemaRoot> batches = reader.readBatches()) {
        assertFalse(batches.iterator().hasNext(), "Empty table should yield zero batches");
      }
    } finally {
      cleanup(catalog, nsCatalog, tableId, ns, engine);
    }
  }

  /** Tests readBatches() reads all partitions of a partitioned table. */
  @Test
  void testReadBatchesFromPartitionedTable() throws IOException {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;
    Namespace ns = Namespace.of("test_ns_rb_part_" + uniqueSuffix());
    TableIdentifier tableId = TableIdentifier.of(ns, "part_batch_table");

    try {
      nsCatalog.createNamespace(ns, new HashMap<>());
      Schema schema = createPartitionedSchema();
      PartitionSpec spec = PartitionSpec.builderFor(schema).identity("category").build();
      Table table = catalog.createTable(tableId, schema, spec);
      writePartitionedRecords(table, 12);
      table = catalog.loadTable(tableId);

      try (TableReader reader = new TableReader(table);
          CloseableIterable<VectorSchemaRoot> batches = reader.readBatches()) {
        int totalRows = 0;
        Set<Long> ids = new HashSet<>();
        for (VectorSchemaRoot batch : batches) {
          BigIntVector idVec = (BigIntVector) batch.getVector("id");
          for (int i = 0; i < batch.getRowCount(); i++) {
            ids.add(idVec.get(i));
          }
          totalRows += batch.getRowCount();
        }
        assertEquals(12, totalRows);
        for (int i = 0; i < 12; i++) {
          assertTrue(ids.contains((long) i), "Missing id: " + i);
        }
      }
    } finally {
      cleanup(catalog, nsCatalog, tableId, ns, engine);
    }
  }

  // ---- Record API tests ----

  /** Tests readRecords() for a non-partitioned table with 10 records. */
  @Test
  void testReadRecordsFromNonPartitionedTable() throws IOException {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;
    Namespace ns = Namespace.of("test_ns_rr_" + uniqueSuffix());
    TableIdentifier tableId = TableIdentifier.of(ns, "record_read_table");

    try {
      nsCatalog.createNamespace(ns, new HashMap<>());
      Table table = catalog.createTable(tableId, createTestSchema());
      writeTestRecords(table, 10);
      table = catalog.loadTable(tableId);

      try (TableReader reader = new TableReader(table);
          CloseableIterable<Record> records = reader.readRecords()) {
        int count = 0;
        Set<Long> ids = new HashSet<>();
        for (Record record : records) {
          Long id = (Long) record.getField("id");
          ids.add(id);
          String name = (String) record.getField("name");
          assertEquals("User_" + id, name);
          count++;
        }
        assertEquals(10, count);
        for (int i = 0; i < 10; i++) {
          assertTrue(ids.contains((long) i), "Missing id: " + i);
        }
      }
    } finally {
      cleanup(catalog, nsCatalog, tableId, ns, engine);
    }
  }

  /** Tests readRecords() correctly handles null values. */
  @Test
  void testReadRecordsWithNullValues() throws IOException {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;
    Namespace ns = Namespace.of("test_ns_rr_null_" + uniqueSuffix());
    TableIdentifier tableId = TableIdentifier.of(ns, "null_record_table");

    try {
      nsCatalog.createNamespace(ns, new HashMap<>());
      Table table = catalog.createTable(tableId, createTestSchema());
      writeRecordsWithNulls(table, 5);
      table = catalog.loadTable(tableId);

      try (TableReader reader = new TableReader(table);
          CloseableIterable<Record> records = reader.readRecords()) {
        int count = 0;
        for (Record record : records) {
          Long id = (Long) record.getField("id");
          assertNotNull(id);
          // Name is null for all records written by writeRecordsWithNulls
          assertNull(record.getField("name"), "name should be null for id " + id);
          count++;
        }
        assertEquals(5, count);
      }
    } finally {
      cleanup(catalog, nsCatalog, tableId, ns, engine);
    }
  }

  /** Tests readRecords() reads all partitions of a partitioned table. */
  @Test
  void testReadRecordsFromPartitionedTable() throws IOException {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;
    Namespace ns = Namespace.of("test_ns_rr_part_" + uniqueSuffix());
    TableIdentifier tableId = TableIdentifier.of(ns, "part_record_table");

    try {
      nsCatalog.createNamespace(ns, new HashMap<>());
      Schema schema = createPartitionedSchema();
      PartitionSpec spec = PartitionSpec.builderFor(schema).identity("category").build();
      Table table = catalog.createTable(tableId, schema, spec);
      writePartitionedRecords(table, 12);
      table = catalog.loadTable(tableId);

      try (TableReader reader = new TableReader(table);
          CloseableIterable<Record> records = reader.readRecords()) {
        int count = 0;
        Set<Long> ids = new HashSet<>();
        Set<String> categories = new HashSet<>();
        for (Record record : records) {
          Long id = (Long) record.getField("id");
          ids.add(id);
          categories.add((String) record.getField("category"));
          count++;
        }
        assertEquals(12, count);
        assertEquals(Set.of("A", "B", "C"), categories);
        for (int i = 0; i < 12; i++) {
          assertTrue(ids.contains((long) i), "Missing id: " + i);
        }
      }
    } finally {
      cleanup(catalog, nsCatalog, tableId, ns, engine);
    }
  }

  // ---- Helper methods ----

  /** Creates a schema with an additional category field for partitioning. */
  private Schema createPartitionedSchema() {
    return new Schema(
        Types.NestedField.required(1, "id", Types.LongType.get()),
        Types.NestedField.optional(2, "name", Types.StringType.get()),
        Types.NestedField.required(3, "category", Types.StringType.get()));
  }

  /** Writes count test records (id=0..count-1, name=User_{id}). */
  private void writeTestRecords(Table table, int count) throws IOException {
    try (TableWriter writer = new TableWriter(table)) {
      GenericRecord record = GenericRecord.create(table.schema());
      for (int i = 0; i < count; i++) {
        record.setField("id", (long) i);
        record.setField("name", "User_" + i);
        writer.write(record);
      }
      writer.commit();
    }
  }

  /** Writes count records with null name values. */
  private void writeRecordsWithNulls(Table table, int count) throws IOException {
    try (TableWriter writer = new TableWriter(table)) {
      GenericRecord record = GenericRecord.create(table.schema());
      for (int i = 0; i < count; i++) {
        record.setField("id", (long) i);
        record.setField("name", null);
        writer.write(record);
      }
      writer.commit();
    }
  }

  /** Writes count partitioned records across categories A, B, C. */
  private void writePartitionedRecords(Table table, int count) throws IOException {
    String[] categories = {"A", "B", "C"};
    try (TableWriter writer = new TableWriter(table)) {
      GenericRecord record = GenericRecord.create(table.schema());
      for (int i = 0; i < count; i++) {
        record.setField("id", (long) i);
        record.setField("name", "User_" + i);
        record.setField("category", categories[i % 3]);
        writer.write(record);
      }
      writer.commit();
    }
  }

  /** Cleans up test resources (table, namespace, engine). */
  private void cleanup(
      Catalog catalog,
      SupportsNamespaces nsCatalog,
      TableIdentifier tableId,
      Namespace ns,
      CryoliteEngine engine) {
    try {
      if (catalog.tableExists(tableId)) {
        catalog.dropTable(tableId, true);
      }
    } catch (Exception e) {
      // ignore cleanup errors
    }
    try {
      nsCatalog.dropNamespace(ns);
    } catch (Exception e) {
      // ignore cleanup errors
    }
    try {
      engine.close();
    } catch (Exception e) {
      // ignore cleanup errors
    }
  }
}
