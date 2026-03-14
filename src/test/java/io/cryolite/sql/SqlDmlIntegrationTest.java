package io.cryolite.sql;

import static org.junit.jupiter.api.Assertions.*;

import io.cryolite.AbstractIntegrationTest;
import io.cryolite.CryoliteEngine;
import io.cryolite.data.TableReader;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for SQL DML execution via {@link SqlSession}.
 *
 * <p>These tests verify that {@code INSERT INTO} statements issued through the high-level SQL API
 * result in Iceberg snapshots that are readable through the low-level API (M6 requirement).
 *
 * <p>Requires Docker Compose services (Polaris + MinIO) to be running.
 */
class SqlDmlIntegrationTest extends AbstractIntegrationTest {

  /**
   * Verifies that a single {@code INSERT INTO ... VALUES} row is written to an Iceberg snapshot and
   * is correctly readable via the low-level {@link TableReader}.
   */
  @Test
  void insertSingleRowCreatesSnapshotAndIsReadable() throws Exception {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    String ns = "sql_dml_insert_" + uniqueSuffix();
    TableIdentifier tableId = TableIdentifier.of(Namespace.of(ns), "users");

    try (SqlSession session = engine.createSqlSession()) {
      session.execute(
          "CREATE TABLE " + ns + ".users (id BIGINT NOT NULL, name VARCHAR, age INTEGER)");

      session.execute("INSERT INTO " + ns + ".users VALUES (1, 'Alice', 30)");

      // Reload table to pick up the new snapshot
      var table = catalog.loadTable(tableId);
      assertNotNull(table.currentSnapshot(), "A snapshot must exist after INSERT");

      // Low-level read-back
      List<Record> records = readAllRecords(table);
      assertEquals(1, records.size(), "Exactly one row expected");
      Record row = records.get(0);
      assertEquals(1L, row.getField("id"));
      assertEquals("Alice", row.getField("name"));
      assertEquals(30, row.getField("age"));
    } finally {
      cleanup(catalog, tableId);
      engine.close();
    }
  }

  /**
   * Verifies that multiple rows supplied in a single {@code VALUES} clause are all written to the
   * same snapshot.
   */
  @Test
  void insertMultipleRowsAreAllReadable() throws Exception {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    String ns = "sql_dml_multi_" + uniqueSuffix();
    TableIdentifier tableId = TableIdentifier.of(Namespace.of(ns), "items");

    try (SqlSession session = engine.createSqlSession()) {
      session.execute("CREATE TABLE " + ns + ".items (id BIGINT NOT NULL, label VARCHAR)");

      session.execute(
          "INSERT INTO " + ns + ".items VALUES (10, 'apple'), (20, 'banana'), (30, 'cherry')");

      var table = catalog.loadTable(tableId);
      List<Record> records = readAllRecords(table);
      assertEquals(3, records.size(), "Three rows expected");

      List<Long> ids = records.stream().map(r -> (Long) r.getField("id")).toList();
      assertTrue(ids.containsAll(List.of(10L, 20L, 30L)), "All inserted ids must be present");
    } finally {
      cleanup(catalog, tableId);
      engine.close();
    }
  }

  /**
   * Verifies that an explicit column list is respected: only the specified columns receive values,
   * and unmentioned nullable columns become {@code null}.
   */
  @Test
  void insertWithExplicitColumnListSetsOnlyNamedColumns() throws Exception {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    String ns = "sql_dml_cols_" + uniqueSuffix();
    TableIdentifier tableId = TableIdentifier.of(Namespace.of(ns), "products");

    try (SqlSession session = engine.createSqlSession()) {
      session.execute(
          "CREATE TABLE " + ns + ".products (id BIGINT NOT NULL, name VARCHAR, price DOUBLE)");

      // Insert only id and name; price stays null
      session.execute("INSERT INTO " + ns + ".products (id, name) VALUES (99, 'Widget')");

      var table = catalog.loadTable(tableId);
      List<Record> records = readAllRecords(table);
      assertEquals(1, records.size());
      assertEquals(99L, records.get(0).getField("id"));
      assertEquals("Widget", records.get(0).getField("name"));
      assertNull(records.get(0).getField("price"), "Unspecified nullable column must be null");
    } finally {
      cleanup(catalog, tableId);
      engine.close();
    }
  }

  /** Verifies that inserting into a non-existent table throws a clear exception. */
  @Test
  void insertIntoNonExistentTableThrowsSqlExecutionException() {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    try (SqlSession session = engine.createSqlSession()) {
      SqlExecutionException ex =
          assertThrows(
              SqlExecutionException.class,
              () -> session.execute("INSERT INTO ghost_ns.ghost_table VALUES (1)"));
      assertTrue(
          ex.getMessage().contains("does not exist"),
          "Error must mention that table does not exist, was: " + ex.getMessage());
    } finally {
      engine.close();
    }
  }

  // --- helpers ---

  private List<Record> readAllRecords(org.apache.iceberg.Table table) throws Exception {
    List<Record> result = new ArrayList<>();
    try (TableReader reader = new TableReader(table);
        CloseableIterable<Record> records = reader.readRecords()) {
      for (Record r : records) {
        result.add(r);
      }
    }
    return result;
  }

  private void cleanup(Catalog catalog, TableIdentifier tableId) {
    try {
      catalog.dropTable(tableId, true);
      if (catalog instanceof SupportsNamespaces nsCatalog) {
        nsCatalog.dropNamespace(tableId.namespace());
      }
    } catch (Exception ignored) {
      // Cleanup failures are non-fatal
    }
  }
}
