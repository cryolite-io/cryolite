package io.cryolite.sql;

import static org.junit.jupiter.api.Assertions.*;

import io.cryolite.AbstractIntegrationTest;
import io.cryolite.CryoliteEngine;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for SQL SELECT execution via {@link SqlSession}.
 *
 * <p>These tests verify that {@code SELECT *} statements issued through the high-level SQL API
 * return correct Arrow columnar batches (M7 requirement).
 *
 * <p>Requires Docker Compose services (Polaris + MinIO) to be running.
 */
class SqlSelectIntegrationTest extends AbstractIntegrationTest {

  /**
   * Verifies the full SQL lifecycle: CREATE TABLE → INSERT → SELECT * → Arrow result. This is the
   * core M7 acceptance criterion.
   */
  @Test
  void selectStarReturnsInsertedDataAsArrowBatches() throws Exception {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    String ns = "sql_select_star_" + uniqueSuffix();
    TableIdentifier tableId = TableIdentifier.of(Namespace.of(ns), "users");

    try (SqlSession session = engine.createSqlSession()) {
      session.execute(
          "CREATE TABLE " + ns + ".users (id BIGINT NOT NULL, name VARCHAR, age INTEGER)");

      session.execute(
          "INSERT INTO " + ns + ".users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Carol', 35)");

      // Query via SQL and collect Arrow data
      List<Long> ids = new ArrayList<>();
      List<String> names = new ArrayList<>();

      try (CloseableIterable<VectorSchemaRoot> batches =
          session.query("SELECT * FROM " + ns + ".users")) {
        for (VectorSchemaRoot batch : batches) {
          assertTrue(batch.getRowCount() > 0, "Each batch must have at least one row");

          BigIntVector idVector = (BigIntVector) batch.getVector("id");
          VarCharVector nameVector = (VarCharVector) batch.getVector("name");

          for (int i = 0; i < batch.getRowCount(); i++) {
            ids.add(idVector.get(i));
            names.add(new String(nameVector.get(i)));
          }
        }
      }

      assertEquals(3, ids.size(), "Should have 3 rows");
      assertTrue(ids.containsAll(List.of(1L, 2L, 3L)), "All inserted IDs must be present");
      assertTrue(
          names.containsAll(List.of("Alice", "Bob", "Carol")),
          "All inserted names must be present");
    } finally {
      cleanup(catalog, tableId);
      engine.close();
    }
  }

  /** Verifies that SELECT * on an empty table (no snapshot) returns an empty iterable. */
  @Test
  void selectStarFromEmptyTableReturnsNoBatches() throws Exception {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    String ns = "sql_select_empty_" + uniqueSuffix();
    TableIdentifier tableId = TableIdentifier.of(Namespace.of(ns), "empty_table");

    try (SqlSession session = engine.createSqlSession()) {
      session.execute("CREATE TABLE " + ns + ".empty_table (id BIGINT NOT NULL, name VARCHAR)");

      int batchCount = 0;
      try (CloseableIterable<VectorSchemaRoot> batches =
          session.query("SELECT * FROM " + ns + ".empty_table")) {
        for (VectorSchemaRoot ignored : batches) {
          batchCount++;
        }
      }

      assertEquals(0, batchCount, "Empty table should produce zero batches");
    } finally {
      cleanup(catalog, tableId);
      engine.close();
    }
  }

  /** Verifies that SELECT from a non-existent table throws a clear exception. */
  @Test
  void selectFromNonExistentTableThrowsSqlExecutionException() {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    try (SqlSession session = engine.createSqlSession()) {
      assertThrows(Exception.class, () -> session.query("SELECT * FROM ghost_ns.ghost_table"));
    } finally {
      engine.close();
    }
  }

  // --- helpers ---

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
