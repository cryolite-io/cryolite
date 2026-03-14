package io.cryolite.sql;

import static org.junit.jupiter.api.Assertions.*;

import io.cryolite.AbstractIntegrationTest;
import io.cryolite.CryoliteEngine;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for SQL DDL execution via {@link SqlSession}.
 *
 * <p>These tests verify that SQL {@code CREATE TABLE} statements issued through the high-level SQL
 * API result in tables that are visible through the low-level Iceberg catalog API (M5 requirement).
 *
 * <p>Requires Docker Compose services (Polaris + MinIO) to be running.
 */
class SqlDdlIntegrationTest extends AbstractIntegrationTest {

  /**
   * Verifies that a {@code CREATE TABLE} SQL statement creates a table visible via the low-level
   * catalog API with the correct schema.
   */
  @Test
  void createTableViaSqlIsVisibleInCatalog() {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    String ns = "sql_ddl_create_" + uniqueSuffix();
    TableIdentifier tableId = TableIdentifier.of(Namespace.of(ns), "events");

    try (SqlSession session = engine.createSqlSession()) {
      session.execute(
          "CREATE TABLE "
              + ns
              + ".events ("
              + "  event_id  BIGINT    NOT NULL,"
              + "  user_id   BIGINT,"
              + "  payload   VARCHAR"
              + ")");

      assertTrue(catalog.tableExists(tableId), "Table must be visible in low-level catalog");

      var loadedTable = catalog.loadTable(tableId);
      var schema = loadedTable.schema();
      assertEquals(3, schema.columns().size());

      Types.NestedField eventId = schema.findField("event_id");
      assertNotNull(eventId, "event_id column must exist");
      assertEquals(Types.LongType.get(), eventId.type());
      assertTrue(eventId.isRequired(), "event_id must be NOT NULL (required)");

      Types.NestedField userId = schema.findField("user_id");
      assertNotNull(userId, "user_id column must exist");
      assertEquals(Types.LongType.get(), userId.type());
      assertTrue(userId.isOptional(), "user_id must be nullable (optional)");

      Types.NestedField payload = schema.findField("payload");
      assertNotNull(payload, "payload column must exist");
      assertEquals(Types.StringType.get(), payload.type());
      assertTrue(payload.isOptional(), "payload must be nullable (optional)");
    } finally {
      cleanup(catalog, tableId);
      engine.close();
    }
  }

  /**
   * Verifies that {@code CREATE TABLE IF NOT EXISTS} silently succeeds when the table already
   * exists (no exception thrown, no data loss).
   */
  @Test
  void createTableIfNotExistsDoesNotThrowWhenTableExists() {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    String ns = "sql_ddl_ifnotexists_" + uniqueSuffix();
    TableIdentifier tableId = TableIdentifier.of(Namespace.of(ns), "items");

    String createSql = "CREATE TABLE IF NOT EXISTS " + ns + ".items (id BIGINT NOT NULL)";

    try (SqlSession session = engine.createSqlSession()) {
      session.execute(createSql);
      assertTrue(catalog.tableExists(tableId));

      // Second execution must NOT throw
      assertDoesNotThrow(() -> session.execute(createSql));
      assertTrue(catalog.tableExists(tableId));
    } finally {
      cleanup(catalog, tableId);
      engine.close();
    }
  }

  /**
   * Verifies that {@code CREATE TABLE} without {@code IF NOT EXISTS} throws when the table already
   * exists, matching the behavior of the low-level API.
   */
  @Test
  void createDuplicateTableWithoutIfNotExistsThrows() {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    String ns = "sql_ddl_dup_" + uniqueSuffix();
    TableIdentifier tableId = TableIdentifier.of(Namespace.of(ns), "orders");

    String createSql = "CREATE TABLE " + ns + ".orders (id BIGINT NOT NULL)";

    try (SqlSession session = engine.createSqlSession()) {
      session.execute(createSql);
      assertThrows(
          org.apache.iceberg.exceptions.AlreadyExistsException.class,
          () -> session.execute(createSql));
    } finally {
      cleanup(catalog, tableId);
      engine.close();
    }
  }

  /** Verifies that a table name without namespace qualifier produces a clear error. */
  @Test
  void createTableWithoutNamespaceThrowsSqlExecutionException() {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());

    try (SqlSession session = engine.createSqlSession()) {
      SqlExecutionException ex =
          assertThrows(
              SqlExecutionException.class,
              () -> session.execute("CREATE TABLE unqualified_table (id BIGINT)"));
      assertTrue(
          ex.getMessage().contains("fully qualified"),
          "Error message should mention namespace qualification");
    } finally {
      engine.close();
    }
  }

  private void cleanup(Catalog catalog, TableIdentifier tableId) {
    try {
      catalog.dropTable(tableId, true);
      if (catalog instanceof SupportsNamespaces nsCatalog) {
        nsCatalog.dropNamespace(tableId.namespace());
      }
    } catch (Exception ignored) {
      // Ignore cleanup failures
    }
  }
}
