package io.cryolite;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/** Tests for table operations via the Iceberg Catalog API. */
class TableOperationsTest extends AbstractIntegrationTest {

  /**
   * Tests that a table can be successfully created in an existing namespace.
   *
   * <p>Verifies that:
   *
   * <ul>
   *   <li>A table can be created with a valid schema
   *   <li>The created table is immediately visible via tableExists()
   * </ul>
   *
   * <p>This is a positive test for the basic table creation workflow.
   */
  @Test
  void testCreateTable() {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;

    Namespace ns = Namespace.of("test_ns_tbl_create_" + uniqueSuffix());
    TableIdentifier tableId = TableIdentifier.of(ns, "test_table");

    try {
      nsCatalog.createNamespace(ns, new HashMap<>());

      Table table = catalog.createTable(tableId, createTestSchema());
      assertNotNull(table);
      assertTrue(catalog.tableExists(tableId));
    } finally {
      try {
        catalog.dropTable(tableId, true);
        nsCatalog.dropNamespace(ns);
      } catch (Exception e) {
        // Ignore cleanup errors
      }
      engine.close();
    }
  }

  /**
   * Tests that multiple tables can be created in a namespace and listed.
   *
   * <p>Verifies that:
   *
   * <ul>
   *   <li>Multiple tables can be created independently in the same namespace
   *   <li>listTables() returns all created tables
   * </ul>
   *
   * <p>This tests the table listing functionality and ensures that the catalog correctly tracks
   * multiple tables within a namespace.
   */
  @Test
  void testListTables() {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;

    Namespace ns = Namespace.of("test_ns_tbl_list_" + uniqueSuffix());
    TableIdentifier table1 = TableIdentifier.of(ns, "table_1");
    TableIdentifier table2 = TableIdentifier.of(ns, "table_2");

    try {
      nsCatalog.createNamespace(ns, new HashMap<>());
      catalog.createTable(table1, createTestSchema());
      catalog.createTable(table2, createTestSchema());

      List<TableIdentifier> tables = catalog.listTables(ns);
      assertTrue(tables.contains(table1));
      assertTrue(tables.contains(table2));
    } finally {
      try {
        catalog.dropTable(table1, true);
        catalog.dropTable(table2, true);
        nsCatalog.dropNamespace(ns);
      } catch (Exception e) {
        // Ignore cleanup errors
      }
      engine.close();
    }
  }

  /**
   * Tests that a table can be loaded from the catalog and its schema is correct.
   *
   * <p>Verifies that:
   *
   * <ul>
   *   <li>A created table can be loaded via loadTable()
   *   <li>The loaded table has the correct schema (2 columns in test schema)
   * </ul>
   *
   * <p>This tests the table loading functionality and schema preservation.
   */
  @Test
  void testLoadTable() {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;

    Namespace ns = Namespace.of("test_ns_tbl_load_" + uniqueSuffix());
    TableIdentifier tableId = TableIdentifier.of(ns, "test_table");

    try {
      nsCatalog.createNamespace(ns, new HashMap<>());
      catalog.createTable(tableId, createTestSchema());

      Table loadedTable = catalog.loadTable(tableId);
      assertNotNull(loadedTable);
      assertEquals(2, loadedTable.schema().columns().size());
    } finally {
      try {
        catalog.dropTable(tableId, true);
        nsCatalog.dropNamespace(ns);
      } catch (Exception e) {
        // Ignore cleanup errors
      }
      engine.close();
    }
  }

  /**
   * Tests the complete lifecycle of a table: create, verify existence, and drop.
   *
   * <p>Verifies that:
   *
   * <ul>
   *   <li>A non-existent table returns false from tableExists()
   *   <li>After creation, tableExists() returns true
   *   <li>After dropping, tableExists() returns false again
   * </ul>
   *
   * <p>This tests the full CRUD lifecycle for tables.
   */
  @Test
  void testTableExistsAndDrop() {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;

    Namespace ns = Namespace.of("test_ns_tbl_exists_" + uniqueSuffix());
    TableIdentifier tableId = TableIdentifier.of(ns, "test_table");

    try {
      nsCatalog.createNamespace(ns, new HashMap<>());

      assertFalse(catalog.tableExists(tableId));

      catalog.createTable(tableId, createTestSchema());
      assertTrue(catalog.tableExists(tableId));

      catalog.dropTable(tableId, true);
      assertFalse(catalog.tableExists(tableId));
    } finally {
      try {
        nsCatalog.dropNamespace(ns);
      } catch (Exception e) {
        // Ignore cleanup errors
      }
      engine.close();
    }
  }

  // ========== Negative Tests ==========

  /**
   * Tests that creating a table in a non-existent namespace throws NoSuchNamespaceException.
   *
   * <p>This is a negative test that verifies the catalog correctly rejects table creation attempts
   * in non-existent namespaces, preventing orphaned tables.
   */
  @Test
  void testCreateTableInNonExistentNamespace() {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();

    Namespace ns = Namespace.of("test_ns_nonexistent_tbl_" + uniqueSuffix());
    TableIdentifier tableId = TableIdentifier.of(ns, "test_table");

    try {
      assertThrows(
          org.apache.iceberg.exceptions.NoSuchNamespaceException.class,
          () -> catalog.createTable(tableId, createTestSchema()));
    } finally {
      engine.close();
    }
  }

  /**
   * Tests that creating a table with a name that already exists throws AlreadyExistsException.
   *
   * <p>This is a negative test that verifies the catalog correctly rejects duplicate table creation
   * attempts, preventing accidental overwrites.
   */
  @Test
  void testCreateDuplicateTable() {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;

    Namespace ns = Namespace.of("test_ns_dup_tbl_" + uniqueSuffix());
    TableIdentifier tableId = TableIdentifier.of(ns, "test_table");

    try {
      nsCatalog.createNamespace(ns, new HashMap<>());
      catalog.createTable(tableId, createTestSchema());

      assertThrows(
          org.apache.iceberg.exceptions.AlreadyExistsException.class,
          () -> catalog.createTable(tableId, createTestSchema()));
    } finally {
      try {
        catalog.dropTable(tableId, true);
        nsCatalog.dropNamespace(ns);
      } catch (Exception e) {
        // Ignore cleanup errors
      }
      engine.close();
    }
  }

  /**
   * Tests that dropping a non-existent table returns false (no exception).
   *
   * <p>This verifies that the catalog gracefully handles attempts to drop tables that don't exist,
   * returning false instead of throwing an exception. This is the expected behavior in Polaris.
   */
  @Test
  void testDropNonExistentTable() {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;

    Namespace ns = Namespace.of("test_ns_drop_nonexistent_" + uniqueSuffix());
    TableIdentifier tableId = TableIdentifier.of(ns, "nonexistent_table");

    try {
      nsCatalog.createNamespace(ns, new HashMap<>());

      assertFalse(catalog.tableExists(tableId));
      // Dropping a non-existent table returns false (no exception in Polaris)
      assertFalse(catalog.dropTable(tableId, true));
    } finally {
      try {
        nsCatalog.dropNamespace(ns);
      } catch (Exception e) {
        // Ignore cleanup errors
      }
      engine.close();
    }
  }

  /**
   * Tests that loading a non-existent table throws NoSuchTableException.
   *
   * <p>This is a negative test that verifies the catalog correctly rejects attempts to load tables
   * that don't exist, preventing undefined behavior.
   */
  @Test
  void testLoadNonExistentTable() {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;

    Namespace ns = Namespace.of("test_ns_load_nonexistent_" + uniqueSuffix());
    TableIdentifier tableId = TableIdentifier.of(ns, "nonexistent_table");

    try {
      nsCatalog.createNamespace(ns, new HashMap<>());

      assertThrows(
          org.apache.iceberg.exceptions.NoSuchTableException.class,
          () -> catalog.loadTable(tableId));
    } finally {
      try {
        nsCatalog.dropNamespace(ns);
      } catch (Exception e) {
        // Ignore cleanup errors
      }
      engine.close();
    }
  }

  // ========== Extended Schema Tests ==========

  /**
   * Tests that table schemas are correctly preserved and can be validated.
   *
   * <p>Verifies that:
   *
   * <ul>
   *   <li>The schema has the correct number of columns
   *   <li>Each field has the correct name, type, and required/optional status
   *   <li>Schema information is preserved through create/load cycles
   * </ul>
   *
   * <p>This is an extended test that validates schema integrity and field metadata.
   */
  @Test
  void testTableSchemaValidation() {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;

    Namespace ns = Namespace.of("test_ns_schema_" + uniqueSuffix());
    TableIdentifier tableId = TableIdentifier.of(ns, "test_table");

    try {
      nsCatalog.createNamespace(ns, new HashMap<>());
      catalog.createTable(tableId, createTestSchema());

      Table loadedTable = catalog.loadTable(tableId);
      Schema schema = loadedTable.schema();

      assertEquals(2, schema.columns().size());

      Types.NestedField idField = schema.findField("id");
      assertNotNull(idField, "Field 'id' should exist");
      assertEquals(Types.LongType.get(), idField.type());
      assertTrue(idField.isRequired(), "Field 'id' should be required");

      Types.NestedField nameField = schema.findField("name");
      assertNotNull(nameField, "Field 'name' should exist");
      assertEquals(Types.StringType.get(), nameField.type());
      assertTrue(nameField.isOptional(), "Field 'name' should be optional");
    } finally {
      try {
        catalog.dropTable(tableId, true);
        nsCatalog.dropNamespace(ns);
      } catch (Exception e) {
        // Ignore cleanup errors
      }
      engine.close();
    }
  }

  /**
   * Tests that dropping a namespace with tables throws NamespaceNotEmptyException.
   *
   * <p>Verifies that:
   *
   * <ul>
   *   <li>A namespace containing tables cannot be dropped
   *   <li>After dropping all tables, the namespace can be dropped successfully
   * </ul>
   *
   * <p>This is an extended test that validates namespace lifecycle constraints.
   */
  @Test
  void testDropNamespaceWithTables() {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;

    Namespace ns = Namespace.of("test_ns_drop_with_tables_" + uniqueSuffix());
    TableIdentifier tableId = TableIdentifier.of(ns, "test_table");

    try {
      nsCatalog.createNamespace(ns, new HashMap<>());
      catalog.createTable(tableId, createTestSchema());

      assertThrows(
          org.apache.iceberg.exceptions.NamespaceNotEmptyException.class,
          () -> nsCatalog.dropNamespace(ns));

      catalog.dropTable(tableId, true);
      assertTrue(nsCatalog.dropNamespace(ns));
    } finally {
      try {
        catalog.dropTable(tableId, true);
        nsCatalog.dropNamespace(ns);
      } catch (Exception e) {
        // Ignore cleanup errors
      }
      engine.close();
    }
  }
}
