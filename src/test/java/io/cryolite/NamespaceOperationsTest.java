package io.cryolite;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.junit.jupiter.api.Test;

/** Tests for namespace operations via the Iceberg Catalog API. */
class NamespaceOperationsTest extends AbstractIntegrationTest {

  /**
   * Tests that a namespace can be successfully created and verified to exist.
   *
   * <p>Verifies that:
   *
   * <ul>
   *   <li>A namespace can be created via the Iceberg Catalog API
   *   <li>The created namespace is immediately visible via namespaceExists()
   * </ul>
   *
   * <p>This is a positive test for the basic namespace creation workflow.
   */
  @Test
  void testCreateNamespace() {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;

    Namespace ns = Namespace.of("test_ns_create_" + uniqueSuffix());

    try {
      nsCatalog.createNamespace(ns, new HashMap<>());
      assertTrue(nsCatalog.namespaceExists(ns));
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
   * Tests that multiple namespaces can be created and listed.
   *
   * <p>Verifies that:
   *
   * <ul>
   *   <li>Multiple namespaces can be created independently
   *   <li>listNamespaces() returns all created namespaces
   * </ul>
   *
   * <p>This tests the namespace listing functionality and ensures that the catalog correctly tracks
   * multiple namespaces.
   */
  @Test
  void testListNamespaces() {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;

    Namespace ns1 = Namespace.of("test_ns_list_1_" + uniqueSuffix());
    Namespace ns2 = Namespace.of("test_ns_list_2_" + uniqueSuffix());

    try {
      nsCatalog.createNamespace(ns1, new HashMap<>());
      nsCatalog.createNamespace(ns2, new HashMap<>());

      List<Namespace> namespaces = nsCatalog.listNamespaces();
      assertTrue(namespaces.contains(ns1));
      assertTrue(namespaces.contains(ns2));
    } finally {
      try {
        nsCatalog.dropNamespace(ns1);
        nsCatalog.dropNamespace(ns2);
      } catch (Exception e) {
        // Ignore cleanup errors
      }
      engine.close();
    }
  }

  /**
   * Tests the complete lifecycle of a namespace: create, verify existence, and drop.
   *
   * <p>Verifies that:
   *
   * <ul>
   *   <li>A non-existent namespace returns false from namespaceExists()
   *   <li>After creation, namespaceExists() returns true
   *   <li>After dropping, namespaceExists() returns false again
   * </ul>
   *
   * <p>This tests the full CRUD lifecycle for namespaces.
   */
  @Test
  void testNamespaceExistsAndDrop() {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;

    Namespace ns = Namespace.of("test_ns_exists_" + uniqueSuffix());

    try {
      assertFalse(nsCatalog.namespaceExists(ns));

      nsCatalog.createNamespace(ns, new HashMap<>());
      assertTrue(nsCatalog.namespaceExists(ns));

      nsCatalog.dropNamespace(ns);
      assertFalse(nsCatalog.namespaceExists(ns));
    } finally {
      engine.close();
    }
  }

  /**
   * Tests that namespace metadata can be created and retrieved.
   *
   * <p>Verifies that:
   *
   * <ul>
   *   <li>A namespace can be created with custom properties/metadata
   *   <li>loadNamespaceMetadata() returns the metadata for the namespace
   * </ul>
   *
   * <p>This tests the metadata management functionality for namespaces.
   */
  @Test
  void testLoadNamespaceMetadata() {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;

    Namespace ns = Namespace.of("test_ns_metadata_" + uniqueSuffix());
    Map<String, String> properties = new HashMap<>();
    properties.put("description", "Test namespace for metadata");

    try {
      nsCatalog.createNamespace(ns, properties);

      Map<String, String> metadata = nsCatalog.loadNamespaceMetadata(ns);
      assertNotNull(metadata);
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
   * Tests that creating a namespace with a name that already exists throws AlreadyExistsException.
   *
   * <p>This is a negative test that verifies the catalog correctly rejects duplicate namespace
   * creation attempts, preventing accidental overwrites.
   */
  @Test
  void testCreateDuplicateNamespace() {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;

    Namespace ns = Namespace.of("test_ns_duplicate_" + uniqueSuffix());

    try {
      nsCatalog.createNamespace(ns, new HashMap<>());

      assertThrows(
          org.apache.iceberg.exceptions.AlreadyExistsException.class,
          () -> nsCatalog.createNamespace(ns, new HashMap<>()));
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
   * Tests that dropping a non-existent namespace returns false (no exception).
   *
   * <p>This verifies that the catalog gracefully handles attempts to drop namespaces that don't
   * exist, returning false instead of throwing an exception. This is the expected behavior in
   * Polaris.
   */
  @Test
  void testDropNonExistentNamespace() {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;

    Namespace ns = Namespace.of("test_ns_nonexistent_" + uniqueSuffix());

    try {
      assertFalse(nsCatalog.namespaceExists(ns));
      // Dropping a non-existent namespace returns false (no exception in Polaris)
      assertFalse(nsCatalog.dropNamespace(ns));
    } finally {
      engine.close();
    }
  }
}
