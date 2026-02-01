package io.cryolite;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Tests for CryoliteEngine lifecycle and core functionality. */
class CryoliteEngineTest extends AbstractIntegrationTest {

  /**
   * Tests that a CryoliteEngine can be successfully created with a valid configuration.
   *
   * <p>Verifies that:
   *
   * <ul>
   *   <li>The engine instance is not null
   *   <li>The engine is not closed after creation
   *   <li>The engine returns the same config instance that was passed to the constructor
   * </ul>
   */
  @Test
  void testEngineCreation() {
    CryoliteConfig config = createTestConfig();
    CryoliteEngine engine = new CryoliteEngine(config);

    assertNotNull(engine);
    assertFalse(engine.isClosed());
    assertEquals(config, engine.getConfig());
    engine.close();
  }

  /**
   * Tests that closing the engine properly updates its state.
   *
   * <p>Verifies that:
   *
   * <ul>
   *   <li>The engine is not closed initially
   *   <li>After calling close(), isClosed() returns true
   * </ul>
   */
  @Test
  void testEngineClose() {
    CryoliteConfig config = createTestConfig();
    CryoliteEngine engine = new CryoliteEngine(config);

    assertFalse(engine.isClosed());
    engine.close();
    assertTrue(engine.isClosed());
  }

  /**
   * Tests that calling close() multiple times is safe (idempotent operation).
   *
   * <p>Verifies that:
   *
   * <ul>
   *   <li>The first close() call marks the engine as closed
   *   <li>Subsequent close() calls do not throw exceptions
   *   <li>The engine remains closed after multiple close() calls
   * </ul>
   */
  @Test
  void testEngineCloseIdempotent() {
    CryoliteConfig config = createTestConfig();
    CryoliteEngine engine = new CryoliteEngine(config);

    engine.close();
    assertTrue(engine.isClosed());
    // Calling close again should be safe (idempotent)
    engine.close();
    assertTrue(engine.isClosed());
  }

  /**
   * Tests that creating an engine with a null configuration throws IllegalArgumentException.
   *
   * <p>This is a defensive programming test to ensure the engine fails fast with a clear error
   * message when given invalid input.
   */
  @Test
  void testEngineNullConfig() {
    assertThrows(IllegalArgumentException.class, () -> new CryoliteEngine(null));
  }

  /**
   * Tests that the engine's toString() method returns a meaningful string representation.
   *
   * <p>Verifies that:
   *
   * <ul>
   *   <li>toString() returns a non-null string
   *   <li>The string contains "CryoliteEngine" for easy identification
   * </ul>
   */
  @Test
  void testEngineToString() {
    CryoliteConfig config = createTestConfig();
    CryoliteEngine engine = new CryoliteEngine(config);

    String str = engine.toString();
    assertNotNull(str);
    assertTrue(str.contains("CryoliteEngine"));
    engine.close();
  }

  /**
   * Tests that getCatalog() returns a valid Iceberg Catalog instance.
   *
   * <p>This is the primary API for interacting with the Iceberg catalog. The catalog provides
   * access to namespace and table operations.
   */
  @Test
  void testGetCatalog() {
    CryoliteConfig config = createTestConfig();
    CryoliteEngine engine = new CryoliteEngine(config);

    assertNotNull(engine.getCatalog());
    engine.close();
  }

  /**
   * Tests that calling getCatalog() on a closed engine throws IllegalStateException.
   *
   * <p>This ensures that the engine fails fast when used incorrectly, preventing potential resource
   * leaks or undefined behavior.
   */
  @Test
  void testGetCatalogWhenClosed() {
    CryoliteConfig config = createTestConfig();
    CryoliteEngine engine = new CryoliteEngine(config);
    engine.close();

    assertThrows(IllegalStateException.class, engine::getCatalog);
  }

  /**
   * Tests that the engine reports healthy status when services are running.
   *
   * <p>This is an integration test that verifies the health check logic against real Polaris and
   * MinIO services running in Docker Compose.
   *
   * <p>The health check validates connectivity to both the catalog and storage services.
   */
  @Test
  void testIsHealthy() {
    CryoliteConfig config = createTestConfig();
    CryoliteEngine engine = new CryoliteEngine(config);

    // With Docker Compose running, the engine should be healthy
    // This tests the actual health check logic against real services
    assertTrue(engine.isHealthy(), "Engine should be healthy when services are running");
    engine.close();
  }

  /**
   * Tests that a closed engine always reports unhealthy status.
   *
   * <p>Verifies that the health check correctly identifies a closed engine as unhealthy, regardless
   * of the state of external services.
   */
  @Test
  void testIsHealthyWhenClosed() {
    CryoliteConfig config = createTestConfig();
    CryoliteEngine engine = new CryoliteEngine(config);
    engine.close();

    assertFalse(engine.isHealthy());
  }
}
