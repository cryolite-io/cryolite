package io.cryolite;

import io.cryolite.catalog.CatalogManager;
import io.cryolite.data.TableReader;
import io.cryolite.data.TableWriter;
import io.cryolite.sql.SqlSession;
import java.io.IOException;
import java.util.List;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;

/**
 * CRYOLITE Runs Your Open Lightweight Iceberg Table Engine.
 *
 * <p>CryoliteEngine is the main entry point for the embedded Iceberg table engine. It provides
 * access to the Iceberg Catalog for all table and namespace operations.
 *
 * <p>This is an embedded library - no CLI, no server, no REST service. It is designed to be used
 * directly from Java applications.
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * CryoliteEngine engine = new CryoliteEngine(config);
 * Catalog catalog = engine.getCatalog();
 *
 * // Namespace operations (cast to SupportsNamespaces)
 * SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;
 * nsCatalog.createNamespace(Namespace.of("my_namespace"), Map.of());
 *
 * // Table operations
 * catalog.createTable(TableIdentifier.of("my_namespace", "my_table"), schema);
 * }</pre>
 *
 * @since 0.1.0
 */
public class CryoliteEngine {

  private final CryoliteConfig config;
  private final CatalogManager catalogManager;
  private volatile boolean closed = false;

  /**
   * Creates a new CryoliteEngine with the given configuration.
   *
   * @param config the engine configuration
   * @throws IllegalArgumentException if config is null
   */
  public CryoliteEngine(CryoliteConfig config) {
    if (config == null) {
      throw new IllegalArgumentException("Configuration cannot be null");
    }
    this.config = config;
    this.catalogManager =
        new CatalogManager(config.getCatalogOptions(), config.getStorageOptions());
  }

  /**
   * Returns the engine configuration.
   *
   * @return the configuration
   */
  public CryoliteConfig getConfig() {
    return config;
  }

  /**
   * Returns the Iceberg Catalog for table and namespace operations.
   *
   * <p>The returned catalog can be cast to {@code SupportsNamespaces} for namespace operations.
   *
   * @return the Iceberg Catalog
   * @throws IllegalStateException if engine is closed
   */
  public org.apache.iceberg.catalog.Catalog getCatalog() {
    if (closed) {
      throw new IllegalStateException("Engine is closed");
    }
    return catalogManager.getCatalog();
  }

  /**
   * Checks if the engine is healthy (catalog is accessible).
   *
   * @return true if catalog is healthy, false otherwise
   */
  public boolean isHealthy() {
    if (closed) {
      return false;
    }
    return catalogManager.isHealthy();
  }

  /**
   * Checks if the engine is closed.
   *
   * @return true if closed, false otherwise
   */
  public boolean isClosed() {
    return closed;
  }

  /**
   * Appends a list of records to the specified table as a single atomic Iceberg snapshot.
   *
   * <p>This is the engine-level write operation used by the SQL layer (DML) and available directly
   * via the low-level API. Internally it delegates to {@link TableWriter}.
   *
   * @param tableId the target table
   * @param records the records to append; must conform to the table's schema
   * @throws IllegalStateException if the engine is closed
   * @throws IOException if writing or committing the snapshot fails
   */
  public void append(TableIdentifier tableId, List<Record> records) throws IOException {
    if (closed) {
      throw new IllegalStateException("Engine is closed");
    }
    Table table = catalogManager.getCatalog().loadTable(tableId);
    try (TableWriter writer = new TableWriter(table)) {
      for (Record record : records) {
        writer.write(record);
      }
      writer.commit();
    }
  }

  /**
   * Scans the specified table and returns its data as Arrow columnar batches.
   *
   * <p>This is the engine-level read operation used by the SQL layer (query) and available directly
   * via the low-level API. Internally it delegates to {@link TableReader}.
   *
   * <p><b>Memory Lifecycle:</b> Each {@link VectorSchemaRoot} batch is valid only during iteration.
   * Callers must process each batch within the iteration loop and close the returned iterable when
   * done.
   *
   * @param tableId the table to scan
   * @return a closeable iterable of Arrow batches; empty if table has no snapshots
   * @throws IllegalStateException if the engine is closed
   * @throws IOException if reading fails
   */
  public CloseableIterable<VectorSchemaRoot> scan(TableIdentifier tableId) throws IOException {
    if (closed) {
      throw new IllegalStateException("Engine is closed");
    }
    Table table = catalogManager.getCatalog().loadTable(tableId);
    try (TableReader reader = new TableReader(table)) {
      return reader.readBatches();
    }
  }

  /**
   * Creates a new {@link SqlSession} for executing SQL statements.
   *
   * <p>The session uses this engine for all DDL and DML operations. Use a try-with-resources block
   * to ensure the session is properly closed:
   *
   * <pre>{@code
   * try (SqlSession session = engine.createSqlSession()) {
   *   session.execute("CREATE TABLE my_ns.my_table (id BIGINT NOT NULL, name VARCHAR)");
   *   session.execute("INSERT INTO my_ns.my_table VALUES (1, 'Alice')");
   * }
   * }</pre>
   *
   * @return a new SqlSession
   * @throws IllegalStateException if the engine is closed
   */
  public SqlSession createSqlSession() {
    if (closed) {
      throw new IllegalStateException("Engine is closed");
    }
    return new SqlSession(this);
  }

  /**
   * Closes the engine and releases all resources.
   *
   * <p>After calling this method, the engine cannot be used anymore. This method is idempotent -
   * calling it multiple times is safe.
   */
  public void close() {
    if (!closed) {
      closed = true;
      catalogManager.close();
    }
  }

  @Override
  public String toString() {
    return "CryoliteEngine{" + "config=" + config + ", closed=" + closed + '}';
  }
}
