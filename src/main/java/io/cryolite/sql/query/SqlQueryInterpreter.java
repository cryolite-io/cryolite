package io.cryolite.sql.query;

import io.cryolite.CryoliteEngine;
import io.cryolite.sql.SqlExecutionException;
import io.cryolite.sql.util.SqlIdentifiers;
import java.io.IOException;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlSelect;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;

/**
 * Interprets and executes SQL query statements against the Iceberg catalog.
 *
 * <p>Maps the Calcite {@code SELECT * FROM namespace.table} AST to the CRYOLITE engine scan path.
 * Returns Arrow columnar batches for vectorized processing.
 *
 * <p>Supported query statements:
 *
 * <ul>
 *   <li>{@code SELECT * FROM namespace.table}
 * </ul>
 *
 * @since 0.1.0
 */
public class SqlQueryInterpreter {

  private final CryoliteEngine engine;

  /**
   * Creates a new SqlQueryInterpreter backed by the given engine.
   *
   * <p>The engine provides the scan operation for reading table data. This ensures the SQL layer
   * stays within the engine's API boundary and does not directly manage low-level I/O primitives.
   *
   * @param engine the CRYOLITE engine used for table scans
   */
  public SqlQueryInterpreter(CryoliteEngine engine) {
    this.engine = engine;
  }

  /**
   * Executes a {@code SELECT} query and returns the result as Arrow columnar batches.
   *
   * <p>Currently supports {@code SELECT *} only. The caller is responsible for closing the returned
   * iterable to release Arrow memory.
   *
   * @param select the parsed SELECT AST node from Calcite
   * @return a closeable iterable of Arrow batches
   * @throws SqlExecutionException if the table cannot be found or the scan fails
   */
  public CloseableIterable<VectorSchemaRoot> execute(SqlSelect select) {
    TableIdentifier tableId = resolveTableIdentifier(select);

    try {
      return engine.scan(tableId);
    } catch (IOException e) {
      throw new SqlExecutionException(
          "Failed to scan table '" + tableId + "': " + e.getMessage(), e);
    }
  }

  private TableIdentifier resolveTableIdentifier(SqlSelect select) {
    if (select.getFrom() == null) {
      throw new SqlExecutionException(
          "SELECT without FROM clause is not supported. Use: SELECT * FROM namespace.table");
    }
    if (!(select.getFrom() instanceof SqlIdentifier fromId)) {
      throw new SqlExecutionException(
          "Unsupported FROM clause: '"
              + select.getFrom().getKind()
              + "'. Only simple table references are supported (e.g., namespace.table).");
    }
    return SqlIdentifiers.resolveTableIdentifier(fromId);
  }
}
