package io.cryolite.sql.query;

import io.cryolite.CryoliteEngine;
import io.cryolite.filter.BatchPredicate;
import io.cryolite.sql.SqlExecutionException;
import io.cryolite.sql.filter.SqlWhereConverter;
import io.cryolite.sql.util.SqlIdentifiers;
import java.io.IOException;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;

/**
 * Interprets and executes SQL query statements against the Iceberg catalog.
 *
 * <p>Maps the Calcite {@code SELECT} AST to the CRYOLITE engine scan path. Supports optional WHERE
 * clauses with comparison operators ({@code =}, {@code <>}, {@code <}, {@code <=}, {@code >},
 * {@code >=}) and logical {@code AND}. Filtering is applied as residual evaluation on Arrow
 * batches.
 *
 * <p>Supported query statements:
 *
 * <ul>
 *   <li>{@code SELECT * FROM namespace.table}
 *   <li>{@code SELECT * FROM namespace.table WHERE column = value}
 *   <li>{@code SELECT * FROM namespace.table WHERE col1 > 10 AND col2 = 'x'}
 * </ul>
 *
 * @since 0.1.0
 */
public class SqlQueryInterpreter {

  private final CryoliteEngine engine;

  /**
   * Creates a new SqlQueryInterpreter backed by the given engine.
   *
   * <p>The engine provides the scan operation for reading table data and the catalog for schema
   * lookups. This ensures the SQL layer stays within the engine's API boundary.
   *
   * @param engine the CRYOLITE engine used for table scans
   */
  public SqlQueryInterpreter(CryoliteEngine engine) {
    this.engine = engine;
  }

  /**
   * Executes a {@code SELECT} query and returns the result as Arrow columnar batches.
   *
   * <p>If the query contains a WHERE clause, the filter is applied as residual evaluation on each
   * Arrow batch. Filtered batches are newly allocated and owned by the caller.
   *
   * <p>The caller is responsible for closing the returned iterable to release Arrow memory.
   *
   * @param select the parsed SELECT AST node from Calcite
   * @return a closeable iterable of Arrow batches
   * @throws SqlExecutionException if the table cannot be found or the scan fails
   */
  public CloseableIterable<VectorSchemaRoot> execute(SqlSelect select) {
    TableIdentifier tableId = resolveTableIdentifier(select);

    // If there is no WHERE clause, return unfiltered batches
    SqlNode where = select.getWhere();
    if (where == null) {
      try {
        return engine.scan(tableId);
      } catch (IOException e) {
        throw new SqlExecutionException(
            "Failed to scan table '" + tableId + "': " + e.getMessage(), e);
      }
    }

    // Load the table schema for type-directed literal conversion
    Table table = loadTable(tableId);
    Schema schema = table.schema();

    // Convert the WHERE clause to a batch predicate and delegate filtering to the engine
    BatchPredicate predicate = SqlWhereConverter.convert(where, schema);
    try {
      return engine.scan(tableId, predicate);
    } catch (IOException e) {
      throw new SqlExecutionException(
          "Failed to scan table '" + tableId + "': " + e.getMessage(), e);
    }
  }

  private Table loadTable(TableIdentifier tableId) {
    Catalog catalog = engine.getCatalog();
    if (!catalog.tableExists(tableId)) {
      throw new SqlExecutionException("Table does not exist: '" + tableId + "'");
    }
    return catalog.loadTable(tableId);
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
