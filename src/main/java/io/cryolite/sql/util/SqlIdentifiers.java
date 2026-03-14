package io.cryolite.sql.util;

import io.cryolite.sql.SqlExecutionException;
import java.util.List;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * Utility methods for resolving Calcite {@link SqlIdentifier} nodes to Iceberg identifiers.
 *
 * <p>This class centralises the mapping from SQL-level identifiers (which may be one-part,
 * two-part, or multi-part) to Iceberg's {@link TableIdentifier} and {@link Namespace}. Both the DDL
 * and DML interpreters delegate to this class so that the resolution logic is defined exactly once.
 *
 * <p>Current constraint: table names must be fully qualified with at least a namespace, e.g. {@code
 * my_ns.my_table}. Single-part names are rejected because Iceberg requires namespace isolation.
 *
 * @since 0.1.0
 */
public final class SqlIdentifiers {

  private SqlIdentifiers() {
    // utility class
  }

  /**
   * Resolves a Calcite {@link SqlIdentifier} to an Iceberg {@link TableIdentifier}.
   *
   * <p>The identifier must contain at least two parts ({@code namespace.table}). The last part is
   * treated as the table name, the second-to-last as the namespace.
   *
   * @param identifier the Calcite SQL identifier (e.g. from {@code CREATE TABLE} or {@code INSERT
   *     INTO})
   * @return the corresponding Iceberg TableIdentifier
   * @throws SqlExecutionException if the identifier has fewer than two parts
   */
  public static TableIdentifier resolveTableIdentifier(SqlIdentifier identifier) {
    List<String> names = identifier.names;
    if (names.size() < 2) {
      throw new SqlExecutionException(
          "Table name must be fully qualified with a namespace, e.g. 'my_namespace.my_table'. "
              + "Got: '"
              + identifier
              + "'");
    }
    String namespaceName = names.get(names.size() - 2);
    String tableName = names.get(names.size() - 1);
    return TableIdentifier.of(Namespace.of(namespaceName), tableName);
  }
}
