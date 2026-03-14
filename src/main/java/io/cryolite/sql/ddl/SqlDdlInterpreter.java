package io.cryolite.sql.ddl;

import io.cryolite.sql.SqlExecutionException;
import io.cryolite.sql.type.CalciteTypeMapper;
import io.cryolite.sql.util.SqlIdentifiers;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Interprets and executes SQL DDL statements against the Iceberg catalog.
 *
 * <p>This class maps the parsed Calcite DDL AST nodes to concrete Iceberg catalog operations. It is
 * the bridge between the SQL layer and the low-level Iceberg API.
 *
 * <p>Supported DDL statements:
 *
 * <ul>
 *   <li>{@code CREATE TABLE [IF NOT EXISTS] namespace.table (columns...)}
 * </ul>
 *
 * <p>Table names must be fully qualified with a namespace (e.g., {@code my_ns.my_table}).
 * Single-part names are not allowed because Iceberg requires namespace isolation.
 *
 * @since 0.1.0
 */
public class SqlDdlInterpreter {

  private final Catalog catalog;

  /**
   * Creates a new SqlDdlInterpreter backed by the given catalog.
   *
   * @param catalog the Iceberg catalog to use for DDL execution
   */
  public SqlDdlInterpreter(Catalog catalog) {
    this.catalog = catalog;
  }

  /**
   * Executes a {@code CREATE TABLE} DDL statement.
   *
   * <p>If {@code IF NOT EXISTS} is specified and the table already exists, the statement is
   * silently ignored. Without {@code IF NOT EXISTS}, an {@link AlreadyExistsException} is
   * propagated.
   *
   * @param createTable the parsed CREATE TABLE AST node from Calcite
   * @throws SqlExecutionException if the table name is not fully qualified, the column list is
   *     empty or contains unsupported constructs, or the catalog operation fails
   */
  public void execute(SqlCreateTable createTable) {
    TableIdentifier tableId = SqlIdentifiers.resolveTableIdentifier(createTable.name);
    Schema schema = buildSchema(createTable);

    ensureNamespaceExists(tableId.namespace());

    try {
      catalog.createTable(tableId, schema);
    } catch (AlreadyExistsException e) {
      if (!createTable.ifNotExists) {
        throw e;
      }
      // IF NOT EXISTS: silently ignore duplicate
    }
  }

  private Schema buildSchema(SqlCreateTable createTable) {
    if (createTable.columnList == null || createTable.columnList.size() == 0) {
      throw new SqlExecutionException(
          "CREATE TABLE must define at least one column. "
              + "CREATE TABLE AS SELECT is not yet supported.");
    }

    List<Types.NestedField> fields = new ArrayList<>();
    int fieldId = 1;

    for (SqlNode node : createTable.columnList) {
      if (!(node instanceof SqlColumnDeclaration colDecl)) {
        // Skip non-column declarations (e.g. table constraints) with a clear error for now
        throw new SqlExecutionException(
            "Unsupported column definition type: '"
                + node.getClass().getSimpleName()
                + "'. Only plain column declarations are supported in this version.");
      }

      String columnName = colDecl.name.getSimple();
      Type icebergType = CalciteTypeMapper.toIcebergType(colDecl.dataType);
      boolean required = (colDecl.strategy == ColumnStrategy.NOT_NULLABLE);

      Types.NestedField field =
          required
              ? Types.NestedField.required(fieldId, columnName, icebergType)
              : Types.NestedField.optional(fieldId, columnName, icebergType);

      fields.add(field);
      fieldId++;
    }

    return new Schema(fields);
  }

  private void ensureNamespaceExists(Namespace namespace) {
    if (!(catalog instanceof SupportsNamespaces nsCatalog)) {
      return; // Catalog does not support namespace management; proceed as-is
    }
    if (!nsCatalog.namespaceExists(namespace)) {
      nsCatalog.createNamespace(namespace, new HashMap<>());
    }
  }
}
