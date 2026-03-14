package io.cryolite.sql.dml;

import io.cryolite.CryoliteEngine;
import io.cryolite.sql.SqlExecutionException;
import io.cryolite.sql.type.SqlLiteralConverter;
import io.cryolite.sql.util.SqlIdentifiers;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;

/**
 * Interprets and executes SQL DML statements against the Iceberg catalog.
 *
 * <p>Maps the Calcite {@code INSERT INTO ... VALUES (...)} AST to the CRYOLITE low-level write path
 * via {@link TableWriter}. Each {@code VALUES} row becomes one {@link GenericRecord}; all rows are
 * written and committed as a single atomic Iceberg snapshot.
 *
 * <p>Supported DML statements:
 *
 * <ul>
 *   <li>{@code INSERT INTO namespace.table [(col, ...)] VALUES (val, ...) [, (val, ...)]}
 * </ul>
 *
 * @since 0.1.0
 */
public class SqlDmlInterpreter {

  private final CryoliteEngine engine;

  /**
   * Creates a new SqlDmlInterpreter backed by the given engine.
   *
   * <p>The engine provides both the catalog (for schema lookups) and the append operation (for
   * writing records). This ensures the SQL layer stays within the engine's API boundary and does
   * not directly manage low-level I/O primitives like {@code TableWriter}.
   *
   * @param engine the CRYOLITE engine used for table lookups and data writes
   */
  public SqlDmlInterpreter(CryoliteEngine engine) {
    this.engine = engine;
  }

  /**
   * Executes an {@code INSERT INTO} statement.
   *
   * <p>Loads the target table, maps each VALUES row to a {@link GenericRecord} using the table
   * schema, writes all records, and commits them as a new snapshot.
   *
   * @param insert the parsed INSERT AST node from Calcite
   * @throws SqlExecutionException if the table cannot be found, the VALUES structure is unexpected,
   *     a literal cannot be converted, or the write/commit fails
   */
  public void execute(SqlInsert insert) {
    TableIdentifier tableId =
        SqlIdentifiers.resolveTableIdentifier((SqlIdentifier) insert.getTargetTable());
    Table table = loadTable(tableId);
    Schema schema = table.schema();

    // Determine the ordered list of target columns
    List<String> columnNames = resolveColumnNames(insert, schema);

    // Extract all rows from the VALUES clause
    List<List<SqlLiteral>> rows = extractValueRows(insert);

    // Build GenericRecord list to pass to the engine's append operation
    List<org.apache.iceberg.data.Record> records = new ArrayList<>(rows.size());
    for (List<SqlLiteral> row : rows) {
      if (row.size() != columnNames.size()) {
        throw new SqlExecutionException(
            "Column count mismatch: INSERT specifies "
                + columnNames.size()
                + " column(s), but row has "
                + row.size()
                + " value(s).");
      }
      GenericRecord record = GenericRecord.create(schema);
      for (int i = 0; i < columnNames.size(); i++) {
        String colName = columnNames.get(i);
        Types.NestedField field = schema.findField(colName);
        if (field == null) {
          throw new SqlExecutionException("Column not found in table schema: '" + colName + "'");
        }
        Object value = SqlLiteralConverter.toJavaValue(row.get(i), field.type());
        record.setField(colName, value);
      }
      records.add(record);
    }

    // Delegate the actual write to the engine – the SQL layer must not manage I/O directly.
    try {
      engine.append(tableId, records);
    } catch (IOException e) {
      throw new SqlExecutionException(
          "Failed to commit INSERT into '" + tableId + "': " + e.getMessage(), e);
    }
  }

  // --- private helpers ---

  private Table loadTable(TableIdentifier tableId) {
    Catalog catalog = engine.getCatalog();
    if (!catalog.tableExists(tableId)) {
      throw new SqlExecutionException("Table does not exist: '" + tableId + "'");
    }
    return catalog.loadTable(tableId);
  }

  private List<String> resolveColumnNames(SqlInsert insert, Schema schema) {
    SqlNodeList columnList = insert.getTargetColumnList();
    if (columnList == null || columnList.isEmpty()) {
      // No explicit column list → use all schema columns in definition order
      List<String> names = new ArrayList<>();
      for (Types.NestedField field : schema.columns()) {
        names.add(field.name());
      }
      return names;
    }
    List<String> names = new ArrayList<>(columnList.size());
    for (SqlNode col : columnList) {
      names.add(((SqlIdentifier) col).getSimple());
    }
    return names;
  }

  private List<List<SqlLiteral>> extractValueRows(SqlInsert insert) {
    SqlNode source = insert.getSource();
    if (!(source instanceof SqlCall valuesCall) || valuesCall.getKind() != SqlKind.VALUES) {
      throw new SqlExecutionException(
          "Only VALUES clauses are supported for INSERT in this version. "
              + "Got source type: "
              + source.getKind());
    }

    List<List<SqlLiteral>> rows = new ArrayList<>();
    for (SqlNode rowNode : valuesCall.getOperandList()) {
      rows.add(extractRowLiterals(rowNode));
    }
    return rows;
  }

  private List<SqlLiteral> extractRowLiterals(SqlNode rowNode) {
    // A single-value row may appear as a bare literal rather than a ROW call
    if (rowNode instanceof SqlLiteral literal) {
      return List.of(literal);
    }
    if (rowNode instanceof SqlCall rowCall) {
      List<SqlLiteral> literals = new ArrayList<>(rowCall.getOperandList().size());
      for (SqlNode operand : rowCall.getOperandList()) {
        if (!(operand instanceof SqlLiteral lit)) {
          throw new SqlExecutionException(
              "Only literal values (constants) are supported in INSERT VALUES. "
                  + "Got: '"
                  + operand
                  + "' (type: "
                  + operand.getClass().getSimpleName()
                  + ")");
        }
        literals.add(lit);
      }
      return literals;
    }
    throw new SqlExecutionException(
        "Unexpected node type in INSERT VALUES row: " + rowNode.getClass().getSimpleName());
  }
}
