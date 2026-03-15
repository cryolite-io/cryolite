package io.cryolite.sql.filter;

import io.cryolite.filter.AndPredicate;
import io.cryolite.filter.BatchPredicate;
import io.cryolite.filter.ComparisonOperator;
import io.cryolite.filter.ComparisonPredicate;
import io.cryolite.sql.SqlExecutionException;
import io.cryolite.sql.type.SqlLiteralConverter;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

/**
 * Converts a Calcite WHERE clause {@link SqlNode} tree into a {@link BatchPredicate} tree.
 *
 * <p>This converter handles the SQL comparison operators ({@code =}, {@code <>}, {@code <}, {@code
 * <=}, {@code >}, {@code >=}) and the logical {@code AND} operator. Each comparison is translated
 * into a {@link ComparisonPredicate} that evaluates against Arrow batch rows at query time.
 *
 * <p>Literal values are converted using the target column's Iceberg type via {@link
 * SqlLiteralConverter}, ensuring type-correct comparisons (e.g., a numeric literal becomes a {@code
 * Long} when compared against a {@code BIGINT} column).
 *
 * <p>This is the residual filter path (M8). Pushdown to Iceberg scan expressions will be added in
 * M9.
 *
 * @since 0.1.0
 */
public final class SqlWhereConverter {

  private SqlWhereConverter() {
    // utility class
  }

  /**
   * Converts a Calcite WHERE clause into a batch predicate.
   *
   * @param whereNode the WHERE clause AST node from {@code SqlSelect.getWhere()}
   * @param schema the Iceberg schema of the target table (for type-directed literal conversion)
   * @return a batch predicate that evaluates the WHERE condition column-wise against Arrow batches
   * @throws SqlExecutionException if the WHERE clause contains unsupported constructs
   */
  public static BatchPredicate convert(SqlNode whereNode, Schema schema) {
    if (whereNode instanceof SqlBasicCall call) {
      return convertCall(call, schema);
    }
    throw new SqlExecutionException(
        "Unsupported WHERE clause node type: '"
            + whereNode.getClass().getSimpleName()
            + "'. Only comparisons (=, <>, <, <=, >, >=) and AND are supported.");
  }

  private static BatchPredicate convertCall(SqlBasicCall call, Schema schema) {
    SqlKind kind = call.getKind();

    if (kind == SqlKind.AND) {
      return convertAnd(call, schema);
    }

    if (SqlKind.COMPARISON.contains(kind)) {
      return convertComparison(call, schema);
    }

    throw new SqlExecutionException(
        "Unsupported WHERE operator: '"
            + kind
            + "'. Only comparisons (=, <>, <, <=, >, >=) and AND are supported in this version.");
  }

  private static BatchPredicate convertAnd(SqlBasicCall call, Schema schema) {
    List<SqlNode> operands = call.getOperandList();
    List<BatchPredicate> predicates = new ArrayList<>(operands.size());
    for (SqlNode operand : operands) {
      predicates.add(convert(operand, schema));
    }
    return new AndPredicate(predicates);
  }

  private static BatchPredicate convertComparison(SqlBasicCall call, Schema schema) {
    List<SqlNode> operands = call.getOperandList();
    if (operands.size() != 2) {
      throw new SqlExecutionException(
          "Comparison operator requires exactly 2 operands, got " + operands.size());
    }

    SqlNode left = operands.get(0);
    SqlNode right = operands.get(1);

    // Support both "column op literal" and "literal op column" (reversed)
    if (left instanceof SqlIdentifier id && right instanceof SqlLiteral lit) {
      return buildComparison(id, call.getKind(), lit, schema);
    }
    if (left instanceof SqlLiteral lit && right instanceof SqlIdentifier id) {
      // Reverse the operator: "5 > age" becomes "age < 5"
      return buildComparison(id, call.getKind().reverse(), lit, schema);
    }

    throw new SqlExecutionException(
        "Unsupported comparison operands: expected 'column op literal' or 'literal op column'. "
            + "Got: '"
            + left.getClass().getSimpleName()
            + " "
            + call.getKind()
            + " "
            + right.getClass().getSimpleName()
            + "'");
  }

  private static ComparisonPredicate buildComparison(
      SqlIdentifier column, SqlKind kind, SqlLiteral literal, Schema schema) {
    String columnName = column.getSimple();
    ComparisonOperator operator = mapOperator(kind);

    // Look up the column type in the Iceberg schema for type-directed conversion
    Types.NestedField field = schema.findField(columnName);
    if (field == null) {
      throw new SqlExecutionException(
          "Column '"
              + columnName
              + "' not found in table schema. "
              + "Available columns: "
              + schema.columns().stream().map(Types.NestedField::name).toList());
    }

    Comparable<?> value = (Comparable<?>) SqlLiteralConverter.toJavaValue(literal, field.type());
    return new ComparisonPredicate(columnName, operator, value);
  }

  private static ComparisonOperator mapOperator(SqlKind kind) {
    return switch (kind) {
      case EQUALS -> ComparisonOperator.EQUALS;
      case NOT_EQUALS -> ComparisonOperator.NOT_EQUALS;
      case LESS_THAN -> ComparisonOperator.LESS_THAN;
      case LESS_THAN_OR_EQUAL -> ComparisonOperator.LESS_THAN_OR_EQUAL;
      case GREATER_THAN -> ComparisonOperator.GREATER_THAN;
      case GREATER_THAN_OR_EQUAL -> ComparisonOperator.GREATER_THAN_OR_EQUAL;
      default -> throw new SqlExecutionException("Unsupported comparison operator: '" + kind + "'");
    };
  }
}
