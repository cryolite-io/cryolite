package io.cryolite.filter;

/**
 * Standard SQL comparison operators for use in {@link ComparisonPredicate}.
 *
 * <p>Each operator implements the comparison using {@link Comparable} semantics. Both operands are
 * cast to {@code Comparable} and compared via {@link Comparable#compareTo}. The operators handle
 * type coercion for common numeric promotions (e.g., Integer vs Long).
 *
 * @since 0.1.0
 */
public enum ComparisonOperator {
  /** Equals ({@code =}). */
  EQUALS {
    @Override
    public boolean apply(Object columnValue, Comparable<?> literal) {
      return compare(columnValue, literal) == 0;
    }
  },

  /** Not equals ({@code <>}). */
  NOT_EQUALS {
    @Override
    public boolean apply(Object columnValue, Comparable<?> literal) {
      return compare(columnValue, literal) != 0;
    }
  },

  /** Less than ({@code <}). */
  LESS_THAN {
    @Override
    public boolean apply(Object columnValue, Comparable<?> literal) {
      return compare(columnValue, literal) < 0;
    }
  },

  /** Less than or equal ({@code <=}). */
  LESS_THAN_OR_EQUAL {
    @Override
    public boolean apply(Object columnValue, Comparable<?> literal) {
      return compare(columnValue, literal) <= 0;
    }
  },

  /** Greater than ({@code >}). */
  GREATER_THAN {
    @Override
    public boolean apply(Object columnValue, Comparable<?> literal) {
      return compare(columnValue, literal) > 0;
    }
  },

  /** Greater than or equal ({@code >=}). */
  GREATER_THAN_OR_EQUAL {
    @Override
    public boolean apply(Object columnValue, Comparable<?> literal) {
      return compare(columnValue, literal) >= 0;
    }
  };

  /**
   * Applies this comparison operator to the given column value and literal.
   *
   * @param columnValue the value extracted from the Arrow vector
   * @param literal the literal value to compare against
   * @return {@code true} if the comparison holds
   */
  public abstract boolean apply(Object columnValue, Comparable<?> literal);

  /**
   * Compares two values with numeric type promotion.
   *
   * <p>When both values are {@link Number} instances but of different types (e.g., Integer vs
   * Long), both are promoted to {@code double} for comparison. Otherwise, standard {@link
   * Comparable#compareTo} is used.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  protected static int compare(Object columnValue, Comparable<?> literal) {
    if (columnValue instanceof Number colNum && literal instanceof Number litNum) {
      return Double.compare(colNum.doubleValue(), litNum.doubleValue());
    }
    // Arrow VarCharVector.getObject() returns org.apache.arrow.vector.util.Text
    if (columnValue instanceof org.apache.arrow.vector.util.Text text) {
      return text.toString().compareTo(literal.toString());
    }
    return ((Comparable) columnValue).compareTo(literal);
  }
}
