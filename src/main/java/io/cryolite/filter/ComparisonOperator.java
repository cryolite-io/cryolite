package io.cryolite.filter;

import java.util.function.DoublePredicate;
import java.util.function.LongPredicate;
import java.util.function.Predicate;

/**
 * Standard SQL comparison operators for use in {@link ComparisonPredicate}.
 *
 * <p>Each operator implements the comparison using {@link Comparable} semantics for generic types,
 * and provides boxing-free primitive variants via {@link #asLongPredicate}, {@link
 * #asDoublePredicate}, and {@link #asStringPredicate}. These primitive variants allow tight loops
 * in {@link ComparisonPredicate} without allocating {@code Long} or {@code Double} wrappers on
 * every row.
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
   * Returns a boxing-free {@link LongPredicate} that applies this operator against {@code literal}.
   *
   * <p>The predicate is built once per column evaluation and reused for every row, so the operator
   * switch is paid only once rather than on each iteration.
   *
   * @param literal the long literal to compare against
   * @return a primitive predicate for use in tight loops over {@code BigIntVector} / {@code
   *     IntVector}
   */
  public LongPredicate asLongPredicate(long literal) {
    return switch (this) {
      case EQUALS -> v -> v == literal;
      case NOT_EQUALS -> v -> v != literal;
      case LESS_THAN -> v -> v < literal;
      case LESS_THAN_OR_EQUAL -> v -> v <= literal;
      case GREATER_THAN -> v -> v > literal;
      case GREATER_THAN_OR_EQUAL -> v -> v >= literal;
    };
  }

  /**
   * Returns a boxing-free {@link DoublePredicate} that applies this operator against {@code
   * literal}.
   *
   * <p>Used for both {@code Float8Vector} (double) and {@code Float4Vector} (float promoted to
   * double) columns.
   *
   * @param literal the double literal to compare against
   * @return a primitive predicate for use in tight loops over floating-point vectors
   */
  public DoublePredicate asDoublePredicate(double literal) {
    return switch (this) {
      case EQUALS -> v -> v == literal;
      case NOT_EQUALS -> v -> v != literal;
      case LESS_THAN -> v -> v < literal;
      case LESS_THAN_OR_EQUAL -> v -> v <= literal;
      case GREATER_THAN -> v -> v > literal;
      case GREATER_THAN_OR_EQUAL -> v -> v >= literal;
    };
  }

  /**
   * Returns a {@link Predicate Predicate&lt;String&gt;} that applies this operator against {@code
   * literal}.
   *
   * <p>Uses {@link String#equals} for equality checks and {@link String#compareTo} for ordering,
   * matching standard SQL string comparison semantics.
   *
   * @param literal the string literal to compare against
   * @return a string predicate for use in tight loops over {@code VarCharVector} columns
   */
  public Predicate<String> asStringPredicate(String literal) {
    return switch (this) {
      case EQUALS -> v -> v.equals(literal);
      case NOT_EQUALS -> v -> !v.equals(literal);
      case LESS_THAN -> v -> v.compareTo(literal) < 0;
      case LESS_THAN_OR_EQUAL -> v -> v.compareTo(literal) <= 0;
      case GREATER_THAN -> v -> v.compareTo(literal) > 0;
      case GREATER_THAN_OR_EQUAL -> v -> v.compareTo(literal) >= 0;
    };
  }

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
