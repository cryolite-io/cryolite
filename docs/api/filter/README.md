# filter package

**Package**: `io.cryolite.filter`
**Since**: 0.1.0 (M8 – SQL WHERE)

The filter package defines the predicate model used by [`TableScanner`](../data/TableScanner.md)
and the SQL [`SqlWhereConverter`](../sql/README.md#sqlwhereconverter). Predicates are
**columnar**: each one evaluates an entire Arrow batch in a single pass and returns a
`BitSet` selection vector. Composition uses bulk bitwise operations rather than per-row
short-circuit evaluation, keeping hot loops cache-friendly.

## Components

| Class | Role |
|---|---|
| `BatchPredicate` | Core interface – `evaluate(VectorSchemaRoot) → BitSet` plus optional `toIcebergExpression()` for pushdown |
| `ComparisonPredicate` | Column-vs-literal comparison (`=`, `<>`, `<`, `<=`, `>`, `>=`) |
| `ComparisonOperator` | Enum of comparison operators; provides boxing-free primitive predicates and Iceberg expression factories |
| `AndPredicate` | Composite predicate combining children via `BitSet.and` |
| `ArrowBatchFilter` | Applies a `BatchPredicate` and copies matching rows into a new `VectorSchemaRoot` |

## BatchPredicate

```java
@FunctionalInterface
public interface BatchPredicate {
    BitSet evaluate(VectorSchemaRoot batch);

    default Optional<Expression> toIcebergExpression() {
        return Optional.empty();
    }
}
```

`evaluate` returns a `BitSet` where bit `i` is set iff row `i` matches.

`toIcebergExpression` enables predicate pushdown to the Iceberg scan layer. The contract:

- If `Optional.empty()` → not pushable, only the residual Arrow-level filter runs.
- If a non-empty `Expression` is returned → it is semantically equivalent to the
  predicate, and the Iceberg scan can use it for partition / manifest / file pruning.
- The Arrow residual filter still runs unconditionally to guarantee row-level correctness
  (see [`TableScanner`](../data/TableScanner.md)).

The default is `empty()` so new predicate types are correctness-safe-by-default.

## ComparisonPredicate

```java
new ComparisonPredicate("amount", ComparisonOperator.GT, 50.0);
```

- One-pass column evaluation: vector type is examined once, then a boxing-free primitive
  predicate (`LongPredicate` / `DoublePredicate` / `Predicate<String>`) is reused per row.
- NULL handling follows SQL three-valued logic: any comparison involving NULL yields
  `false` (the row does not match).
- Pushdown: always returns a non-empty Iceberg `Expression` via
  `ComparisonOperator.asIcebergExpression(...)`.

Supported Arrow vector types: `BigIntVector`, `IntVector`, `Float8Vector`, `Float4Vector`,
`VarCharVector`. Other vector types fall back to `getObject()` plus `Comparable`.

## ComparisonOperator

Enum with three "views" of each operator:

| View | Used for |
|---|---|
| `apply(Object, Comparable<?>)` | Generic fallback path |
| `asLongPredicate(long)`, `asDoublePredicate(double)`, `asStringPredicate(String)` | Boxing-free hot loops |
| `asIcebergExpression(String, Object)` | Pushdown to `TableScan.filter(...)` |

## AndPredicate

```java
new AndPredicate(List.of(
    new ComparisonPredicate("region", ComparisonOperator.EQUALS, "EU"),
    new ComparisonPredicate("amount", ComparisonOperator.GT, 50.0)
));
```

- Evaluates each child once, ANDs the resulting `BitSet`s together (bulk operation).
- Vacuous truth: an empty operand list matches every row.
- Pushdown: returns a combined `Expressions.and(...)` only when **all** children are
  individually pushable. If any child is not pushable, the whole AND falls back to
  residual evaluation. This is conservative but correct.

## ArrowBatchFilter

Two-phase filter:

1. **Evaluate** – call `predicate.evaluate(source)` to obtain the selection vector.
2. **Copy** – allocate a new `VectorSchemaRoot` and copy only the matching rows
   column-by-column using `FieldVector.copyFrom`.

The caller owns the returned batch and is responsible for closing it. Inside CRYOLITE
this responsibility is held by the `FilteredBatchIterable` constructed by
[`TableScanner`](../data/TableScanner.md).

## Why columnar predicates?

- **Cache locality** – iterating a contiguous column once is dramatically faster than
  jumping across columns for each row.
- **Vectorisation potential** – the selection-vector model is compatible with future
  SIMD or Gandiva-based execution.
- **Composable via bitwise ops** – AND / OR / NOT collapse to one `BitSet` operation
  per child instead of per-row short-circuit logic.

## Roadmap

The pushdown contract on `BatchPredicate` is designed to grow incrementally. Future
milestones extend the predicate model:

| Milestone | Predicate |
|---|---|
| M12 | `OrPredicate`, `NotPredicate` |
| M13 | `InPredicate` |
| M14 | `BetweenPredicate` |
| M15 | `LikePredicate` |
| M17 | Three-valued NULL semantics across all predicates |

Each new predicate type implements `BatchPredicate` and opts in to pushdown only when
the equivalent Iceberg expression preserves semantics.

## Related Components

- **[TableScanner](../data/TableScanner.md)** – orchestrates pushdown and residual evaluation
- **[CryoliteEngine.scan](../core/CryoliteEngine.md)** – public entry point that accepts a `BatchPredicate`
- **[sql package – SqlWhereConverter](../sql/README.md#sqlwhereconverter)** – Calcite AST → `BatchPredicate`

## See Also

- [Iceberg Expressions](https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/expressions/Expressions.html)
- [Apache Arrow Java – Vector types](https://arrow.apache.org/docs/java/vector.html)
