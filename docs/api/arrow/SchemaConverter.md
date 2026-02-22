# SchemaConverter

Converts Apache Iceberg schemas to Apache Arrow schemas for vectorized batch reading.

## Overview

`SchemaConverter` bridges the two schema systems so that Arrow's `VectorSchemaRoot` can carry
data that originated in an Iceberg table. It is used internally by `TableReader` but is a
standalone utility with no side effects.

## Supported Type Mappings

| Iceberg type | Arrow type |
|---|---|
| `BOOLEAN` | `Bool` |
| `INTEGER` | `Int(32, signed)` |
| `LONG` | `Int(64, signed)` |
| `FLOAT` | `FloatingPoint(SINGLE)` |
| `DOUBLE` | `FloatingPoint(DOUBLE)` |
| `STRING` | `Utf8` |
| `BINARY` | `Binary` |
| `DATE` | `Date(DAY)` |
| `TIME` | `Time(MICROSECOND, 64)` |
| `TIMESTAMP` (no tz) | `Timestamp(MICROSECOND, null)` |
| `TIMESTAMP` (with tz) | `Timestamp(MICROSECOND, "UTC")` |
| `TIMESTAMP_NANO` (no tz) | `Timestamp(NANOSECOND, null)` |
| `TIMESTAMP_NANO` (with tz) | `Timestamp(NANOSECOND, "UTC")` |
| `DECIMAL(p, s)` | `Decimal(p, s, 128)` |
| `UUID` | `FixedSizeBinary(16)` |
| `FIXED(len)` | `FixedSizeBinary(len)` |

Nested types (struct, list, map) are not supported and throw `UnsupportedOperationException`.

## Usage

```java
Schema icebergSchema = table.schema();
org.apache.arrow.vector.types.pojo.Schema arrowSchema = SchemaConverter.toArrow(icebergSchema);
```

## See Also

- [`RecordConverter`](RecordConverter.md) - Reads values from Arrow vectors back into `GenericRecord`
- [`TableReader`](../data/TableReader.md) - Uses `SchemaConverter` as part of the read pipeline

