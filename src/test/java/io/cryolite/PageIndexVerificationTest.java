package io.cryolite;

import static org.junit.jupiter.api.Assertions.*;

import io.cryolite.data.TableWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Integration test for M11.5 - Page Index and Bloom Filter verification.
 *
 * <p>Page Index (ColumnIndex / OffsetIndex) is a Parquet 1.11+ feature, enabled by Iceberg by
 * default. It allows page-level pruning inside a single row group. This test downloads the written
 * Parquet file and asserts that the footer contains a {@link ColumnIndex} and {@link OffsetIndex}
 * for the filterable column.
 *
 * <p>Bloom Filters are an optional Parquet feature that must be explicitly enabled via table
 * properties. This test enables Bloom Filters for the {@code name} column and verifies that the
 * reader can load the Bloom Filter from the data file's footer.
 */
@Tag("integration")
class PageIndexVerificationTest extends AbstractIntegrationTest {

  @Test
  void columnIndexAndOffsetIndexArePresentForFilterableColumn() throws IOException {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;

    Namespace ns = Namespace.of("test_ns_pageidx_" + uniqueSuffix());
    TableIdentifier tableId = TableIdentifier.of(ns, "page_index_table");

    try {
      nsCatalog.createNamespace(ns, new HashMap<>());

      Schema schema =
          new Schema(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.optional(2, "name", Types.StringType.get()));

      Table table = catalog.createTable(tableId, schema, PartitionSpec.unpartitioned());
      // Tiny page size forces many pages per row group so the ColumnIndex is non-trivial.
      table
          .updateProperties()
          .set("write.parquet.page-size-bytes", "1024")
          .set("write.parquet.row-group-size-bytes", "65536")
          .commit();

      try (TableWriter writer = new TableWriter(table)) {
        GenericRecord record = GenericRecord.create(table.schema());
        for (long i = 1; i <= 2000; i++) {
          record.setField("id", i);
          record.setField("name", "User-" + i);
          writer.write(record);
        }
        writer.commit();
      }

      table = catalog.loadTable(tableId);
      List<FileScanTask> tasks =
          StreamSupport.stream(table.newScan().planFiles().spliterator(), false)
              .collect(Collectors.toList());
      assertEquals(1, tasks.size(), "Should have exactly 1 data file");

      try (ParquetFileReader reader =
          openParquetFooter(table.io(), tasks.get(0).file().location())) {
        List<BlockMetaData> blocks = reader.getFooter().getBlocks();
        assertFalse(blocks.isEmpty(), "Footer must contain at least one row group");

        // Locate the 'id' column in the first row group and load its page indexes.
        ColumnChunkMetaData idChunk =
            blocks.get(0).getColumns().stream()
                .filter(c -> "id".equals(c.getPath().toDotString()))
                .findFirst()
                .orElseThrow();

        ColumnIndex columnIndex = reader.readColumnIndex(idChunk);
        OffsetIndex offsetIndex = reader.readOffsetIndex(idChunk);

        assertNotNull(columnIndex, "ColumnIndex must be written for the 'id' column");
        assertNotNull(offsetIndex, "OffsetIndex must be written for the 'id' column");
        assertTrue(
            offsetIndex.getPageCount() > 1,
            "Tiny page size must produce multiple data pages (found "
                + offsetIndex.getPageCount()
                + ")");
      }

    } finally {
      if (catalog.tableExists(tableId)) {
        catalog.dropTable(tableId, true);
      }
      nsCatalog.dropNamespace(ns);
      engine.close();
    }
  }

  @Test
  void bloomFilterIsWrittenWhenEnabledViaTableProperty() throws IOException {
    CryoliteEngine engine = new CryoliteEngine(createTestConfig());
    Catalog catalog = engine.getCatalog();
    SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;

    Namespace ns = Namespace.of("test_ns_bloom_" + uniqueSuffix());
    TableIdentifier tableId = TableIdentifier.of(ns, "bloom_table");

    try {
      nsCatalog.createNamespace(ns, new HashMap<>());

      Schema schema =
          new Schema(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.optional(2, "name", Types.StringType.get()));

      Table table = catalog.createTable(tableId, schema, PartitionSpec.unpartitioned());
      // Iceberg-specific keys to enable Bloom Filters for the 'name' column.
      table
          .updateProperties()
          .set("write.parquet.bloom-filter-enabled.column.name", "true")
          .commit();

      try (TableWriter writer = new TableWriter(table)) {
        GenericRecord record = GenericRecord.create(table.schema());
        for (long i = 1; i <= 500; i++) {
          record.setField("id", i);
          record.setField("name", "User-" + i);
          writer.write(record);
        }
        writer.commit();
      }

      table = catalog.loadTable(tableId);
      List<FileScanTask> tasks =
          StreamSupport.stream(table.newScan().planFiles().spliterator(), false)
              .collect(Collectors.toList());
      assertEquals(1, tasks.size(), "Should have exactly 1 data file");

      try (ParquetFileReader reader =
          openParquetFooter(table.io(), tasks.get(0).file().location())) {
        ColumnChunkMetaData nameChunk =
            reader.getFooter().getBlocks().get(0).getColumns().stream()
                .filter(c -> "name".equals(c.getPath().toDotString()))
                .findFirst()
                .orElseThrow();

        // A non-negative offset means the writer persisted a Bloom Filter for this column.
        assertTrue(
            nameChunk.getBloomFilterOffset() >= 0,
            "Bloom Filter offset must be set for the 'name' column");

        BloomFilter bloom = reader.readBloomFilter(nameChunk);
        assertNotNull(bloom, "Bloom Filter must be readable from the footer");
      }

    } finally {
      if (catalog.tableExists(tableId)) {
        catalog.dropTable(tableId, true);
      }
      nsCatalog.dropNamespace(ns);
      engine.close();
    }
  }
}
