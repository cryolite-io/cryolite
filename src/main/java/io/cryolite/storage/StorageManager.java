package io.cryolite.storage;

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Manages storage connections (S3/MinIO).
 *
 * <p>Handles initialization and lifecycle of S3-compatible storage connections for Iceberg data
 * files.
 *
 * @since 0.1.0
 */
public class StorageManager {

  private final FileSystem fileSystem;
  private final String warehousePath;
  private volatile boolean closed = false;

  /**
   * Creates a StorageManager for S3-compatible storage.
   *
   * @param endpoint the S3 endpoint (e.g., http://localhost:9000)
   * @param accessKey the S3 access key
   * @param secretKey the S3 secret key
   * @param warehousePath the warehouse path (e.g., s3://bucket/warehouse)
   * @param storageOptions additional storage configuration options
   * @throws IllegalArgumentException if required parameters are null or empty
   * @throws Exception if FileSystem initialization fails
   */
  public StorageManager(
      String endpoint,
      String accessKey,
      String secretKey,
      String warehousePath,
      Map<String, String> storageOptions)
      throws Exception {
    if (endpoint == null || endpoint.isEmpty()) {
      throw new IllegalArgumentException("Endpoint cannot be null or empty");
    }
    if (warehousePath == null || warehousePath.isEmpty()) {
      throw new IllegalArgumentException("Warehouse path cannot be null or empty");
    }

    this.warehousePath = warehousePath;

    Configuration conf = new Configuration();
    conf.set("fs.s3a.endpoint", endpoint);
    conf.set("fs.s3a.access.key", accessKey);
    conf.set("fs.s3a.secret.key", secretKey);
    conf.set("fs.s3a.path.style.access", "true");
    conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

    // Apply additional storage options
    for (Map.Entry<String, String> entry : storageOptions.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }

    this.fileSystem = FileSystem.get(new Path(warehousePath).toUri(), conf);
  }

  /**
   * Gets the underlying FileSystem.
   *
   * @return the FileSystem instance
   */
  public FileSystem getFileSystem() {
    if (closed) {
      throw new IllegalStateException("StorageManager is closed");
    }
    return fileSystem;
  }

  /**
   * Gets the warehouse path.
   *
   * @return the warehouse path
   */
  public String getWarehousePath() {
    if (closed) {
      throw new IllegalStateException("StorageManager is closed");
    }
    return warehousePath;
  }

  /**
   * Checks if the storage is accessible.
   *
   * @return true if storage is accessible, false otherwise
   */
  public boolean isHealthy() {
    if (closed) {
      return false;
    }
    try {
      fileSystem.exists(new Path(warehousePath));
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Closes the storage connection.
   *
   * <p>This method is idempotent - calling it multiple times is safe.
   */
  public void close() {
    if (!closed) {
      closed = true;
      try {
        fileSystem.close();
      } catch (Exception e) {
        // Log but don't throw
      }
    }
  }

  /**
   * Checks if the storage manager is closed.
   *
   * @return true if closed, false otherwise
   */
  public boolean isClosed() {
    return closed;
  }
}
