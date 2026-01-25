package io.cryolite.storage;

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.io.FileIO;

/**
 * Manages storage connections (S3/MinIO).
 *
 * <p>Handles initialization and lifecycle of S3-compatible storage connections for Iceberg data
 * files using Iceberg's native S3FileIO.
 *
 * @since 0.1.0
 */
public class StorageManager {

  private final FileIO fileIO;
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
   * @throws Exception if FileIO initialization fails
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

    // Configure S3FileIO with AWS SDK v2 properties
    Map<String, String> properties = new HashMap<>();
    properties.put("s3.endpoint", endpoint);
    properties.put("s3.access-key-id", accessKey);
    properties.put("s3.secret-access-key", secretKey);
    properties.put("s3.path-style-access", "true");

    // Apply additional storage options
    properties.putAll(storageOptions);

    // Initialize S3FileIO
    S3FileIO s3FileIO = new S3FileIO();
    s3FileIO.initialize(properties);
    this.fileIO = s3FileIO;
  }

  /**
   * Gets the underlying FileIO.
   *
   * @return the FileIO instance
   */
  public FileIO getFileIO() {
    if (closed) {
      throw new IllegalStateException("StorageManager is closed");
    }
    return fileIO;
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
      // Try to create an input file to check connectivity
      // This is a lightweight check that doesn't require file existence
      fileIO.newInputFile(warehousePath);
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
        fileIO.close();
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
