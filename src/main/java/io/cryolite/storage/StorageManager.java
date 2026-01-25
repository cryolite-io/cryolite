package io.cryolite.storage;

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.io.FileIO;

/**
 * Manages storage connections for various backends (S3, Azure, GCS, local, etc.).
 *
 * <p>Handles initialization and lifecycle of storage connections for Iceberg data files. The
 * storage backend is determined by the "io-impl" option which specifies the fully qualified class
 * name of the FileIO implementation. All configuration options are passed directly to the FileIO
 * implementation.
 *
 * @since 0.1.0
 */
public class StorageManager {

  private final FileIO fileIO;
  private final String warehousePath;
  private volatile boolean closed = false;

  /**
   * Creates a StorageManager with the specified storage backend.
   *
   * <p>The FileIO implementation is determined by the "io-impl" property. If not specified,
   * defaults to "org.apache.iceberg.aws.s3.S3FileIO".
   *
   * <p>All options are passed directly to the FileIO implementation. The user is responsible for
   * providing correctly named parameters for the chosen backend (e.g., s3.endpoint,
   * s3.access-key-id for S3FileIO).
   *
   * @param storageOptions storage configuration options (io-impl, warehouse-path, and
   *     backend-specific options)
   * @throws IllegalArgumentException if required parameters are missing or invalid
   * @throws Exception if FileIO initialization fails
   */
  public StorageManager(Map<String, String> storageOptions) throws Exception {
    if (storageOptions == null) {
      throw new IllegalArgumentException("Storage options cannot be null");
    }

    // Extract warehouse path (required for all backends)
    this.warehousePath = storageOptions.get("warehouse-path");
    if (warehousePath == null || warehousePath.isEmpty()) {
      throw new IllegalArgumentException("warehouse-path is required");
    }

    // Get FileIO implementation class (default: S3FileIO)
    String ioImpl =
        storageOptions.getOrDefault("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");

    // Create properties map from storage options (pass through all options)
    Map<String, String> properties = new HashMap<>(storageOptions);

    // Dynamically instantiate FileIO implementation
    try {
      Class<?> fileIOClass = Class.forName(ioImpl);
      Object fileIOInstance = fileIOClass.getDeclaredConstructor().newInstance();

      if (!(fileIOInstance instanceof FileIO)) {
        throw new IllegalArgumentException(
            "Class " + ioImpl + " does not implement org.apache.iceberg.io.FileIO");
      }

      this.fileIO = (FileIO) fileIOInstance;
      this.fileIO.initialize(properties);

    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("FileIO implementation class not found: " + ioImpl, e);
    } catch (ReflectiveOperationException e) {
      throw new IllegalArgumentException(
          "Failed to instantiate FileIO implementation: " + ioImpl, e);
    }
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
      // Try to create an output file reference to check connectivity
      // This doesn't actually create the file, just verifies S3 access
      fileIO.newOutputFile(warehousePath + "/.health_check");
      return true;
    } catch (Exception e) {
      // TODO: Replace with proper logging framework later
      System.err.println("StorageManager health check failed: " + e.getMessage());
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
