package io.cryolite.test;

import io.cryolite.CryoliteConfig;
import java.io.IOException;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

/**
 * Helper class for testing S3-compatible storage operations.
 *
 * <p>Provides methods to verify that data files are correctly written to S3-compatible storage
 * (e.g., MinIO, AWS S3). Works with any S3-compatible endpoint configured in CryoliteConfig.
 */
public class S3StorageTestHelper {

  private final S3Client s3Client;
  private final String bucket;

  /**
   * Creates a new S3StorageTestHelper.
   *
   * @param config the test configuration containing S3-compatible storage credentials and endpoint
   */
  public S3StorageTestHelper(CryoliteConfig config) {
    this.bucket = extractBucketFromWarehousePath(config);
    this.s3Client = createS3Client(config);
  }

  /**
   * Checks if a file exists in S3-compatible storage.
   *
   * @param filePath the S3 path to the file (e.g., "s3://bucket/path/to/file.parquet")
   * @return true if the file exists, false otherwise
   */
  public boolean fileExists(String filePath) {
    try {
      String key = extractKeyFromPath(filePath);
      HeadObjectRequest request = HeadObjectRequest.builder().bucket(bucket).key(key).build();
      s3Client.headObject(request);
      return true;
    } catch (NoSuchKeyException e) {
      return false;
    } catch (Exception e) {
      throw new RuntimeException("Error checking if file exists in S3 storage: " + filePath, e);
    }
  }

  /**
   * Gets the size of a file in S3-compatible storage.
   *
   * @param filePath the S3 path to the file
   * @return the file size in bytes
   * @throws RuntimeException if the file does not exist or an error occurs
   */
  public long getFileSize(String filePath) {
    try {
      String key = extractKeyFromPath(filePath);
      HeadObjectRequest request = HeadObjectRequest.builder().bucket(bucket).key(key).build();
      HeadObjectResponse response = s3Client.headObject(request);
      return response.contentLength();
    } catch (NoSuchKeyException e) {
      throw new RuntimeException("File not found in S3 storage: " + filePath, e);
    } catch (Exception e) {
      throw new RuntimeException("Error getting file size from S3 storage: " + filePath, e);
    }
  }

  /**
   * Closes the S3 client.
   *
   * @throws IOException if an error occurs
   */
  public void close() throws IOException {
    if (s3Client != null) {
      s3Client.close();
    }
  }

  /**
   * Creates an S3 client configured for S3-compatible storage.
   *
   * @param config the test configuration
   * @return a configured S3Client
   */
  private S3Client createS3Client(CryoliteConfig config) {
    var storageOptions = config.getStorageOptions();
    String endpoint = storageOptions.get("s3.endpoint");
    String accessKey = storageOptions.get("s3.access-key-id");
    String secretKey = storageOptions.get("s3.secret-access-key");

    S3ClientBuilder builder =
        S3Client.builder()
            .region(Region.US_WEST_2)
            .endpointOverride(java.net.URI.create(endpoint))
            .credentialsProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
            .forcePathStyle(true);

    return builder.build();
  }

  /**
   * Extracts the bucket name from the warehouse path.
   *
   * @param config the test configuration
   * @return the bucket name
   */
  private String extractBucketFromWarehousePath(CryoliteConfig config) {
    String warehousePath = config.getStorageOptions().get("warehouse-path");
    // Expected format: s3://bucket-name/path/to/warehouse
    if (warehousePath.startsWith("s3://")) {
      String[] parts = warehousePath.substring(5).split("/", 2);
      return parts[0];
    }
    throw new IllegalArgumentException("Invalid warehouse path format: " + warehousePath);
  }

  /**
   * Extracts the S3 key from a full S3 path.
   *
   * @param filePath the full S3 path (e.g., "s3://bucket/path/to/file.parquet")
   * @return the S3 key (e.g., "path/to/file.parquet")
   */
  private String extractKeyFromPath(String filePath) {
    if (filePath.startsWith("s3://")) {
      String[] parts = filePath.substring(5).split("/", 2);
      return parts.length > 1 ? parts[1] : "";
    }
    return filePath;
  }
}
