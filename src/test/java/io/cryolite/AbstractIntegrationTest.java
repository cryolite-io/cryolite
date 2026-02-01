package io.cryolite;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeAll;

/**
 * Base class for integration tests that require Docker Compose services.
 *
 * <p>Provides common setup (ensuring services are running) and helper methods for creating test
 * configurations and schemas.
 */
abstract class AbstractIntegrationTest {

  @BeforeAll
  static void ensureServicesRunning() throws Exception {
    TestEnvironment.ensureServicesRunning();
  }

  /**
   * Creates a standard test configuration for CryoliteEngine.
   *
   * @return a configured CryoliteConfig instance
   */
  protected CryoliteConfig createTestConfig() {
    return new CryoliteConfig.Builder()
        .catalogOption("uri", TestConfig.getPolarisUri())
        .catalogOption(
            "credential",
            TestConfig.getPolarisClientId() + ":" + TestConfig.getPolarisClientSecret())
        .catalogOption("scope", TestConfig.getPolarisScope())
        .catalogOption("warehouse", TestConfig.getPolarisWarehouse())
        .storageOption("io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .storageOption("s3.endpoint", TestConfig.getMinioEndpoint())
        .storageOption("s3.access-key-id", TestConfig.getMinioAccessKey())
        .storageOption("s3.secret-access-key", TestConfig.getMinioSecretKey())
        .storageOption("s3.path-style-access", "true")
        .storageOption("client.region", "us-west-2")
        .storageOption("warehouse-path", TestConfig.getMinioWarehousePath())
        .build();
  }

  /**
   * Creates a standard test schema with id (long, required) and name (string, optional).
   *
   * @return a test schema
   */
  protected Schema createTestSchema() {
    return new Schema(
        Types.NestedField.required(1, "id", Types.LongType.get()),
        Types.NestedField.optional(2, "name", Types.StringType.get()));
  }

  /**
   * Generates a unique suffix for test resources based on current timestamp.
   *
   * @return a unique suffix string
   */
  protected String uniqueSuffix() {
    return String.valueOf(System.currentTimeMillis());
  }
}
