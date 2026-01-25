package io.cryolite;

/**
 * Loads test configuration from environment variables (typically from .env file). Secrets are never
 * hardcoded in test files.
 *
 * <p>Configuration priority: 1. Environment variables (from .env) 2. Hardcoded defaults
 *
 * <p>To configure tests, set environment variables in .env file or export them before running
 * tests.
 */
public class TestConfig {

  public static String getPolarisUri() {
    return getEnv("POLARIS_URI", "http://localhost:8181/api/catalog");
  }

  public static String getPolarisClientId() {
    return getEnv("POLARIS_CLIENT_ID", "polarisadmin");
  }

  public static String getPolarisClientSecret() {
    return getEnv("POLARIS_CLIENT_SECRET", "polarisadmin");
  }

  public static String getPolarisWarehouse() {
    return getEnv("POLARIS_WAREHOUSE", "cryolite_catalog");
  }

  public static String getPolarisScope() {
    return getEnv("POLARIS_SCOPE", "PRINCIPAL_ROLE:ALL");
  }

  public static String getMinioEndpoint() {
    return getEnv("MINIO_ENDPOINT", "http://localhost:9000");
  }

  public static String getMinioAccessKey() {
    return getEnv("MINIO_ACCESS_KEY", "minioadmin");
  }

  public static String getMinioSecretKey() {
    return getEnv("MINIO_SECRET_KEY", "minioadmin");
  }

  public static String getMinioWarehousePath() {
    return getEnv("MINIO_WAREHOUSE_PATH", "s3a://cryolite-warehouse");
  }

  private static String getEnv(String envKey, String defaultValue) {
    String envValue = System.getenv(envKey);
    return (envValue != null && !envValue.isEmpty()) ? envValue : defaultValue;
  }
}
