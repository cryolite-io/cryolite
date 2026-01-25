package io.cryolite;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Loads test configuration from test.properties or environment variables. Secrets are never
 * hardcoded in test files.
 */
public class TestConfig {
  private static final Properties properties = new Properties();

  static {
    try (InputStream input =
        TestConfig.class.getClassLoader().getResourceAsStream("test.properties")) {
      if (input != null) {
        properties.load(input);
      }
    } catch (IOException e) {
      // test.properties not found, will use environment variables
    }
  }

  public static String getPolarisUri() {
    return getProperty("polaris.uri", "POLARIS_URI", "http://localhost:8181/api/catalog");
  }

  public static String getPolarisClientId() {
    // First try environment variable from .env, then test.properties, then default
    String envValue = System.getenv("POLARIS_CLIENT_ID");
    if (envValue != null && !envValue.isEmpty()) {
      return envValue;
    }
    return getProperty("polaris.client.id", "POLARIS_CLIENT_ID", "polarisadmin");
  }

  public static String getPolarisClientSecret() {
    // First try environment variable from .env, then test.properties, then default
    String envValue = System.getenv("POLARIS_CLIENT_SECRET");
    if (envValue != null && !envValue.isEmpty()) {
      return envValue;
    }
    return getProperty("polaris.client.secret", "POLARIS_CLIENT_SECRET", "polarisadmin");
  }

  public static String getPolarisWarehouse() {
    String envValue = System.getenv("POLARIS_WAREHOUSE");
    if (envValue != null && !envValue.isEmpty()) {
      return envValue;
    }
    return getProperty("polaris.warehouse", "POLARIS_WAREHOUSE", "cryolite_catalog");
  }

  public static String getPolarisScope() {
    String envValue = System.getenv("POLARIS_SCOPE");
    if (envValue != null && !envValue.isEmpty()) {
      return envValue;
    }
    return getProperty("polaris.scope", "POLARIS_SCOPE", "PRINCIPAL_ROLE:ALL");
  }

  public static String getMinioEndpoint() {
    return getProperty("minio.endpoint", "MINIO_ENDPOINT", "http://localhost:9000");
  }

  public static String getMinioAccessKey() {
    return getProperty("minio.access.key", "MINIO_ACCESS_KEY", "minioadmin");
  }

  public static String getMinioSecretKey() {
    return getProperty("minio.secret.key", "MINIO_SECRET_KEY", "minioadmin");
  }

  public static String getMinioWarehousePath() {
    return getProperty("minio.warehouse.path", "MINIO_WAREHOUSE_PATH", "s3a://cryolite-warehouse");
  }

  private static String getProperty(String propertyKey, String envKey, String defaultValue) {
    // First try environment variable
    String envValue = System.getenv(envKey);
    if (envValue != null && !envValue.isEmpty()) {
      return envValue;
    }

    // Then try properties file
    String propValue = properties.getProperty(propertyKey);
    if (propValue != null && !propValue.isEmpty()) {
      return propValue;
    }

    // Finally use default
    return defaultValue;
  }
}
