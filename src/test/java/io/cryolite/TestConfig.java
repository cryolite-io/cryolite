package io.cryolite;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Loads test configuration from environment variables (typically from .env file). Secrets are never
 * hardcoded in test files.
 *
 * <p>Configuration priority: 1. System environment variables 2. .env file 3. Hardcoded defaults
 *
 * <p>The .env file is automatically loaded from the project root directory when the class is first
 * accessed.
 */
public class TestConfig {

  private static final Map<String, String> envVars = new HashMap<>();
  private static boolean loaded = false;

  static {
    loadEnvFile();
  }

  /** Loads environment variables from .env file in the project root directory. */
  private static void loadEnvFile() {
    if (loaded) {
      return;
    }

    // Try to find .env file in project root (parent of src directory)
    Path envFile = Paths.get(".env");
    if (!Files.exists(envFile)) {
      // Try alternative path (when running from different working directory)
      envFile = Paths.get("../../.env");
    }

    if (Files.exists(envFile)) {
      try (BufferedReader reader = Files.newBufferedReader(envFile)) {
        String line;
        while ((line = reader.readLine()) != null) {
          line = line.trim();
          // Skip empty lines and comments
          if (line.isEmpty() || line.startsWith("#")) {
            continue;
          }
          // Parse KEY=VALUE format
          int equalsIndex = line.indexOf('=');
          if (equalsIndex > 0) {
            String key = line.substring(0, equalsIndex).trim();
            String value = line.substring(equalsIndex + 1).trim();
            // Remove quotes if present
            if (value.startsWith("\"") && value.endsWith("\"")) {
              value = value.substring(1, value.length() - 1);
            }
            envVars.put(key, value);
          }
        }
      } catch (IOException e) {
        // Silently ignore - will fall back to defaults
        System.err.println("Warning: Could not load .env file: " + e.getMessage());
      }
    }

    loaded = true;
  }

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
    return getEnv("MINIO_WAREHOUSE_PATH", "s3://cryolite-warehouse");
  }

  /**
   * Gets environment variable value with fallback to .env file and default value.
   *
   * <p>Priority: 1. System environment variable 2. .env file 3. Default value
   */
  private static String getEnv(String envKey, String defaultValue) {
    // First check system environment variables
    String envValue = System.getenv(envKey);
    if (envValue != null && !envValue.isEmpty()) {
      return envValue;
    }

    // Then check .env file
    envValue = envVars.get(envKey);
    if (envValue != null && !envValue.isEmpty()) {
      return envValue;
    }

    // Finally use default
    return defaultValue;
  }
}
