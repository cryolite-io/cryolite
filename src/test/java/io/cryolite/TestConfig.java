package io.cryolite;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Loads test configuration from environment variables (typically from .env file).
 *
 * <p>Configuration priority: 1. .env file 2. System environment variables 3. Fail if not found
 *
 * <p>The .env file is automatically loaded from the project root directory when the class is first
 * accessed.
 *
 * <p><strong>IMPORTANT:</strong> No default credentials are provided. You must configure all
 * required environment variables in your .env file or system environment.
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
    return getEnvRequired("POLARIS_URI");
  }

  public static String getPolarisClientId() {
    return getEnvRequired("POLARIS_CLIENT_ID");
  }

  public static String getPolarisClientSecret() {
    return getEnvRequired("POLARIS_CLIENT_SECRET");
  }

  public static String getPolarisWarehouse() {
    return getEnvRequired("POLARIS_WAREHOUSE");
  }

  public static String getPolarisScope() {
    return getEnvRequired("POLARIS_SCOPE");
  }

  public static String getMinioEndpoint() {
    return getEnvRequired("MINIO_ENDPOINT");
  }

  public static String getMinioAccessKey() {
    return getEnvRequired("MINIO_ACCESS_KEY");
  }

  public static String getMinioSecretKey() {
    return getEnvRequired("MINIO_SECRET_KEY");
  }

  public static String getMinioWarehousePath() {
    return getEnvRequired("MINIO_WAREHOUSE_PATH");
  }

  /**
   * Gets required environment variable value.
   *
   * <p>Priority: 1. .env file 2. System environment variable
   *
   * @param envKey the environment variable key
   * @return the environment variable value
   * @throws IllegalStateException if the environment variable is not set
   */
  private static String getEnvRequired(String envKey) {
    // First check .env file (project-specific configuration takes precedence)
    String envValue = envVars.get(envKey);
    if (envValue != null && !envValue.isEmpty()) {
      return envValue;
    }

    // Then check system environment variables
    envValue = System.getenv(envKey);
    if (envValue != null && !envValue.isEmpty()) {
      return envValue;
    }

    // Fail if not found
    throw new IllegalStateException(
        "Required environment variable '"
            + envKey
            + "' is not set. "
            + "Please configure it in your .env file or system environment.");
  }
}
