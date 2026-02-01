package io.cryolite;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;

/**
 * Utility class for managing the test environment.
 *
 * <p>Provides methods to start Docker Compose services and wait for them to become healthy using
 * polling instead of fixed sleep times.
 */
public final class TestEnvironment {

  private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(60);
  private static final Duration POLL_INTERVAL = Duration.ofMillis(500);
  private static volatile boolean initialized = false;

  private TestEnvironment() {
    // Utility class
  }

  /**
   * Ensures Docker Compose services are running and healthy.
   *
   * <p>This method is idempotent - calling it multiple times is safe and will only initialize once.
   *
   * @throws Exception if services cannot be started or do not become healthy within timeout
   */
  public static synchronized void ensureServicesRunning() throws Exception {
    if (initialized) {
      return;
    }

    startDockerCompose();
    waitForPolaris(DEFAULT_TIMEOUT);
    waitForMinio(DEFAULT_TIMEOUT);

    initialized = true;
  }

  /**
   * Starts Docker Compose services.
   *
   * @throws Exception if docker compose command fails
   */
  private static void startDockerCompose() throws Exception {
    // Use 'docker compose' (V2) with fallback to 'docker-compose' (V1)
    ProcessBuilder pb;
    try {
      pb = new ProcessBuilder("docker", "compose", "up", "-d");
      pb.redirectErrorStream(true);
      Process process = pb.start();
      int exitCode = process.waitFor();
      if (exitCode != 0) {
        throw new RuntimeException("docker compose failed, trying docker-compose");
      }
    } catch (Exception e) {
      // Fallback to docker-compose (V1)
      pb = new ProcessBuilder("docker-compose", "up", "-d");
      pb.redirectErrorStream(true);
      Process process = pb.start();
      int exitCode = process.waitFor();
      if (exitCode != 0) {
        // Read error output for debugging
        try (BufferedReader reader =
            new BufferedReader(new InputStreamReader(process.getInputStream()))) {
          StringBuilder output = new StringBuilder();
          String line;
          while ((line = reader.readLine()) != null) {
            output.append(line).append("\n");
          }
          throw new RuntimeException(
              "Failed to start Docker Compose services: " + output.toString());
        }
      }
    }
  }

  /**
   * Waits for Polaris to become healthy by polling its health endpoint.
   *
   * @param timeout maximum time to wait
   * @throws Exception if Polaris does not become healthy within timeout
   */
  private static void waitForPolaris(Duration timeout) throws Exception {
    String healthUrl = "http://localhost:8182/q/health";
    waitForHttpEndpoint(healthUrl, timeout, "Polaris");
  }

  /**
   * Waits for MinIO to become healthy by polling its health endpoint.
   *
   * @param timeout maximum time to wait
   * @throws Exception if MinIO does not become healthy within timeout
   */
  private static void waitForMinio(Duration timeout) throws Exception {
    String healthUrl = "http://localhost:9000/minio/health/live";
    waitForHttpEndpoint(healthUrl, timeout, "MinIO");
  }

  /**
   * Polls an HTTP endpoint until it returns a successful response.
   *
   * @param url the URL to poll
   * @param timeout maximum time to wait
   * @param serviceName name of the service for error messages
   * @throws Exception if endpoint does not respond successfully within timeout
   */
  private static void waitForHttpEndpoint(String url, Duration timeout, String serviceName)
      throws Exception {
    Instant deadline = Instant.now().plus(timeout);

    while (Instant.now().isBefore(deadline)) {
      try {
        HttpURLConnection connection = (HttpURLConnection) URI.create(url).toURL().openConnection();
        connection.setConnectTimeout(1000);
        connection.setReadTimeout(1000);
        connection.setRequestMethod("GET");

        int responseCode = connection.getResponseCode();
        if (responseCode >= 200 && responseCode < 300) {
          return; // Service is healthy
        }
      } catch (Exception e) {
        // Service not ready yet, continue polling
      }

      Thread.sleep(POLL_INTERVAL.toMillis());
    }

    throw new RuntimeException(
        serviceName + " did not become healthy within " + timeout.getSeconds() + " seconds");
  }
}
