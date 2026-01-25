package io.cryolite;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Configuration for CryoliteEngine.
 *
 * <p>Holds configuration for catalog, storage, and other engine options. Supports both catalog and
 * storage configuration without embedding secrets in code.
 *
 * @since 0.1.0
 */
public class CryoliteConfig {

  private final String catalogType;
  private final Map<String, String> catalogOptions;
  private final String storageType;
  private final Map<String, String> storageOptions;
  private final Map<String, String> engineOptions;

  private CryoliteConfig(Builder builder) {
    this.catalogType = builder.catalogType;
    this.catalogOptions = new HashMap<>(builder.catalogOptions);
    this.storageType = builder.storageType;
    this.storageOptions = new HashMap<>(builder.storageOptions);
    this.engineOptions = new HashMap<>(builder.engineOptions);
  }

  public String getCatalogType() {
    return catalogType;
  }

  public Map<String, String> getCatalogOptions() {
    return new HashMap<>(catalogOptions);
  }

  public String getStorageType() {
    return storageType;
  }

  public Map<String, String> getStorageOptions() {
    return new HashMap<>(storageOptions);
  }

  public Map<String, String> getEngineOptions() {
    return new HashMap<>(engineOptions);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CryoliteConfig that = (CryoliteConfig) o;
    return Objects.equals(catalogType, that.catalogType)
        && Objects.equals(catalogOptions, that.catalogOptions)
        && Objects.equals(storageType, that.storageType)
        && Objects.equals(storageOptions, that.storageOptions)
        && Objects.equals(engineOptions, that.engineOptions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(catalogType, catalogOptions, storageType, storageOptions, engineOptions);
  }

  @Override
  public String toString() {
    return "CryoliteConfig{"
        + "catalogType='"
        + catalogType
        + '\''
        + ", storageType='"
        + storageType
        + '\''
        + '}';
  }

  /** Builder for CryoliteConfig. */
  public static class Builder {
    private String catalogType = "rest";
    private final Map<String, String> catalogOptions = new HashMap<>();
    private String storageType = "s3";
    private final Map<String, String> storageOptions = new HashMap<>();
    private final Map<String, String> engineOptions = new HashMap<>();

    public Builder catalogType(String catalogType) {
      this.catalogType = catalogType;
      return this;
    }

    public Builder catalogOption(String key, String value) {
      this.catalogOptions.put(key, value);
      return this;
    }

    public Builder storageType(String storageType) {
      this.storageType = storageType;
      return this;
    }

    public Builder storageOption(String key, String value) {
      this.storageOptions.put(key, value);
      return this;
    }

    public Builder engineOption(String key, String value) {
      this.engineOptions.put(key, value);
      return this;
    }

    public CryoliteConfig build() {
      return new CryoliteConfig(this);
    }
  }
}
