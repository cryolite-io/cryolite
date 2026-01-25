#!/bin/bash
set -e

# Setup Polaris catalog for integration tests
# This script creates a catalog, principal, and grants necessary privileges

POLARIS_URL="${POLARIS_URL:-http://localhost:8181}"
ROOT_CLIENT_ID="${ROOT_CLIENT_ID}"
ROOT_CLIENT_SECRET="${ROOT_CLIENT_SECRET}"

if [ -z "$ROOT_CLIENT_ID" ] || [ -z "$ROOT_CLIENT_SECRET" ]; then
  echo "Error: ROOT_CLIENT_ID and ROOT_CLIENT_SECRET must be set"
  exit 1
fi

echo "Setting up Polaris catalog at $POLARIS_URL..."

# Get OAuth token for root principal
echo "Getting root OAuth token..."
ROOT_TOKEN_RESPONSE=$(curl -s -X POST "$POLARIS_URL/api/catalog/v1/oauth/tokens" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -u "$ROOT_CLIENT_ID:$ROOT_CLIENT_SECRET" \
  -d "grant_type=client_credentials&scope=PRINCIPAL_ROLE:ALL")

ROOT_TOKEN=$(echo "$ROOT_TOKEN_RESPONSE" | grep -o '"access_token":"[^"]*"' | cut -d'"' -f4)

if [ -z "$ROOT_TOKEN" ]; then
  echo "Error: Failed to get root OAuth token"
  echo "Response: $ROOT_TOKEN_RESPONSE"
  exit 1
fi

echo "Root token obtained successfully"

# Create catalog
# For INTERNAL catalogs with Polaris configured with S3 environment variables,
# we don't need to specify storageConfigInfo - Polaris will use the global config
echo "Creating catalog 'cryolite_catalog'..."
CATALOG_RESPONSE=$(curl -s -X POST "$POLARIS_URL/api/management/v1/catalogs" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $ROOT_TOKEN" \
  -d '{
    "catalog": {
      "name": "cryolite_catalog",
      "type": "INTERNAL",
      "properties": {
        "default-base-location": "s3a://cryolite-warehouse/"
      },
      "storageConfigInfo": {
        "storageType": "S3",
        "allowedLocations": ["s3a://cryolite-warehouse/"]
      }
    }
  }')

echo "Catalog response: $CATALOG_RESPONSE"

# Create principal
echo "Creating principal 'cryolite_user'..."
PRINCIPAL_RESPONSE=$(curl -s -X POST "$POLARIS_URL/api/management/v1/principals" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $ROOT_TOKEN" \
  -d '{
    "principal": {
      "name": "cryolite_user",
      "type": "SERVICE"
    }
  }')

echo "Principal response: $PRINCIPAL_RESPONSE"

USER_CLIENT_ID=$(echo "$PRINCIPAL_RESPONSE" | grep -o '"clientId":"[^"]*"' | cut -d'"' -f4)
USER_CLIENT_SECRET=$(echo "$PRINCIPAL_RESPONSE" | grep -o '"clientSecret":"[^"]*"' | cut -d'"' -f4)

if [ -z "$USER_CLIENT_ID" ]; then
  echo "Warning: Principal may already exist, using root credentials"
  USER_CLIENT_ID="$ROOT_CLIENT_ID"
  USER_CLIENT_SECRET="$ROOT_CLIENT_SECRET"
fi

# Create principal role
echo "Creating principal role 'cryolite_user_role'..."
PRINCIPAL_ROLE_RESPONSE=$(curl -s -X POST "$POLARIS_URL/api/management/v1/principal-roles" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $ROOT_TOKEN" \
  -d '{
    "principalRole": {
      "name": "cryolite_user_role"
    }
  }')

echo "Principal role response: $PRINCIPAL_ROLE_RESPONSE"

# Create catalog role
echo "Creating catalog role 'cryolite_catalog_role'..."
CATALOG_ROLE_RESPONSE=$(curl -s -X POST "$POLARIS_URL/api/management/v1/catalogs/cryolite_catalog/catalog-roles" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $ROOT_TOKEN" \
  -d '{
    "catalogRole": {
      "name": "cryolite_catalog_role"
    }
  }')

echo "Catalog role response: $CATALOG_ROLE_RESPONSE"

# Grant principal role to principal
echo "Granting principal role to principal..."
GRANT_PRINCIPAL_RESPONSE=$(curl -s -X PUT "$POLARIS_URL/api/management/v1/principals/cryolite_user/principal-roles" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $ROOT_TOKEN" \
  -d '{
    "principalRole": {
      "name": "cryolite_user_role"
    }
  }')

echo "Grant principal response: $GRANT_PRINCIPAL_RESPONSE"

# Grant catalog role to principal role
echo "Granting catalog role to principal role..."
GRANT_CATALOG_ROLE_RESPONSE=$(curl -s -X PUT "$POLARIS_URL/api/management/v1/catalogs/cryolite_catalog/catalog-roles/cryolite_catalog_role/principal-roles" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $ROOT_TOKEN" \
  -d '{
    "principalRole": {
      "name": "cryolite_user_role"
    }
  }')

echo "Grant catalog role response: $GRANT_CATALOG_ROLE_RESPONSE"

# Grant privileges to catalog role
echo "Granting CATALOG_MANAGE_CONTENT privilege..."
GRANT_PRIVILEGE_RESPONSE=$(curl -s -X PUT "$POLARIS_URL/api/management/v1/catalogs/cryolite_catalog/catalog-roles/cryolite_catalog_role/grants" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $ROOT_TOKEN" \
  -d '{
    "grant": {
      "type": "catalog",
      "privilege": "CATALOG_MANAGE_CONTENT"
    }
  }')

echo "Grant privilege response: $GRANT_PRIVILEGE_RESPONSE"

# Get OAuth token for user
echo "Getting user OAuth token..."
USER_TOKEN_RESPONSE=$(curl -s -X POST "$POLARIS_URL/api/catalog/v1/oauth/tokens" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -u "$USER_CLIENT_ID:$USER_CLIENT_SECRET" \
  -d "grant_type=client_credentials&scope=PRINCIPAL_ROLE:ALL")

USER_TOKEN=$(echo "$USER_TOKEN_RESPONSE" | grep -o '"access_token":"[^"]*"' | cut -d'"' -f4)

echo ""
echo "Polaris setup complete!"
echo "User Client ID: $USER_CLIENT_ID"
echo "User Client Secret: $USER_CLIENT_SECRET"
echo "User Token: ${USER_TOKEN:0:20}..."
echo ""
echo "Export these for tests:"
echo "export POLARIS_USER_CLIENT_ID='$USER_CLIENT_ID'"
echo "export POLARIS_USER_CLIENT_SECRET='$USER_CLIENT_SECRET'"
echo "export POLARIS_USER_TOKEN='$USER_TOKEN'"

