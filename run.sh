# Copyright (c) Cosmo Tech corporation.
# Licensed under the MIT license.

set -x

docker run \
--network="host" \
-e AZURE_CLIENT_ID="$(printenv AZURE_CLIENT_ID)" \
-e AZURE_CLIENT_SECRET="$(printenv AZURE_CLIENT_SECRET)" \
-e AZURE_TENANT_ID="$(printenv AZURE_TENANT_ID)" \
-e AZURE_DIGITAL_TWINS_URL="$(printenv AZURE_DIGITAL_TWINS_URL)" \
-e TWIN_CACHE_HOST="$(printenv TWIN_CACHE_HOST)" \
-e TWIN_CACHE_NAME="$(printenv TWIN_CACHE_NAME)" \
-e TWIN_CACHE_PORT="$(printenv TWIN_CACHE_PORT)" \
-e TWIN_CACHE_ROTATION="1" \
-e LOG_LEVEL="INFO" \
adt-twincache-connector
