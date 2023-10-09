# Copyright (c) Cosmo Tech corporation.
# Licensed under the MIT license.
import logging
import os
import sys

from ADT_TwinCache_Connector.adt_connector import ADTTwinCacheConnector

main_logger_name = "ADTTwinCacheConnector_main"

env_var_required = ["AZURE_CLIENT_ID", "AZURE_TENANT_ID", "AZURE_CLIENT_SECRET", "AZURE_DIGITAL_TWINS_URL",
                    "TWIN_CACHE_HOST", "TWIN_CACHE_PORT", "TWIN_CACHE_NAME"]

missing_env_vars = []


def check_env_var():
    """
    Check if all required environment variables are specified
    """
    for env_var in env_var_required:
        if env_var not in os.environ:
            missing_env_vars.append(env_var)


if __name__ == "__main__":

    log_level_name = os.getenv("LOG_LEVEL") if "LOG_LEVEL" in os.environ else "INFO"
    log_level = logging.getLevelName(log_level_name)
    logging.basicConfig(stream=sys.stdout, level=log_level,
                        format='%(levelname)s(%(name)s) - %(asctime)s - %(message)s',
                        datefmt='%d-%m-%y %H:%M:%S')
    logger = logging.getLogger(__name__)

    check_env_var()
    if not missing_env_vars:
        adt_source_url = os.getenv("AZURE_DIGITAL_TWINS_URL")
        twin_cache_host = os.getenv("TWIN_CACHE_HOST")
        twin_cache_port = os.getenv("TWIN_CACHE_PORT")
        twin_cache_name = os.getenv("TWIN_CACHE_NAME")
        twin_cache_password = os.getenv("TWIN_CACHE_PASSWORD")

        ADTTwinCacheConnector(adt_source_url=adt_source_url, twin_cache_host=twin_cache_host,
                              twin_cache_port=twin_cache_port, twin_cache_name=twin_cache_name,
                              twin_cache_password=twin_cache_password).run()
    else:
        raise Exception(f"Missing environment variables named {missing_env_vars}")
