# Azure Digital Twins -> Cosmo Tech Twin Cache connector

The aim of this project is to read data from an ADT and store data into Cosmo Tech Twin Cache solution

## Changelog

### Version 0.0.3

#### Fix

- Add ADT url in key metadata 

### Version 0.0.2

#### Features

- Handle password for default account

#### Chore

- Upgrade CosmoTech_Acceleration_Library version to 0.1.3

### Version 0.0.1

#### Features

- Read ADT instance regarding environment variables
- Store data into Cosmo Tech Twin Cache instance
- Data rotation is handled
- Metrics logging

## Environment variables :

Here is the list of environment variables:

- **AZURE_CLIENT_ID** : the Azure client id (can be found under the App registration screen)
- **AZURE_TENANT_ID** : the Azure Tenant id (can be found under the App registration screen)
- **AZURE_CLIENT_SECRET** : the app client secret (an already created secret can not be retrieved, thus it must be asked from its creator or a new one should be created)
- **AZURE_DIGITAL_TWINS_URL** : the url of the ADT targeted (can be found in the specific resource screen)
- **TWIN_CACHE_HOST**: the twin cache host
- **TWIN_CACHE_PORT**: the twin cache port
- **TWIN_CACHE_NAME**: the twin cache key name where data will be stored
- **TWIN_CACHE_ROTATION**: defined the data rotation (a.k.a. the amount of keys to keep until data is overwritten) (default 1)
- **TWIN_CACHE_PASSWORD**: default account/user password (default None)

## Log level

Default log level defined is "INFO".
We use the logging API [logging](https://docs.python.org/3/library/logging.html).
You can change the log level by setting an environment variable named: **LOG_LEVEL**.
Log levels used for identifying the severity of an event. Log levels are organized from most specific to least:

- CRITICAL
- ERROR
- WARNING
- INFO
- DEBUG
- NOTSET

## How to run your image locally

### Build the docker image

`docker build -t adt-twincache-connector .`

### Run the docker image

Fill the following command with your information:

```
export AZURE_CLIENT_ID=<<azure_client_id>>
export AZURE_TENANT_ID=<azure_tenant_id>
export AZURE_CLIENT_SECRET=<azure_client_secret>
export AZURE_DIGITAL_TWINS_URL=https://<your_adt_instance>.digitaltwins.azure.net
export TWIN_CACHE_HOST=<twin_cache_host>
export TWIN_CACHE_NAME=<twin_cache_name>
export TWIN_CACHE_PORT=<twin_cache_port>
export TWIN_CACHE_PASSWORD=<twin_cache_password>
```

Then run:

`./run.sh`

**N.B:**

- Default log level is set to 'info'
- Default graph rotation is set to 1

## Tasks :

- [X]  Handle password for default connection on secured twin cache
- [ ]  Handle username/password for any secured twin cache connection
