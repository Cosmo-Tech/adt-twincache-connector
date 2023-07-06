# Copyright (c) Cosmo Tech corporation.
# Licensed under the MIT license.
import logging
import time
import csv
import os
import json

from CosmoTech_Acceleration_Library.Modelops.core.io.model_writer import ModelWriter
from CosmoTech_Acceleration_Library.Modelops.core.io.model_importer import ModelImporter
from CosmoTech_Acceleration_Library.Modelops.core.common.writer.CsvWriter import CsvWriter
from azure.digitaltwins.core import DigitalTwinsClient
from azure.identity import DefaultAzureCredential

logger = logging.getLogger(__name__)


def get_rels(client: DigitalTwinsClient) -> dict:
    """
    Get all relationships from ADT
    :param client: a DigitalTwinsClient
    :return: a dict containing all relationships information
    """
    logger.info("Start getting relationships...")
    rels_content = dict()
    relations_query = 'SELECT * FROM relationships'
    query_result = client.query_twins(relations_query)
    for relation in query_result:
        tr = {
            "$relationshipId": "id",
            "$sourceId": "src",
            "$targetId": "dest"
        }
        r_content = {k: v for k, v in relation.items()}
        print(f'query content {r_content}')
        for k, v in tr.items():
            r_content[v] = r_content[k]
        for k in relation.keys():
            if k[0] == '$':
                del r_content[k]
        rels_content.setdefault(relation['$relationshipName'], [])
        rels_content[relation['$relationshipName']].append(r_content)
    logger.info("...End getting relationships")
    return rels_content


def get_twins(client: DigitalTwinsClient) -> dict[str, list[dict]]:
    """
    Get all twins from ADT
    :param client: a DigitalTwinsClient
    :return: a dict containing all twins information
    """
    logger.info("Start getting twins...")
    query_expression = 'SELECT * FROM digitaltwins'
    query_result = client.query_twins(query_expression)
    twins_content = dict()
    for twin in query_result:
        entity_type = twin.get('$metadata').get('$model').split(':')[-1].split(';')[0]
        t_content = {k: v for k, v in twin.items()}
        t_content['id'] = t_content['$dtId']
        for k in twin.keys():
            if k[0] == '$':
                del t_content[k]
        twins_content.setdefault(entity_type, [])
        twins_content[entity_type].append(t_content)
    logger.info("...End getting twins")
    return twins_content


class ADTTwinCacheConnector:
    """
    Connector class to fetch data from ADT and store them into a twin cache
    """

    def __init__(self, twin_cache_host: str, twin_cache_port: int,
                 twin_cache_name: str, twin_cache_password: str = None, adt_source_url: str = "",
                 twin_cache_rotation: int = 3):
        self.credentials = DefaultAzureCredential()
        self.adt_source_url = adt_source_url
        self.twin_cache_host = twin_cache_host
        self.twin_cache_port = twin_cache_port
        self.twin_cache_name = twin_cache_name
        self.twin_cache_rotation = twin_cache_rotation
        self.twin_cache_password = twin_cache_password

    def get_data(self) -> tuple:
        """
        Retrieve all data regarding environment variables set
        :return: tuple(twins:dict, rels: dict)
        """
        logger.info("Start getting data...")
        get_data_start = time.time()
        client = DigitalTwinsClient(self.adt_source_url, self.credentials)
        twins_content_start = time.time()
        twins_content = get_twins(client)
        twins_content_timing = time.time() - twins_content_start
        rels_content_start = time.time()
        rels_content = get_rels(client)
        rels_content_timing = time.time() - rels_content_start
        get_data_timing = time.time() - get_data_start
        logger.debug(f"GetTwins took : {twins_content_timing} s")
        logger.debug(f"GetRels took : {rels_content_timing} s")
        logger.debug(f"GetAllData took : {get_data_timing} s")
        logger.info("...End getting data")
        return twins_content, rels_content

    def get_adt_to_redis_schemas(self):
        client = DigitalTwinsClient(self.adt_source_url, self.credentials)
        result_set = client.list_models(include_model_definition=True)

        redis_schemas = {"integer": "INTEGER",
                         "double": "DOUBLE",
                         "float": "FLOAT",
                         "long": "LONG",
                         "boolean": "BOOLEAN",
                         "string": "STRING",
                         "array": "ARRAY",
                         "date": "STRING",
                         "dateTime": "STRING",
                         "duration": "STRING",
                         "time": "STRING",
                         "Object": "STRING",
                         "Map": "STRING",
                         "Enum": "STRING",
                         "Array": "STRING"}

        models = [r.model for r in result_set]
        dict_dict = {}
        dict_extend = {}
        for m in models:
            contents = m["contents"]
            dict_mod = {}
            for attribute in contents:
                if attribute["@type"] == "Property":
                    try:
                        dict_mod[attribute["name"]] = redis_schemas[attribute["schema"]]
                    except TypeError as e:
                        # schema is a complexe type
                        dict_mod[attribute["name"]] = redis_schemas[attribute["schema"]['@type']]
                elif attribute["@type"] == "Relationship":
                    print(f'attribute: {attribute}')
                    dict_rel_modl = {}
                    if 'properties' in attribute:
                        properties = attribute["properties"]
                        for prop in properties:
                            try:
                                dict_rel_modl[prop["name"]] = redis_schemas[prop["schema"]]
                            except TypeError as e:
                                # schema is a complexe type
                                dict_rel_modl[prop["name"]] = redis_schemas[prop["schema"]['@type']]
                    dict_dict[attribute["name"]] = dict_rel_modl

            trimed_id = m["@id"].split(':')[-1].split(';')[0]
            dict_dict[trimed_id] = dict_mod
            # add model extends
            if 'extends' in m:
                dict_extend[trimed_id] = [ext.split(':')[-1].split(';')[0] for ext in m['extends']]
        # add extends models properties
        for model_id, extends in dict_extend.items():
            for extend in extends:
                dict_dict[model_id].update(dict_dict[extend])

        return dict_dict

    def run(self):
        """
        Run connector logic (fetch, transform and store)
        """
        logger.info("Run start...")
        run_start = time.time()
        source_data = self.get_data()
        models = self.get_adt_to_redis_schemas()
        print(models)

        # go bulk
        twin_data = source_data[0]
        rel_data = source_data[1]

        # create twins files
        twins_files_paths = []
        twins_folder_path = './twins/'
        os.makedirs(os.path.dirname(twins_folder_path), exist_ok=True)
        for twin_type, rows in twin_data.items():
            file_path = f'{twins_folder_path}/{twin_type}.csv'
            with open(file_path, 'w+') as f:
                fieldnames = list({k for r in rows for k in r})
                fieldnames.insert(0, fieldnames.pop(fieldnames.index('id')))

                schemas = models[twin_type]
                schemas.update({"id": "ID"})
                enforced_schema_fieldnames = [f'{header}:{schemas[header]}' for header in fieldnames]
                logger.debug(f'fieldnames: {enforced_schema_fieldnames}')

                logger.info(f'create new file {file_path}')
                # write header with enforced schema
                csv_w = csv.DictWriter(f, fieldnames=enforced_schema_fieldnames)
                csv_w.writeheader()

                csv_w = csv.DictWriter(f, fieldnames=fieldnames)
                for row in rows:
                    csv_w.writerow({k: CsvWriter._to_csv_format(v) for k, v in row.items()})
            twins_files_paths.append(file_path)

        # Create rels files
        rels_files_paths = []
        rels_folder_path = './rels/'
        os.makedirs(os.path.dirname(rels_folder_path), exist_ok=True)
        for rel_type, rows in rel_data.items():
            file_path = f'{rels_folder_path}/{rel_type}.csv'
            with open(file_path, 'w+') as f:
                fieldnames = list({k for r in rows for k in r})
                fieldnames.insert(0, fieldnames.pop(fieldnames.index('src')))
                fieldnames.insert(1, fieldnames.pop(fieldnames.index('dest')))

                schemas = models[rel_type]
                schemas.update({"id": "STRING"})
                schemas.update({"src": "START_ID", "dest": "END_ID"})
                enforced_schema_fieldnames = [f'{header}:{schemas[header]}' for header in fieldnames]
                logger.debug(f'fieldnames: {enforced_schema_fieldnames}')

                logger.info(f'create new file {file_path}')
                # write header with enforced schema
                csv_w = csv.DictWriter(f, fieldnames=enforced_schema_fieldnames)
                csv_w.writeheader()

                csv_w = csv.DictWriter(f, fieldnames=fieldnames)
                for row in rows:
                    csv_w.writerow({k: CsvWriter._to_csv_format(v) for k, v in row.items()})
            rels_files_paths.append(file_path)

        mi = ModelImporter(host=self.twin_cache_host, port=self.twin_cache_port,
                           name=self.twin_cache_name,
                           source_url=self.adt_source_url, graph_rotation=self.twin_cache_rotation,
                           password=self.twin_cache_password)
        mi.bulk_import(twin_file_paths=twins_files_paths, relationship_file_paths=rels_files_paths, enforce_schema=True)

        run_timing = time.time() - run_start
        logger.info(f"Run took : {run_timing} s")
