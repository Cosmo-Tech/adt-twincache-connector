# Copyright (c) Cosmo Tech corporation.
# Licensed under the MIT license.
import logging
import time

from CosmoTech_Acceleration_Library.Modelops.core.io.model_writer import ModelWriter
from CosmoTech_Acceleration_Library.Modelops.core.utils.model_util import ModelUtil
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
        for k, v in tr.items():
            r_content[v] = r_content[k]
        for k in relation.keys():
            if k[0] == '$':
                del r_content[k]
        rels_content.setdefault(relation['$relationshipName'], [])
        rels_content[relation['$relationshipName']].append(r_content)
    logger.info("...End getting relationships")
    return rels_content


def get_twins(client: DigitalTwinsClient) -> dict:
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
        t_content['dt_id'] = t_content['$dtId']
        for k in twin.keys():
            if k[0] == '$':
                del t_content[k]
        twins_content.setdefault(entity_type, [])
        twins_content[entity_type].append(t_content)
    logger.info("...End getting twins")
    return twins_content


def transform_data(data: tuple) -> dict:
    """
    Transform tuple data to tuple queries
    :param data: twins and relationships information (tuple(twins:dict, rels: dict))
    :return: a tuple containing queries (tuple(twins_queries:dict, rels_queries: dict))
    """
    logger.info("Start transforming data...")
    twin_data = data[0]
    twin_types = twin_data.keys()
    twin_queries = []
    for twin_type in twin_types:
        twin_instances = twin_data[twin_type]
        for twin_instance in twin_instances:
            query = ModelUtil.create_twin_query(twin_type, twin_instance)
            twin_queries.append(query)

    rel_data = data[1]
    rel_types = rel_data.keys()
    rel_queries = []
    for rel_type in rel_types:
        rel_instances = rel_data[rel_type]
        for rel_instance in rel_instances:
            rel_queries.append(ModelUtil.create_relationship_query(rel_type, rel_instance))
    logger.info("...End transforming data")
    return twin_queries, rel_queries


class ADTTwinCacheConnector:
    """
    Connector class to fetch data from ADT and store them into a twin cache
    """

    def __init__(self, adt_source_url: str, twin_cache_host: str, twin_cache_port: int,
                 twin_cache_name: str, twin_cache_password: str = None, twin_cache_rotation: int = 1):
        self.credentials = DefaultAzureCredential()
        self.adt_source_url = adt_source_url
        self.twin_cache_host = twin_cache_host
        self.twin_cache_port = twin_cache_port
        self.twin_cache_name = twin_cache_name
        self.twin_cache_rotation = twin_cache_rotation
        self.mw = ModelWriter(host=twin_cache_host, port=twin_cache_port,
                              name=twin_cache_name, graph_rotation=twin_cache_rotation, password=twin_cache_password)

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

    def store_data(self, data: tuple):
        """
        Store data into a twin cache
        :param data: tuple(twins:dict, rels: dict)
        """
        logger.debug("Start storing data...")
        twin_queries = data[0]
        rel_queries = data[1]
        store_data_start = time.time()
        create_twins_start = time.time()
        for twin_query in twin_queries:
            self.mw.graph.query(twin_query, read_only=False)

        create_twins_timing = time.time() - create_twins_start
        create_rels_start = time.time()
        for rel_query in rel_queries:
            self.mw.graph.query(rel_query, read_only=False)
        create_rels_timing = time.time() - create_rels_start
        store_data_timing = time.time() - store_data_start
        logger.debug(f"Create Twins took : {create_twins_timing} s")
        logger.debug(f"Create Rels took : {create_rels_timing} s")
        logger.debug(f"Create all data took : {store_data_timing} s")

    def run(self):
        """
        Run connector logic (fetch, transform and store)
        """
        logger.info("Run start...")
        run_start = time.time()
        source_data = self.get_data()
        prepared_data = transform_data(source_data)
        self.store_data(prepared_data)
        run_timing = time.time() - run_start
        logger.info(f"Run took : {run_timing} s")
