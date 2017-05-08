#!/usr/bin/env python

import copy
import json

from src.utils.class_loader import ClassLoader
from src.utils.zookeeper_client import ZookeeperClient
from src.utils.variables_helper import VariablesHelper

import logging
logger = logging.getLogger(__name__)

OPENNLP_TYPE = "nlp-extractor"
LOOKUP_TYPE = "lookup-extractor"

"""
    Should be used to upgrade from 2.1.x to 2.4.x
"""
class PipelinesNLPMigrator:
    def __init__(self):
        self.class_loader = ClassLoader()
        self.zk_fusion_host = VariablesHelper.get_fusion_zookeeper_host()
        self.zk_fusion_node = VariablesHelper.get_fusion_zookeeper_node()
        self.zookeeper_client = ZookeeperClient(self.zk_fusion_host)

    def migrate_indexpipelines(self):
        INDEXPIPELINES_ZPATH = "lucid/index-pipelines"
        # Get all the index pipelines
        zk_pipelines_node = "{}/{}".format(self.zk_fusion_node, INDEXPIPELINES_ZPATH)
        self.zookeeper_client.start()

        if not self.zookeeper_client.exists(zk_pipelines_node):
            return

        children = self.zookeeper_client.get_children(zk_pipelines_node)
        for child in children:
            pipeline_node = "{}/{}".format(zk_pipelines_node, child)
            pipeline = self.zookeeper_client.get_as_json(pipeline_node)
            is_pipeline_updated = fix_pipeline_extractor_stages(pipeline)
            if is_pipeline_updated:
                logger.info("Updating pipeline '{}'".format(pipeline.get("id")))
                self.zookeeper_client.set_as_json(pipeline_node, pipeline)

def fix_pipeline_extractor_stages(pipeline):
    stages = pipeline["stages"]
    updated = False
    for stage in stages:
        if stage["type"] == OPENNLP_TYPE or stage["type"] == LOOKUP_TYPE:
            updated = True
            # Do the modification
            rules = stage["rules"]
            copy_rules = copy.deepcopy(rules)
            for index, rule in enumerate(copy_rules):
                if "definitions" in rule:
                    rules[index]["entityDefinitions"] = rules[index]["definitions"]
                    del rules[index]["definitions"]
                if "sentenceModel" in rule:
                    rules[index]["sentenceModelLocation"] = rules[index]["sentenceModel"]
                    del rules[index]["sentenceModel"]
                if "tokenizerModel" in rule:
                    rules[index]["tokenizerModelLocation"] = rules[index]["tokenizerModel"]
                    del rules[index]["tokenizerModel"]
                if stage["type"] == LOOKUP_TYPE:
                    if "entityTypes" in rule:
                        copy_entityTypes = copy.deepcopy(rule["entityTypes"])
                        for index1, entity in enumerate(copy_entityTypes):
                            if "definitions" in entity:
                                rules[index]["entityTypes"][index1]["entityDefinitions"] = entity["definitions"]
                                del rules[index]["entityTypes"][index1]["definitions"]
    return updated

"""
    Use this class to upgrade from 2.1.x to 3.0.x
"""
class PipelinesNLPMigrator3x:
    def __init__(self, config, zk, old_zk):
        self.class_loader = ClassLoader()
        self.zk_fusion_host = config["fusion.zk.connect"]
        self.zk_fusion_node = config["api.namespace"]
        self.read_zk = zk
        self.write_zk = zk
        self.old_zk = old_zk

    def migrate_indexpipelines(self):
        INDEXPIPELINES_ZPATH = "index-pipelines"
        # Get all the index pipelines
        zk_pipelines_node = "{}/{}".format(self.zk_fusion_node, INDEXPIPELINES_ZPATH)

        if self.old_zk and self.old_zk.exists(zk_pipelines_node):
            read_zk = old_zk
        elif not self.read_zk.exists(zk_pipelines_node):
            logger.info("NLP znode path {} does not exist. No migrations to perform".format(zk_pipeline_node))
            return

        children = self.read_zk.get_children(zk_pipelines_node)
        for child in children:
            pipeline_node = "{}/{}".format(zk_pipelines_node, child)
            value, zstat = self.read_zk.get(pipeline_node)
            pipeline = json.loads(value)

            is_pipeline_updated = fix_pipeline_extractor_stages(pipeline)
            if is_pipeline_updated:
                logger.info("Updating pipeline '{}'".format(pipeline.get("id")))
                self.write_zk.set_as_json(pipeline_node, pipeline)
