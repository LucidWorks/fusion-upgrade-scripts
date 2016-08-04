#!/usr/bin/env python

from distutils.version import StrictVersion

from src.utils.class_loader import ClassLoader
from src.utils.zookeeper_client import ZookeeperClient
from src.utils.resource_manager import ResourceManager
from src.utils.variables_helper import VariablesHelper
from src.utils.constants import *

import logging
import re
import sys

class ConnectorsMigrator:

  def __init__(self):
    self.class_loader = ClassLoader()
    self.zk_fusion_host = VariablesHelper.get_fusion_zookeeper_host()
    self.zk_fusion_node = VariablesHelper.get_fusion_zookeeper_node()
    self.fusion_version = self.cleanup_fusion_version(VariablesHelper.get_fusion_version())
    self.old_fusion_version = self.cleanup_fusion_version(VariablesHelper.get_old_fusion_version())
    self.zookeeper_client = ZookeeperClient(self.zk_fusion_host)

  def cleanup_fusion_version(self, version):
    # Remove SNAPSHOT or RC numbers from the version if present
    m = re.search('\d+\.\d+\.\d+', version)
    if m is not None:
      return m.group(0)
    else:
      logging.error("Could not extract a valid version number from '{}'".format(version))
      sys.exit(1)

  def start(self, data_source_to_migrate):
    migrators_file = ResourceManager.get_resource(MIGRATORS_FILE)
    zk_datasources_node = "{}/{}".format(self.zk_fusion_node, DATASOURCES_NODE)
    self.zookeeper_client.start()

    if not self.zookeeper_client.exists(zk_datasources_node):
      return

    if len(data_source_to_migrate) == 1 and data_source_to_migrate[0] == "all":
      children = self.zookeeper_client.get_children(zk_datasources_node)
    else:
      children = data_source_to_migrate

      for child in children:
        child_node = "{}/{}".format(zk_datasources_node, child)
        if not self.zookeeper_client.exists(child_node):
          logging.info("Node {} does not exist".format(child_node))
          return

    for child in children:
      data_source_node = "{}/{}".format(zk_datasources_node, child)
      data_source = self.zookeeper_client.get_as_json(data_source_node)
      ds_type = data_source["type"]

      logging.info("Trying to migrate datasource: %s, type: %s", child, ds_type)

      ds_version = self.old_fusion_version
      ds_migrators = migrators_file.get(ds_type, None)
      if ds_migrators and isinstance(ds_migrators, list):
        classname = None
        for counter, ds_migrator in enumerate(ds_migrators):
          classname = None
          # Match the migrator based on the current and old version
          migrator_clz = ds_migrator["migrator"]
          base_version = ds_migrator["base_version"]
          target_version = ds_migrator["target_version"]
          if StrictVersion(base_version) <= StrictVersion(ds_version) <= StrictVersion(target_version):
            classname = migrator_clz
            if classname is None:
              logging.info("No classname found for datasource type '{}' between version {} and new version {}".format(ds_type, ds_version, self.fusion_version))
              continue
            migrator = self.class_loader.get_instance(classname)
            # Start the migration
            if migrator is None:
              logging.error("Migrator '{}' does not exist or the classname is malformed".format(classname))
              continue
            logging.info("Executing {}.migrate(data_source)".format(classname))
            updated_datasource = migrator.migrate(data_source)
            ds_version = target_version
            # Only updated in ZK when it's the last migrator
            if counter == len(ds_migrators) - 1:
              print counter, len(ds_migrators)
              self.zookeeper_client.set_as_json(data_source_node, updated_datasource)
          else:
            logging.info("No migrator found for version '{}' and new version '{}' for type".format(ds_version, self.fusion_version, ds_type))
            continue
