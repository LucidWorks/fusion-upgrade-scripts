#!/usr/bin/env python

import sys
import os
import argparse
import logging

from distutils.version import StrictVersion

current_dir = os.path.dirname(__file__)
app_path = os.path.join(current_dir, "../")
sys.path.append(app_path)

from src.migrator.znodes_migration3 import ZNodesMigrator3
from src.utils.variables_helper import VariablesHelper
from src.utils.load_fusion_3x_config import load_or_generate_config3
from src.utils.zookeeper_client import ZookeeperClient
from src.migrator.api_pojo_migrator import update_searchcluster_pojo


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(name)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
parser = argparse.ArgumentParser(description="Migrate data")
parser.add_argument("--fusion-url", default="http://localhost:8764/api", help="URL of the Fusion proxy server")
parser.add_argument("--fusion-username", default="admin", help="Username to use when authenticating to the Fusion application (should be an admin)")

def ensure_env_variables_defined():
    if not VariablesHelper.ensure_fusion_home():
        logger.info("FUSION_HOME env variable is not set")
        exit()

    if not VariablesHelper.ensure_old_fusion_home():
        logger.info("FUSION_OLD_HOME env variable is not set")
        exit()

def start_zk_client(fconfig):
    zookeeper_client = ZookeeperClient(fconfig["fusion.zk.connect"])
    zookeeper_client.start()
    zk = zookeeper_client.zk
    return zk

def stop_zk_client(zk):
    zk.stop()

def upgrade_zk_data(fusion_old_home, fusion_home, old_fusion_version, fusion_version):
    config = load_or_generate_config3(fusion_home)
    old_config = load_or_generate_config3(fusion_old_home)
    print fusion_old_home
    print old_config

    zk_client = start_zk_client(config)

    logger.info("Migrating from fusion version '{}' to '{}'".format(old_fusion_version, fusion_version))
    if StrictVersion(fusion_version) > StrictVersion(old_fusion_version) >= StrictVersion("3.0.0"):
        znode_migrator = ZNodesMigrator3(old_config, config, zk_client)
        logger.info("Copying znodes from old fusion paths to new paths")
        znode_migrator.start()
        logger.info("Migration from old znode paths to new paths complete")

        update_searchcluster_pojo(config, zk_client)

    stop_zk_client(zk_client)


if __name__ == "__main__":
    args = parser.parse_args()
    ensure_env_variables_defined()
    old_fusion_version = VariablesHelper.get_old_fusion_version()
    fusion_version = VariablesHelper.get_fusion_version()
    fusion_home = VariablesHelper.FUSION_HOME
    fusion_old_home = VariablesHelper.FUSION_OLD_HOME

    upgrade_zk_data(fusion_old_home, fusion_home, old_fusion_version, fusion_version)
