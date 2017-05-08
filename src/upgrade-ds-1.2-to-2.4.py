#!/usr/bin/env python

import sys
import os
import argparse
import logging

current_dir = os.path.dirname(__file__)
app_path = os.path.join(current_dir, "../")
sys.path.append(app_path)

from src.migrator.connectors_migrator import ConnectorsMigrator
from src.migrator.nlp_pipelines_migrator import PipelinesNLPMigrator
from src.utils.variables_helper import VariablesHelper

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
parser = argparse.ArgumentParser(description="Migrate datasource properties")
parser.add_argument("--datasources", required=True, nargs='+',
                    help="Set 'all' to migrate all datasources with a valid migrator implementation, or set a datasources list to be migrated")

if __name__ == "__main__":
  args = parser.parse_args()
  data_sources_to_migrate = args.datasources

  if not VariablesHelper.ensure_fusion_home():
    logger.info("FUSION_HOME variable is not set")
    exit()

  if not VariablesHelper.ensure_old_fusion_home():
    logger.info("FUSION_OLD_HOME variable is not set")
    exit()

  connectors_migrator = ConnectorsMigrator()
  connectors_migrator.start(data_sources_to_migrate)

  pipelines_migrator = PipelinesNLPMigrator()
  pipelines_migrator.migrate_indexpipelines()
