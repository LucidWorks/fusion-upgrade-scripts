#!/usr/bin/env python

import json
import sys
import os
import argparse
import logging
import getpass
import requests

from distutils.version import StrictVersion

current_dir = os.path.dirname(__file__)
app_path = os.path.join(current_dir, "../")
sys.path.append(app_path)

from src.migrator.connectors_migrator import ConnectorsMigrator3x
from src.migrator.nlp_pipelines_migrator import PipelinesNLPMigrator, PipelinesNLPMigrator3x
from src.migrator.znodes_migration import ZNodesMigrator
from src.migrator.api_pojo_migrator import update_searchcluster_pojo
from src.migrator.proxy_pojo_migrator import update_initmeta_pojo
from src.utils.variables_helper import VariablesHelper
from src.utils.load_fusion_3x_config import load_or_generate_config
from src.utils.zookeeper_client import ZookeeperClient
from src.migrator.config_migrator import ConfigMigrator
from src.migrator.splitter_migrator import SplitterMigrator

log_format = "%(asctime)s - %(name)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s"
log_level = logging.INFO

logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(description="Migrate datasource properties")
parser.add_argument("--datasources", 
                    required=False, 
                    nargs='*',
                    help="Set 'all' to migrate all datasources with a valid "
                         "migrator implementation, or set a datasources list to be migrated")
parser.add_argument("--upgrade", 
                    required=True, 
                    choices=['config', 'zk', 'banana'],
                    help="Type of upgrade to perform")
parser.add_argument("--fusion-url", 
                    default="http://localhost:8764/api", 
                    help="URL of the Fusion proxy server")
parser.add_argument("--fusion-username", 
                    default="admin", 
                    help="Username to use when authenticating to the Fusion application (should be an admin)")
parser.add_argument("--dualZK", 
                    default="false", 
                    required=False, 
                    help="If true, will try to pull Zookeeper configs from server"
                         " associated with OLD_FUSION_HOME instead of from one associated "
                         "with FUSION_HOME", 
                    choices=['T', 't', 'True', 'true', 'F', 'f', 'false', 'False'])
parser.add_argument("--log-level",
                    default="INFO",
                    required=False,
                    help="Output log level for script run",
                    choices=['CRITICAL','ERROR','WARNING','INFO','DEBUG'])

def ensure_env_variables_defined():
    if not VariablesHelper.ensure_fusion_home():
        logger.critical("FUSION_HOME env variable is not set")
        exit(1)

    if not VariablesHelper.ensure_old_fusion_home():
        logger.critical("FUSION_OLD_HOME env variable is not set")
        exit(1)

def start_zk_client(fconfig):
    zookeeper_client = ZookeeperClient(fconfig["fusion.zk.connect"])
    zookeeper_client.start()
    zk = zookeeper_client.zk
    return zk

def stop_zk_client(zk):
    zk.stop()

def upgrade_zk_data(fusion_home, fusion_old_home, old_fusion_version, fusion_version):
    # Load the 3.0.0 config from file or generate if needed. This will load from the config generated above.
    config = load_or_generate_config(fusion_home)
    zk_client = start_zk_client(config)
    old_config = None
    old_zk_client = None
    if fusion_old_home:
    	#TODO: Check version for < 3 before assuming that need to pass in new fusion_home for jar
        old_config = load_or_generate_config(fusion_old_home, 'ui', fusion_home)
        old_zk_client = start_zk_client(old_config)

    logger.info("Migrating from fusion version '{}' to '{}'".format(old_fusion_version, fusion_version))
    if StrictVersion(fusion_version) >= StrictVersion("3.0.0") > StrictVersion(old_fusion_version):
        znode_migrator = ZNodesMigrator(config, zk_client, old_zk_client)
        logger.info("Copying znodes from old fusion paths to new paths")
        znode_migrator.start()
        logger.info("Migration from old znode paths to new paths complete")

        update_searchcluster_pojo(config, zk_client, old_zk_client)
        update_initmeta_pojo(config, zk_client, old_zk_client)

        # Update datasource payloads
        connectors_migrator = ConnectorsMigrator3x(config, zk_client, old_zk_client)
        connectors_migrator.start(data_sources_to_migrate)

        logger.info("Performing splitter migrator")
        splitter_migrator = SplitterMigrator(config, zk_client, old_zk_client)
        splitter_migrator.start()

    if StrictVersion(fusion_version) >= StrictVersion("3.0.0") and StrictVersion("2.1.4") >= StrictVersion(old_fusion_version):
        pipelines_migrator = PipelinesNLPMigrator3x(config, zk_client, old_zk_client)
        pipelines_migrator.migrate_indexpipelines()
    elif StrictVersion("2.1.4") >= StrictVersion(old_fusion_version):
        pipelines_migrator = PipelinesNLPMigrator()
        pipelines_migrator.migrate_indexpipelines()

    stop_zk_client(zk_client)
    stop_zk_client(old_zk_client)

def admin_session(url, username, password):
    headers = {"Content-type": "application/json"}
    data = {'username': username, 'password': password}

    session = requests.Session()
    resp = session.post("{0}/session".format(url), data=json.dumps(data), headers=headers)
    if resp.status_code == 201:
        return session
    else:
        logger.critical("Expected status code 201. Got {}\n{}".format(resp.status_code,resp.content))
        sys.exit(1)

def get_dashboards_from_solr(session, url):
    solr_url = "{}/apollo/query-pipelines/default/collections/system_banana/select?q=*:*&wt=json&rows=100".format(url)
    resp = session.get(solr_url)
    try:
        if resp.status_code == 200:
            dashboard_documents = resp.json()["response"]["docs"]
            logger.info("Found '{}' dashboards from collection system_banana".format(len(dashboard_documents)))
            return dashboard_documents
        else:
            logger.critical("Error retrieving documents from url {}\n. Response text is {}".format(solr_url, resp.text))
            sys.exit(1)
    except Exception as e:
        logger.error("Exception while processing request to {}".format(solr_url))
        raise e

def upload_dashboards_to_blobstore(session, url, docs):
    blob_url = "{}/apollo/blobs".format(url)
    for doc in docs:
        resp = None
        if "id" in doc:
            dashboard_url = "{}/{}?subtype=banana".format(blob_url, doc["id"])
            resp = session.put(dashboard_url, data=json.dumps([doc]))
        else:
            dashboard_url = "{}?subtype=banana".format(blob_url)
            resp = session.post(dashboard_url, data=json.dumps([doc]))
        if resp and resp.status_code == 200:
            logger.info("Uploaded dashboard {} to blob store".format(doc["id"]))
        else:
            logger.error("Dashboard upload to blob store resulted in '{}' status code. Response is '{}'".format(resp.status_code, resp.text))

def upgrade_banana_dashboards(url, username, password):
    session = admin_session(url, username, password)
    docs = get_dashboards_from_solr(session, url)
    if docs and len(docs) > 0:
        upload_dashboards_to_blobstore(session, url, docs)
    else:
        logger.info("No dashboards found in the system collection 'system_banana'")

if __name__ == "__main__":
    args = parser.parse_args()
    loger_level = args.log_level.lower()
    if loger_level == 'critical':
        log_level = logging.CRITICAL
    elif loger_level == 'error':
        log_level = logging.ERROR
    elif loger_level == 'warning':
        log_level = logging.WARNING
    elif loger_level == 'info':
        log_level = logging.INFO
    elif loger_level == 'debug':
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO
    
    logging.basicConfig(level=log_level, format=log_format)
    logger.setLevel(log_level)
    
    data_sources_to_migrate = args.datasources
    type_of_upgrade = args.upgrade
    dual_zk = args.dualZK
    ensure_env_variables_defined()
    old_fusion_version = VariablesHelper.get_old_fusion_version()
    fusion_version = VariablesHelper.get_fusion_version()
    fusion_home = VariablesHelper.FUSION_HOME
    fusion_old_home = VariablesHelper.FUSION_OLD_HOME

    if type_of_upgrade == "config":
        config_migrator = ConfigMigrator(old_fusion_version, fusion_old_home, fusion_home)
        # This is going to write a new config, based on the old config
        config_migrator.convert()
    elif type_of_upgrade == "zk":
        foh = None
        #Exclude old home by default and only try to load if specified in params
        if dual_zk and dual_zk.lower() in ['t','true']:
            foh = fusion_old_home
        upgrade_zk_data(fusion_home, foh, old_fusion_version, fusion_version)
    elif type_of_upgrade == "banana":
        url = args.fusion_url
        username = args.fusion_username
        # Start a new Fusion session
        password = getpass.getpass('Enter password for user \'{}\' at host {}: '.format(username, url))
        upgrade_banana_dashboards(url, username, password)
