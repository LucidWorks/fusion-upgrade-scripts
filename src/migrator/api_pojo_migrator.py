import logging
import json
import sys

def update_searchcluster_pojo(config, zk, old_zk):
    solr_zk_connect_string = config["solr.zk.connect"]
    searchcluster_znode_path = "{}/search-clusters/default".format(config["api.namespace"])
    
    # if the POJO does not exist on either server, exit... Otherwise, grab it from the appropriate server
    if old_zk and old_zk.exists(searchcluster_znode_path):
        value, zstat = old_zk.get(searchcluster_znode_path)    
    elif zk.exists(searchcluster_znode_path):
        value, zstat = zk.get(searchcluster_znode_path)
    else:
        sys.exit("Search cluster POJO does not exist at zpath '{}'".format(searchcluster_znode_path))

    # Read the payload from Zookeeper
    
    deser_payload = json.loads(value)
    deser_payload["connectString"] = solr_zk_connect_string

    logging.info("Updating search-cluster payload at path '{}'".format(searchcluster_znode_path))
    # Write the updated payload to Zookeeper
    zk.set(searchcluster_znode_path, value=json.dumps(deser_payload))
