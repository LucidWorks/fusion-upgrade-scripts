import json
import logging

logger = logging.getLogger(__name__)

def update_initmeta_pojo(config, zk, old_zk):
    initmeta_znode_path = "{}/sys/init-meta".format(config["proxy.namespace"])
    
    # if the POJO does not exist on either server, exit... Otherwise, grab it from the appropriate server
    if old_zk and old_zk.exists(initmeta_znode_path):
        value, zstat = old_zk.get(initmeta_znode_path)    
    elif zk.exists(initmeta_znode_path):
        value, zstat = zk.get(initmeta_znode_path)
    else:
        # I am leaving this in here for now but unsure why the system used to just warn and continue instead
        # of exiting or returning when the init-meta POJO doesn't exist
        logger.warn("init-meta POJO does not exist at zpath '{}'".format(initmeta_znode_path))
        #logger.critical("init-meta POJO does not exist at zpath '{}'".format(initmeta_znode_path))
        #sys.exit(1)

    # Read the payload from Zookeeper
    deser_payload = json.loads(value)
    deser_payload["datasets-installed-at"] = deser_payload.get("initialized-at")
    deser_payload["initial-db-installed-at"] = deser_payload.get("initialized-at")

    logger.info("Updating init-meta payload at path '{}'".format(initmeta_znode_path))
    # Write the updated payload to Zookeeper
    zk.set(initmeta_znode_path, value=json.dumps(deser_payload))
