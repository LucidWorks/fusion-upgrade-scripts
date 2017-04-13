import sys
import logging

class ZNodesMigrator3:
    logger = logging.getLogger(__name__)
    def __init__(self, old_config, config, zk):
        self.old_config = old_config
        self.config = config
        self.zk = zk

    def start(self):
        self.migrate_solr_data()
        self.migrate_core_data()
        self.migrate_proxy_data()

    def migrate_solr_data(self):
        logger.info("Migrating Solr data from {} to new ZK namespace {}".format(self.old_config["solr.namespace"], self.config["solr.namespace"]))
        old_solr_znode_root = self.old_config["solr.namespace"]
        if not self.zk.exists(old_solr_znode_root):
            logger.critical("Could not find zkpath '{}'".format(old_solr_znode_root))
            sys.exit(1)
        solr_znodes = ["aliases.json", "clusterstate.json", "collections", "configs", "live_nodes", "overseer",
                       "overseer_elect", "security.json"]
        self.copy_znode_data(solr_znodes, self.old_config["solr.namespace"], self.config["solr.namespace"])

    def migrate_core_data(self):
        logger.info("Migrating api data from {} to new ZK namespace {}".format(self.old_config["api.namespace"], self.config["api.namespace"]))
        old_core_znode_root = self.old_config["api.namespace"]
        if not self.zk.exists(old_core_znode_root):
            logger.critical("Could not find zkpath '{}'".format(old_core_znode_root))
            sys.exit(1)
        self.copy_znode_data(self.zk.get_children(old_core_znode_root), old_core_znode_root, self.config["api.namespace"])

    def migrate_proxy_data(self):
        logger.info("Migrating proxy data from {} to new ZK namespace {}".format(self.old_config["proxy.namespace"], self.config["proxy.namespace"]))
        old_core_znode_root = self.old_config["proxy.namespace"]
        if not self.zk.exists(old_core_znode_root):
            logger.critical("Could not find zkpath '{}'".format(old_core_znode_root))
            sys.exit(1)
        self.copy_znode_data(self.zk.get_children(old_core_znode_root), old_core_znode_root, self.config["proxy.namespace"])

    def copy_znode_data(self, znodes, old_root, new_root):
        for node_name in znodes:
            logger.debug("Migrating znode '{}' data from old path '{}' to new path '{}'".format(node_name, old_root, new_root))
            znode_fullpath = "{}/{}".format(old_root, node_name)
            if self.zk.exists(znode_fullpath):
                value, zstat = self.zk.get(znode_fullpath)
                new_fullpath = "{}/{}".format(new_root, node_name)
                self.migrate_znode_data(new_fullpath, value)
                children = self.zk.get_children(znode_fullpath)
                self.copy_znode_data(children, znode_fullpath, "{}/{}".format(new_root, node_name))
            else:
                logger.error("Znode path '{}' does not exist".format(znode_fullpath))

    def migrate_znode_data(self, path, data):
        if not self.zk.exists(path):
            logger.debug("not copying znode '{}' since it already exists".format(path))
            real_path = self.zk.create(path, value=data, makepath=True)
