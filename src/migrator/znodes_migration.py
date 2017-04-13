import sys
import logging

class ZNodesMigrator:
    def __init__(self, config, zk, old_zk):
        self.config = config
        self.zk = zk
        self.old_zk = old_zk

    def start(self):
        self.migrate_solr_data()
        self.migrate_core_data()
        self.migrate_proxy_data()

    def migrate_solr_data(self):
        logging.info("Migrating Solr data to new ZK namespace {}".format(self.config["solr.namespace"]))
        solr_znodes = ["aliases.json", "clusterstate.json", "collections", "configs", "live_nodes", "overseer",
                       "overseer_elect", "security.json"]
        self.copy_znode_data(solr_znodes, "/", self.config["solr.namespace"])

    def migrate_core_data(self):
        logging.info("Migrating api data to new ZK namespace {}".format(self.config["api.namespace"]))
        old_core_znode_root = "/lucid"
        
        if self.old_zk.exists(old_core_znode_root):
        	self.copy_znode_data(self.old_zk.get_children(old_core_znode_root), old_core_znode_root, self.config["api.namespace"])
        else if self.zk.exists(old_core_znode_root):
        	self.copy_znode_data(self.zk.get_children(old_core_znode_root), old_core_znode_root, self.config["api.namespace"])
        else:
            sys.exit("Could not find zkpath '{}'".format(old_core_znode_root))

    def migrate_proxy_data(self):
        logging.info("Migrating proxy data to new ZK namespace {}".format(self.config["proxy.namespace"]))
        old_core_znode_root = "/lucid-apollo-admin"
        if self.old_zk.exists(old_core_znode_root):
        	self.copy_znode_data(self.old_zk.get_children(old_core_znode_root), old_core_znode_root, self.config["proxy.namespace"])
        else if self.zk.exists(old_core_znode_root):
        	self.copy_znode_data(self.zk.get_children(old_core_znode_root), old_core_znode_root, self.config["proxy.namespace"])
        else:
            sys.exit("Could not find zkpath '{}'".format(old_core_znode_root))

    def copy_znode_data(self, znodes, old_root, new_root):
        for node_name in znodes:
            logging.debug("Migrating znode '{}' data from old path '{}' to new path '{}'".format(node_name, old_root, new_root))
            
            # Get proper root if old root is just "/"
            if old_root == '/':
            	znode_fullpath = "/{}".format(node_name)
            else:
	            znode_fullpath = "{}/{}".format(old_root, node_name)
			
			# if oldzk is present and the node exists there then grab the information from the old host
			if self.old_zk && self.old_zk.exists(znode_fullpath):
				value, zstat = self.old_zk.get(znode_fullpath)
                children = self.old_zk.get_children(znode_fullpath)
            # if oldzk doesn't exist or have the node information then check newzk for it 
            elif self.zk.exists(znode_fullpath):  
                value, zstat = self.zk.get(znode_fullpath)
                children = self.zk.get_children(znode_fullpath)
            # if neither have the node then log the error and skip to the next one
            else:
                logging.error("Znode path '{}' does not exist".format(znode_fullpath))
                return
            
            # generate the new_fullpath
            if new_root:
            	new_fullpath = "{}/{}".format(new_root, node_name)
            else:
            	new_fullpath = "/{}".format(node_name)

            # actually save the data to the new zk or recurse to children
            if value:
                self.migrate_znode_data(new_fullpath, value)
            if children:
                self.copy_znode_data(children, znode_fullpath, new_fullpath)

    def migrate_znode_data(self, path, data):
        if not self.zk.exists(path):
            logging.debug("not copying znode '{}' since it already exists".format(path))
            real_path = self.zk.create(path, value=data, makepath=True)
