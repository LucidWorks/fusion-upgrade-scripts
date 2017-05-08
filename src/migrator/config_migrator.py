import logging
import os.path
import shutil
import re
from collections import OrderedDict

from utils.jproperties import Properties
from utils.variables_helper import VariablesHelper

logger = logging.getLogger(__name__)

class ConfigMigrator():
    # These are the defaults as of 2.4.3. Do we need different defaults for different versions?
    default_properties = {"API_JAVA_OPTIONS": "-Xmx1g -XX:MaxPermSize=256m -Dapple.awt.UIElement=true",
                          "API_PORT": "8765",
                          "API_STOP_PORT": "7765",
                          "API_STOP_KEY": "fusion",
                          "CONNECTORS_PORT": "8984",
                          "CONNECTORS_STOP_PORT": "7984",
                          "CONNECTORS_STOP_KEY": "fusion",
                          "CONNECTORS_JAVA_OPTIONS": "-Xmx2g -XX:MaxPermSize=256m -Dapple.awt.UIElement=true",
                          "SOLR_PORT": "8983",
                          "SOLR_STOP_PORT": "7983",
                          "SOLR_STOP_KEY": "fusion",
                          "SOLR_JAVA_OPTIONS": "-Xmx2g -Dapple.awt.UIElement=true",
                          "UI_PORT": "8764",
                          "UI_STOP_PORT": "7764",
                          "UI_STOP_KEY": "fusion",
                          "UI_JAVA_OPTIONS": "-Xmx512m -XX:MaxPermSize=256m -Dapple.awt.UIElement=true",
                          "SPARK_MASTER_PORT": "8766",
                          "SPARK_MASTER_UI_PORT": "8767",
                          "SPARK_MASTER_JAVA_OPTIONS": "-Xmx512m -XX:MaxPermSize=128m -Dapple.awt.UIElement=true",
                          "SPARK_JOB_SERVER_PORT": "8768",
                          "SPARK_WORKER_PORT": "8769",
                          "SPARK_WORKER_UI_PORT": "8770",
                          "SPARK_WORKER_JAVA_OPTIONS": "-Xmx1g -XX:MaxPermSize=256m -Dapple.awt.UIElement=true",
                          "ZOOKEEPER_PORT": "9983",
                          "FUSION_ZK": "localhost:9983",
                          "FUSION_SOLR_ZK": "localhost:9983",
                          "FUSION_CORS_ALLOW_ORIGIN": "\.\*",
                          "GC_LOG_OPTS": "-verbose:gc -XX:+PrintHeapAtGC -XX:+PrintGCDetails -XX:+PrintGCDateStamps "
                                         "-XX:+PrintGCTimeStamps -XX:+PrintTenuringDistribution "
                                         "-XX:+PrintGCApplicationStoppedTime -XX:+UseGCLogFileRotation "
                                         "-XX:NumberOfGCLogFiles=20 -XX:GCLogFileSize=10M",
                          "GC_TUNE": "-XX:NewRatio=3 -XX:SurvivorRatio=4 -XX:TargetSurvivorRatio=90 "
                                     "-XX:MaxTenuringThreshold=8 -XX:+UseConcMarkSweepGC -XX:+UseParNewGC "
                                     "-XX:ConcGCThreads=4 -XX:ParallelGCThreads=4 -XX:+CMSScavengeBeforeRemark "
                                     "-XX:PretenureSizeThreshold=64m -XX:+UseCMSInitiatingOccupancyOnly "
                                     "-XX:CMSInitiatingOccupancyFraction=50 -XX:CMSMaxAbortablePrecleanTime=6000 "
                                     "-XX:+CMSParallelRemarkEnabled -XX:+ParallelRefProcEnabled"}
    
    key_mapping = {"JAVA_OPTIONS": "jvmOptions",
                   "PORT": "port",
                   "STOP_PORT": "stopPort",
                   "STOP_KEY": "stopKey",
                   "FUSION_ZK": "default.zk.connect",
                   "FUSION_SOLR_ZK": "default.solrZk.connect"}
    
    services = ["API", "CONNECTORS", "ZOOKEEPER", "SPARK_MASTER", "SPARK_WORKER", "SOLR", "UI"]
    
    def __init__(self, old_fusion_version, old_home, new_home):
        self.old_fusion_version = old_fusion_version
        self.old_home = old_home
        self.new_home = new_home
    
    def get_old_variable(self, key, service):
        if VariablesHelper.is_windows():
            # in Windows, it's customary to put JVM options in each service's start script
            if key.endswith("JAVA_OPTIONS") and service is not None:
                path = os.path.join(self.old_home, "bin", service.replace("_", "-").lower() + ".cmd")
                if os.path.isfile(path):
                    value = VariablesHelper.get_variable(path, "JAVA_OPTIONS")
                    if value is not None:
                        return value.rstrip("\"")
            
            # failing that, try to find it in the normal config.cmd
            path = os.path.join(self.old_home, "conf", "config.cmd")
            return VariablesHelper.get_variable(path, key)
        else:
            old_config_path = os.path.join(self.old_home, "conf", "config.sh")
            return VariablesHelper.get_variable(old_config_path, key)
    
    def generate_new_config(self):
        new_config = OrderedDict()
        
        for old_key in ["FUSION_ZK", "FUSION_SOLR_ZK"]:
            value = self.get_old_variable(old_key, None)
            if value is not None:
                new_key = self.key_mapping[old_key]
                if old_key in self.default_properties and value != self.default_properties[old_key]:
                    new_config[new_key] = value
        
        for service in self.services:
            for key in ["JAVA_OPTIONS", "PORT", "STOP_PORT", "STOP_KEY"]:
                old_key = "{}_{}".format(service, key)
                new_key = self.get_new_key(service, key)
                value = self.get_old_variable(old_key, service)
                if value is not None:
                    if key == "JAVA_OPTIONS":
                        value = self.clean_java_options(value)
                        # check and see if they're setting hostname explicitly (APOLLO-7321)
                        match = re.search(r"\s*-Dcom.lucidworks.apollo.app.hostname=([\w.]+)\s*", value)
                        if match:
                            hostname_key = self.get_new_key(service, "address")
                            new_config[hostname_key] = match.group(1)
                            value = value.replace(match.group(), "")  # strip out the hostname arg
                    
                    logger.debug("{} ({}): {}".format(old_key, new_key, value))
                    
                    # For each thing we've found, compare against the default values from the old config. If they're
                    # the same as default, don't bother to set them. Note that that means we're potentially changing
                    # values, from old defaults to new defaults. We assume that's ok because if the old defaults were
                    # OK with them, the new defaults should be OK with them too (or even better). Basically,
                    # we only want to migrate properties that they've customized.
                    if old_key in self.default_properties and value != self.default_properties[old_key]:
                        new_config[new_key] = value
        
        logger.debug("new config: {}".format(new_config))
        return new_config
    
    def convert(self):
        new_config = self.generate_new_config()
        if len(new_config) == 0:
            logger.info("No properties to update from config.sh to fusion.properties")
            return
        
        config_file_path = os.path.join(self.new_home, "conf", "fusion.properties")
        props = Properties()
        with open(config_file_path, "r") as f:
            props.load(f)
        
        # copy the new stuff into the properties
        for k in new_config:
            props[k] = new_config[k]
        
        # Copy fusion.properties to fusion.properties.original before overwriting
        copy_to_path = os.path.join(self.new_home, "conf", "fusion.properties.original")
        shutil.copy2(config_file_path, copy_to_path)
        
        # write it
        with open(config_file_path, "w") as f:
            f.write(str(props))
            logger.info("Updated fusion.properties with the properties: {}".format(new_config))
    
    def clean_java_options(self, java_options):
        return java_options.lstrip('( ').rstrip(') ')
    
    def get_new_key(self, service, key):
        return "{}.{}".format(service.replace("_", "-").lower(), self.key_mapping.get(key, key))
