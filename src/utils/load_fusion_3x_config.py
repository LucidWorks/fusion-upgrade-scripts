import os
import sys
import logging
import subprocess
import shlex
import json

logger = logging.getLogger(__name__)

def get_property(fusion_home, property_name, service="api"):
    jar_path = os.path.join(fusion_home, "apps", "lucidworks-agent.jar")
    command = "java -DFUSION_HOME=\"{0}\" -jar \"{1}\" config -p {2} {3}".format(fusion_home, jar_path, property_name,
                                                                                 service)
    args = shlex.split(command)
    # we don't need stderr and stdin, but StackOverflow suggested that might be necessary for communicate() to work on Windows.
    # Although that SO post was referring to Python 2.6, so maybe that's changed.
    popen = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)
    return_code = popen.wait()
    out, err = popen.communicate()
    # Sometimes we accidentally print bogus stuff to the command line in addition to the actual property.
    # The last line is most likely the real result.
    result = out.splitlines()[-1]
    logger.info("Got return code {} while running config command '{}' in {}. Result was: {} \nstderr: {}".format(return_code, command, fusion_home, result, err))
    return result


def generate_config_file(fusion_home, service="ui", jar_home=None):
    output_file_name = "{}.config.json".format(service)
    # we used to write the temporary config file out to FUSION_HOME, but that breaks on Windows for reasons I don't
    # understand.
    # But it's not necessary, and possibly not desired since it could be confusing to have something that looks like a
    # config file there in a user's FUSION_HOME. So we'll write it into the current directory instead.
    file_fullpath = output_file_name
    # even if the file exists, it could have been generated off a seperate set of fusion configs or even a different 
    # fusion version so we should ALWAYS recreate it
    if os.path.exists(file_fullpath):
        logger.info("File '{}' exists, deleting old version".format(file_fullpath))
        os.remove(file_fullpath)
    logger.info("Creating config file using agent")
    #allow for the jar to be in a different version in case am upgrading from < 3
    if jar_home:
    	jar_path = os.path.join(jar_home, "apps", "lucidworks-agent.jar")
    else:
    	jar_path = os.path.join(fusion_home, "apps", "lucidworks-agent.jar")
    command = "java -DFUSION_HOME=\"{0}\" -jar \"{1}\" config -o \"{2}\" {3}".format(fusion_home, jar_path,
                                                                                     file_fullpath, service)
    args = shlex.split(command)
    popen = subprocess.Popen(args)
    return_code = popen.wait()
    if not os.path.exists(file_fullpath):
        logger.critical("Failed to generate config file using the command '{}' at path '{}'. Return code '{}'".format(command, file_fullpath, return_code))
        sys.exit(1)
    logger.info("Generated config file at path '{}'".format(file_fullpath))
    return file_fullpath


def load_config_from_file(path, service="ui"):
    if not os.path.exists(path):
        logger.critical("CRITICAL: Config file does not exist at path '{0}'.\n Please run"
                         " 'java -jar apps/lucidworks-agent.jar config -o {1}.config.json {1}' command from '{2}' "
                         "to generate the config file".format(path, service, VariablesHelper.FUSION_HOME))
        sys.exit(1)

    deser_payload = json.load(open(path))
    config = {}
    try:
        config["fusion.zk.connect"] = deser_payload["zk"]["connect"]
        config["solr.zk.connect"] = deser_payload["solrZk"]["connect"]
        system_props = json.loads(deser_payload["systemProps"])
        if system_props.has_key("com.lucidworks.apollo.api.curator.namespace"):
            config["api.namespace"] = system_props["com.lucidworks.apollo.api.curator.namespace"]
        if system_props.has_key("com.lucidworks.apollo.admin.db.zk.namespace"):
            config["proxy.namespace"] = system_props["com.lucidworks.apollo.admin.db.zk.namespace"]
    except Exception as e:
        logger.error("Could not read config from the file {} ".format(path))
        raise e
    return config


def parse_solr_namespace(solr_zk_connect):
    index = solr_zk_connect.find("/")
    if index != -1:
        return solr_zk_connect[index:len(solr_zk_connect)]
    else:
        logger.error("Could not find any namespace in the Solr ZK connection string '{}'".format(solr_zk_connect))


def load_or_generate_config(fusion_home, service="ui", jar_home=None):
    config_file_path = generate_config_file(fusion_home, service, jar_home)
    config = load_config_from_file(config_file_path, service)
    os.remove(config_file_path)
    # you're assuming here that if zk is external then solr is also external which may not be correct. You can have
    # external zk and internal solr resulting in only a fusion.zk.connect string so you have to account for them both
    # Sidenote: I honestly have no idea why the config is being generated with a solr.zk.connect string when it isn't
    # defined in the fusion.properties... something in that Jar being run to generate the config is off
    config["solr.namespace"] = parse_solr_namespace(config["solr.zk.connect"] or config["fusion.zk.connect"])
    return config


def load_or_generate_config3(fusion_home, service="ui", jar_home=None):
    config_file_path = generate_config_file(fusion_home, service, jar_home)
    config = load_config_from_file(config_file_path, service)
    os.remove(config_file_path)
    # you're assuming here that if zk is external then solr is also external which may not be correct. You can have
    # external zk and internal solr resulting in only a fusion.zk.connect string so you have to account for them both
    # Sidenote: I honestly have no idea why the config is being generated with a solr.zk.connect string when it isn't
    # defined in the fusion.properties... something in that Jar being run to generate the config is off
    config["solr.namespace"] = parse_solr_namespace(config["solr.zk.connect"] or config["fusion.zk.connect"])
    return config
