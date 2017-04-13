import re
import os
import logging
import sys
import platform

logger = logging.getLogger(__name__)
class VariablesHelper:
  def __init__(self):
    pass

  FUSION_HOME = None
  FUSION_OLD_HOME = None
  FUSION_ZK = "FUSION_ZK"

  @staticmethod
  def ensure_fusion_home():

    VariablesHelper.FUSION_HOME = os.environ.get("FUSION_HOME")
    if VariablesHelper.FUSION_HOME is not None and VariablesHelper.is_windows():
      VariablesHelper.FUSION_HOME = VariablesHelper.FUSION_HOME.lstrip("\"").rstrip("\"")
    if VariablesHelper.FUSION_HOME is None:
      return False
    return True

  @staticmethod
  def ensure_old_fusion_home():

    VariablesHelper.FUSION_OLD_HOME = os.environ.get("FUSION_OLD_HOME")
    if VariablesHelper.FUSION_OLD_HOME is not None and VariablesHelper.is_windows():
      VariablesHelper.FUSION_OLD_HOME = VariablesHelper.FUSION_OLD_HOME.lstrip("\"").rstrip("\"")

    if VariablesHelper.FUSION_OLD_HOME is None:
      return False
    return True


  @staticmethod
  def get_variable(path, variable):
    with open(path) as config_data_file:
      data = config_data_file.read()
      result = re.search("{}=.+".format(variable), data)

      if result:
        return result.group(0).replace("{}=".format(variable), "").strip()
      else:
        return None

  @staticmethod
  def get_fusion_variable(variable):
    if (VariablesHelper.is_windows()):
      path = os.path.join(VariablesHelper.FUSION_HOME, "conf", "config.cmd")
    else:
      path = os.path.join(VariablesHelper.FUSION_HOME, "conf", "config.sh")
    return VariablesHelper.get_variable(path, variable)

  @staticmethod
  def get_fusion_version():
    path = os.path.join(VariablesHelper.FUSION_HOME, "fusion.build")
    return VariablesHelper.cleanup_fusion_version(VariablesHelper.get_variable(path, "fusion.version"))

  @staticmethod
  def get_old_fusion_version():
    path = os.path.join(VariablesHelper.FUSION_OLD_HOME, "fusion.build")
    return VariablesHelper.cleanup_fusion_version(VariablesHelper.get_variable(path, "fusion.version"))

  @staticmethod
  def get_fusion_zookeeper_host():
    zk_ensemble = VariablesHelper.get_fusion_variable(VariablesHelper.FUSION_ZK)
    result = zk_ensemble.split("/")
    return result[0]

  @staticmethod
  def get_fusion_zookeeper_node():
    zk_ensemble = VariablesHelper.get_fusion_variable(VariablesHelper.FUSION_ZK)
    result = zk_ensemble.split("/")
    return "" if len(result) <= 1 else "/{}".format(result[1])

  @staticmethod
  def cleanup_fusion_version(version):
    # Remove SNAPSHOT or RC numbers from the version if present
    m = re.search('\d+\.\d+(\.\d+)?', version)
    if m is not None:
      return m.group(0)
    else:
      logger.critical("Could not extract a valid version number from '{}'".format(version))
      sys.exit(1)
     
  @staticmethod
  def is_windows():
    system_name = platform.system()
    return "Windows" in system_name or "Microsoft" in system_name
