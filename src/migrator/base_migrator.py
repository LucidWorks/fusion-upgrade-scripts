from src.utils.constants import *

import logging

class BaseMigrator:
  logger = logging.getLogger(__name__)
  def delete_properties(self, data_source, properties):
    for property in properties:
      try:
        if PROPERTIES in data_source:
          if property in data_source[PROPERTIES]:
            del data_source[PROPERTIES][property]
      except:
        logger.warn("Could not delete property: %s", property)
        continue
    return data_source
