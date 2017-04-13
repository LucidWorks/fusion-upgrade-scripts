#!/usr/bin/env python

import os
import json
import re
import logging

class ResourceManager:
  logger = logging.getLogger(__name__)
  def __init__(self):
    pass

  @staticmethod
  def get_resource(filename):
    source_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(source_dir, "../resources/{}".
                        format(filename))
    logger.info("Loading file from path {}".format(path))
    with open(path) as data_file:
      return json.load(data_file)
