#!/usr/bin/env python

import logging

class ClassLoader:
  classes_cache = {}

  def get_class(self, classname):

    logging.info("Loading class: {}".format(classname))

    if self.classes_cache.has_key(classname):
      return self.classes_cache.get(classname)

    components = classname.split('.')
    full_module = ".".join(components[:-1])
    module = __import__(full_module)

    for component in components[1:]:
      try:
        module = getattr(module, component)
      except:
        logging.error("Component not found: %s", component)
        return None

    self.classes_cache[classname] = module

    return module

  def get_instance(self, classname):
    class_ = self.get_class(classname)

    if class_ is None:
      return None

    return class_()
