import uuid

from src.utils.constants import *
import json
import logging

logger = logging.getLogger(__name__)

class SplitterMigrator():
  DEFAULT_CSV_PARSER = {
    "type": "csv",
    "charset": "detect",
    "ignoreBOM": False,
    "errorHandling": "mark",
    "autoDetect": True,
    "delimiter": ",",
    "comment": "#",
    "hasHeaders": False,
    "headers": [],
    "fillValue": "<FILL>",
    "trimWhitespace": True,
    "skipEmptyLines": True,
    "includeRowNumber": True,
    "commentHandling": "ignore",
    "maxRowLength": 10485760,
    "maxNumColumns": 1000,
    "maxColumnChars": 10485760,
    "columnHandling": "default",
    "enabled": True,
  }

  DEFAULT_ARCHIVE_PARSER = {
    "type": "archive",
    "alwaysDetect": True,
    "errorHandling": "mark",
    "enabled": True,
    "inheritMediaTypes": True
  }

  DEFAULT_JSON_PARSER = {
    "type": "json",
    "splitArrays": True,
    "includePath": False,
    "mappings": [],
    "errorHandling": "mark",
    "enabled": True,
    "mediaTypes": [],
    "pathPatterns": [],
    "inheritMediaTypes": True
  }

  DEFAULT_TEXT_PARSER = {
    "type": "text",
    "charset": "detect",
    "ignoreBOM": False,
    "maxLength": 1048576,
    "maxLineLength": 1048576,
    "outputField": "body",
    "splitLines": False,
    "skipEmptyLines": False,
    "skipHeaderLines": 0,
    "trimWhitespace": False,
    "commentHandling": "include",
    "comment": "#",
    "commentField": "comment",
    "errorHandling": "mark",
    "enabled": True,
    "mediaTypes": [],
    "pathPatterns": [],
    "inheritMediaTypes": True
  }

  DEFAULT_TIKA_PARSER = {
    "type": "tika",
    "includeImages": False,
    "flattenCompound": True,
    "addFailedDocs": False,
    "addOriginalContent": False,
    "contentEncoding": "binary",
    "returnXml": False,
    "keepOriginalStructure": False,
    "extractHtmlLinks": False,
    "extractOtherLinks": False,
    "excludeContentTypes": [],
    "errorHandling": "mark",
    "enabled": True,
    "mediaTypes": [],
    "pathPatterns": [],
    "inheritMediaTypes": True
  }

  DEFAULT_FALLBACK_PARSER = {
    "type": "fallback",
    "metadataOnly": False,
    "maxBytesToKeep": 1048576,
    "errorHandling": "mark",
    "enabled": True,
    "mediaTypes": [],
    "pathPatterns": [],
    "inheritMediaTypes": True
  }

  CONNECTORS_SUPPORTED = [DROPBOX, FILE, GOOGLEDRIVE, SHAREPOINT, WEB, FTP, HDFS, SMB, AZURE, HADOOP, FILE_UPLOAD,
                          TWITTER_SEARCH, TWITTER_STREAM, JIVE, S3]

  def __init__(self, config, zk_client, old_zk_client):
    self.write_zk = zk_client
    self.read_zk = zk_client
    self.old_zk = old_zk_client
    self.zk_fusion_node = config["api.namespace"]

  def generate_id(self):
    return str(uuid.uuid4())

  def add_common_parsers(self, parsers):
    # we don't want to add Tika, text, JSON, etc... it's not backwards-compatible with any parsing they may have in pipelines.
    # we always want the fallback parser, though. That should set data in a _raw_content_ field to be parsed in pipelines.
    fallback = SplitterMigrator.DEFAULT_FALLBACK_PARSER.copy()
    fallback[ID] = self.generate_id()
    parsers.append(fallback)
      
  def create_archive_parser(self):
    archive_parser = SplitterMigrator.DEFAULT_ARCHIVE_PARSER.copy()
    archive_parser[ID] = self.generate_id()
    return archive_parser

  def migrate_anda_splitter(self, datasource):
    logger.info("Executing migrate_anda_splitter for DS {}".format(datasource["id"]))
    parsers = []
    splitter = dict(datasource[PROPERTIES].pop(SPLIT_CSV, {}))
    split_archives = bool(datasource[PROPERTIES].pop(SPLIT_ARCHIVES, False))

    if split_archives:
      archive_parser = self.create_archive_parser()
      parsers.append(archive_parser)

    if len(splitter) == 0:
      # so, there's no CSV splitting
      return parsers, self.set_parser_to_datasource(datasource, None)

    # we are doing csv splitting
    csv_parser = SplitterMigrator.DEFAULT_CSV_PARSER.copy()
    csv_parser[ID] = self.generate_id()
    csv_parser.update({CHARSET: splitter.get(CSV_CHARACTER_SET_OVERRIDE, DETECT)})
    csv_parser.update({DELIMITER: splitter.get(CSV_DELIMETER_OVERRIDE, COMMA)})
    csv_parser.update({COMMENT: splitter.get(CSV_COMMENT_OVERRIDE, NUMBER)})
    csv_parser.update({HAS_HEADER: bool(splitter.get(CSV_WITH_HEADER, False))})
    csv_parser.update({HEADERS: [splitter.get(CSV_ID_COLUMN, DEFAULT_EMPTY)]
    if bool(splitter.get(CSV_WITH_HEADER, False)) else []})
    parsers.append(csv_parser)
    return parsers, datasource

  def migrate_fs_splitter(self, datasource):
    logger.info("Executing migrate_fs_splitter for DS {}".format(datasource["id"]))
    splitter = datasource[PROPERTIES].pop(SPLITTER, {})

    if len(splitter) == 0:
      return [], self.set_parser_to_datasource(datasource, None)
    
    archive_parser = self.create_archive_parser()

    csv_parser = SplitterMigrator.DEFAULT_CSV_PARSER.copy()
    csv_parser[ID] = self.generate_id()
    csv_parser.update({DELIMITER: splitter.get(CSV_DELIMITER, COMMA)})
    csv_parser.update({COMMENT: splitter.get(COMMENT, NUMBER)})
    csv_parser.update({HAS_HEADER: bool(splitter.get(HEADER_LINE, False))})
    csv_parser.update({HEADERS: splitter.get(FIELD_NAMES, DEFAULT_EMPTY).split(COMMA)
    if bool(splitter.get(HEADER_LINE, False)) else []})
    return [archive_parser, csv_parser], datasource, True

  def set_parser_to_datasource(self, datasource, parser_id=DEFAULT):
    if parser_id is not None:
      datasource[PARSER_ID] = parser_id
    return datasource

  def migrate_splitter(self, splitter_function, datasource):
    parsers, datasource = splitter_function(datasource)

    if len(parsers) == 0 :
      # no parsing necessary, so nothing more to do
      return datasource

    ds_id = datasource[ID]

    self.add_common_parsers(parsers)
    parser_name = "parser_{}".format(ds_id)

    parser_conf = {
      ID: parser_name,
      "enableMediaTypeDetection": True,
      "maxParserDepth": 16,
      "parserStages": parsers
    }

    zk_node = "{}/parsers/{}".format(self.zk_fusion_node, parser_name)

    logger.info("Creating parser '{}' at path {}".format(parser_name, zk_node))
    
    if not self.write_zk.exists(zk_node):
      self.write_zk.create(zk_node, makepath=True)

    self.write_zk.set(zk_node, json.dumps(parser_conf, indent=2))
    datasource = self.set_parser_to_datasource(datasource, parser_name)

    return datasource

  def start(self):
    connectors_znode = "{}/connectors".format(self.zk_fusion_node)
    datasources_znode = "{}/datasources".format(connectors_znode)

    # no need to verify that connectors znode exist because it is used to just define datasources_znode
    if self.old_zk and self.old_zk.exists(datasources_znode):
      read_zk = old_zk
    elif not self.read_zk.exists(datasources_znode):
      logger.info("Connectors znode path {} does not exist. No migrations to perform".format(datasources_znode))
      return

    children = self.read_zk.get_children(datasources_znode)

    for child in children:
      datasource_node = "{}/{}".format(datasources_znode, child)
      value, zstat = self.read_zk.get(datasource_node)
      datasource = json.loads(value)
      connector = datasource[CONNECTOR]
      type = datasource[TYPE]

      if not type in SplitterMigrator.CONNECTORS_SUPPORTED:
        continue

      updated_datasource = {}

      if connector == LUCID_ANDA:
        updated_datasource = self.migrate_splitter(self.migrate_anda_splitter, datasource)
      elif connector == LUCID_FS:
        updated_datasource = self.migrate_splitter(self.migrate_fs_splitter, datasource)
      else:
        updated_datasource = self.set_parser_to_datasource(datasource, None)

      self.write_zk.set(datasource_node, json.dumps(updated_datasource, indent=2))
