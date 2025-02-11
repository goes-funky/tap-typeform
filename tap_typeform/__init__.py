#!/usr/bin/env python3

import os
import sys
import json

import singer
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry, Schema

from tap_typeform import schemas, streams
from tap_typeform.context import Context

REQUIRED_CONFIG_KEYS = ["token"]

LOGGER = singer.get_logger()


# def check_authorization(atx):
#    atx.client.get('/settings')


# Some taps do discovery dynamically where the catalog is read in from a
#  call to the api but with the typeform structure, we won't do that here
#  because it's always the same so we just pull it from file we never use
#  atx in here since the schema is from file but we would use it if we
#  pulled schema from the API def discover(atx):

def discover():
    catalog = Catalog([])
    for tap_stream_id in schemas.STATIC_SCHEMA_STREAM_IDS:
        schema = Schema.from_dict(schemas.load_schema(tap_stream_id))
        meta = metadata.new()
        meta = metadata.write(meta, (), 'table-key-properties', schemas.PK_FIELDS[tap_stream_id])
        replication_key = schemas.REPLICATION_KEY[tap_stream_id]
        meta = metadata.write(meta, (), 'valid-replication-keys', replication_key)

        for field_name in schema.properties.keys():
            if field_name in schemas.PK_FIELDS[tap_stream_id]:
                inclusion = 'automatic'
            else:
                inclusion = 'available'
            meta = metadata.write(meta, ("properties", field_name), 'inclusion', inclusion)

        catalog.streams.append(CatalogEntry(
            stream=tap_stream_id,
            tap_stream_id=tap_stream_id,
            replication_method=schemas.REPLICATION_METHOD[tap_stream_id],
            key_properties=schemas.PK_FIELDS[tap_stream_id],
            schema=schema,
            metadata=metadata.to_list(meta),
            replication_key=replication_key
        ))
    return catalog


def get_form_list(atx):
    return atx.client.get('forms')


def get_abs_path(path):
    s = path
    full_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), path)
    print(full_path)

    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


# this is already defined in schemas.py though w/o dependencies.  do we keep this for the sync?
def load_schema(tap_stream_id):
    path = "schemas/{}.json".format(tap_stream_id)
    schema = utils.load_json(get_abs_path(path))
    dependencies = schema.pop("tap_schema_dependencies", [])
    refs = {}
    for sub_stream_id in dependencies:
        refs[sub_stream_id] = load_schema(sub_stream_id)
    if refs:
        singer.resolve_schema_references(schema, refs)
    return schema


def sync(atx):

    streams.sync(atx)

    LOGGER.info('--------------------')
    for stream_name, stream_count in atx.counts.items():
        LOGGER.info('%s: %d', stream_name, stream_count)
    LOGGER.info('--------------------')


@utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    atx = Context(args.config, args.state)
    if args.discover:
        catalog = discover()
        catalog.dump()
    else:
        atx.catalog = Catalog.from_dict(args.properties) \
            if args.properties else discover()
        sync(atx)


if __name__ == "__main__":
    main()
