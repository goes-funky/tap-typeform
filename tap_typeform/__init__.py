#!/usr/bin/env python3

import os
import sys
import json

import singer
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry, Schema

import streams
from tap_typeform.context import Context
import schemas


REQUIRED_CONFIG_KEYS = ["token"]

LOGGER = singer.get_logger()


# def check_authorization(atx):
#    atx.client.get('/settings')


# Some taps do discovery dynamically where the catalog is read in from a
#  call to the api but with the typeform structure, we won't do that here
#  because it's always the same so we just pull it from file we never use
#  atx in here since the schema is from file but we would use it if we
#  pulled schema from the API def discover(atx):
def discover(atx):
    catalog = Catalog([])
    forms_response = get_form_list(atx)
    form_list = forms_response['items']
    for form in form_list:
        for schema_stream_id in schemas.STATIC_SCHEMA_STREAM_IDS:
            tap_stream_id_schema = schema_stream_id
            tap_stream_id = form['id'] + "_" + schema_stream_id
            # print("tap stream id=",tap_stream_id)

            schema = Schema.from_dict(schemas.load_schema(tap_stream_id_schema))

            meta = metadata.new()
            meta = metadata.write(meta, (), 'table-key-properties', schemas.PK_FIELDS[tap_stream_id_schema])
            meta = metadata.write(meta, (), 'selected', True)
            # end

            for field_name in schema.properties.keys():
                # print("field name=",field_name)
                if field_name in schemas.PK_FIELDS[tap_stream_id_schema]:
                    inclusion = 'automatic'
                else:
                    inclusion = 'available'
                meta = metadata.write(meta, ("properties", field_name), 'inclusion', inclusion)

            catalog.streams.append(CatalogEntry(
                stream=tap_stream_id,
                tap_stream_id=tap_stream_id,
                replication_method="INCREMENTAL",
                key_properties=schemas.PK_FIELDS[tap_stream_id_schema],
                schema=schema,
                metadata=metadata.to_list(meta)
            ))

    return catalog


def get_form_list(atx):
    LOGGER.info('Get Form List ...')
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
    # write schemas for selected streams\
    for stream in atx.catalog.streams:
        if stream.tap_stream_id in atx.selected_stream_ids:
            # schemas.load_and_write_schema(stream.tap_stream_id)
            schemas.load_and_write_schema(stream.tap_stream_id)

    # since there is only one set of schemas for all forms, they will always be selected
    streams.sync_forms(atx)

    LOGGER.info('--------------------')
    for stream_name, stream_count in atx.counts.items():
        LOGGER.info('%s: %d', stream_name, stream_count)
    LOGGER.info('--------------------')


@utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    atx = Context(args.config, args.state)
    if args.discover:
        catalog = discover(atx)
        catalog.dump()
    else:
        atx.catalog = Catalog.from_dict(args.properties) \
            if args.properties else discover(atx)
        sync(atx)


if __name__ == "__main__":
    main()
