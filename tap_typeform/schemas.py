import os
import re

import singer
from singer import utils

class IDS(object):
    LANDINGS = 'landings'
    ANSWERS = 'answers'
    QUESTIONS = 'questions'

STATIC_SCHEMA_STREAM_IDS = [
    IDS.LANDINGS,
    IDS.ANSWERS,
    IDS.QUESTIONS
]

PK_FIELDS = {
    IDS.LANDINGS: ['landing_id'],
    IDS.ANSWERS: ['landing_id', 'question_id'],
    IDS.QUESTIONS: ['form_id', 'question_id']
}

REPLICATION_METHOD= {
    IDS.LANDINGS: ['INCREMENTAL'],
    IDS.ANSWERS: ['INCREMENTAL'],
    IDS.QUESTIONS: ['FULL_TABLE']
}
REPLICATION_KEY= {
    IDS.LANDINGS: ['submitted_at'],
    IDS.ANSWERS: ['submitted_at'],
    IDS.QUESTIONS: []
}

def normalize_fieldname(fieldname):
    fieldname = fieldname.lower()
    fieldname = re.sub(r'[\s\-]', '_', fieldname)
    return re.sub(r'[^a-z0-9_]', '', fieldname)


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def load_schema(tap_stream_id):
    path = 'schema/{}.json'.format(tap_stream_id)
    #print("schema path=",path)
    return utils.load_json(get_abs_path(path))

def load_and_write_schema(tap_stream_id):
    schema = load_schema(tap_stream_id)
    singer.write_schema(tap_stream_id, schema, PK_FIELDS[tap_stream_id])
