import time
import datetime
import json

import pendulum
import singer
from singer.bookmarks import write_bookmark, reset_stream
from ratelimit import limits, sleep_and_retry, RateLimitException
from backoff import on_exception, expo, constant
from tap_typeform import schemas
from tap_typeform.client import MetricsRateLimitException

LOGGER = singer.get_logger()

MAX_METRIC_JOB_TIME = 1800
RESPONSE_PAGE_SIZE = 1000
METRIC_JOB_POLL_SLEEP = 1
FORM_STREAMS = ['landings', 'answers']  # streams that get sync'd in sync_forms

DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def count(tap_stream_id, records):
    with singer.metrics.record_counter(tap_stream_id) as counter:
        counter.increment(len(records))


def write_records(atx, tap_stream_id, records):
    extraction_time = singer.utils.now()
    catalog_entry = atx.get_catalog_entry(tap_stream_id)
    stream_metadata = singer.metadata.to_map(catalog_entry.metadata)
    stream_schema = catalog_entry.schema.to_dict()
    with singer.Transformer() as transformer:
        for rec in records:
            rec = transformer.transform(rec, stream_schema, stream_metadata)
            singer.write_record(tap_stream_id, rec, time_extracted=extraction_time)
        atx.counts[tap_stream_id] += len(records)
    count(tap_stream_id, records)


def get_date_and_integer_fields(stream):
    date_fields = []
    integer_fields = []
    for prop, json_schema in stream.schema.properties.items():
        _type = json_schema.type
        if isinstance(_type, list) and 'integer' in _type or \
                _type == 'integer':
            integer_fields.append(prop)
        elif json_schema.format == 'date-time':
            date_fields.append(prop)
    return date_fields, integer_fields


def base_transform(date_fields, integer_fields, obj):
    new_obj = {}
    for field, value in obj.items():
        if value == '':
            value = None
        elif field in integer_fields and value is not None:
            value = int(value)
        elif field in date_fields and value is not None:
            value = pendulum.parse(value).isoformat()
        new_obj[field] = value
    return new_obj


def select_fields(mdata, obj):
    new_obj = {}
    for key, value in obj.items():
        field_metadata = mdata.get(('properties', key))
        if field_metadata and \
                (field_metadata.get('selected') is True or \
                 field_metadata.get('inclusion') == 'automatic'):
            new_obj[key] = value
    return new_obj


@on_exception(constant, MetricsRateLimitException, max_tries=5, interval=60)
@on_exception(expo, RateLimitException, max_tries=5)
@sleep_and_retry
@limits(calls=1, period=6)  # 5 seconds needed to be padded by 1 second to work
def get_form_definition(atx, form_id):
    return atx.client.get(form_id)


@on_exception(constant, MetricsRateLimitException, max_tries=5, interval=60)
@on_exception(expo, RateLimitException, max_tries=5)
@sleep_and_retry
@limits(calls=1, period=6)  # 5 seconds needed to be padded by 1 second to work
def get_form(atx, form_id, start_date, end_date, token_value_last_response):
    LOGGER.info('Forms query - form: {} start_date: {} end_date: {} '.format(
        form_id,
        start_date,
        end_date))
    # the api limits responses to a max of 1000 per call
    # the api doesn't have a means of paging through responses if the number is greater than 1000,
    # so since the order of data retrieved is by submitted_at we have
    # to take the last submitted_at date and use it to cycle through

    if token_value_last_response is None:
        return atx.client.get(form_id, params={'since': start_date, 'until': end_date, 'page_size': RESPONSE_PAGE_SIZE,
                                               'sort': 'submitted_at,asc'})
    else:
        return atx.client.get(form_id, params={'since': start_date, 'until': end_date, 'page_size': RESPONSE_PAGE_SIZE,
                                               'after': token_value_last_response})


def sync_form_definition(atx, form_id):
    with singer.metrics.job_timer('form definition ' + form_id):
        start = time.monotonic()
        while True:
            if (time.monotonic() - start) >= MAX_METRIC_JOB_TIME:
                raise Exception('Metric job timeout ({} secs)'.format(
                    MAX_METRIC_JOB_TIME))
            response = get_form_definition(atx, form_id)
            data = response.get('fields', [])
            if data != '':
                break
            else:
                time.sleep(METRIC_JOB_POLL_SLEEP)

    definition_data_rows = []

    # we only care about a few fields in the form definition
    # just those that give an analyst a reference to the submissions
    for row in data:
        definition_data_rows.append({
            "form_id": form_id,
            "question_id": row['id'],
            "title": row['title'],
            "ref": row['ref']
        })

    write_records(atx, 'questions', definition_data_rows)


def sync_form_data(atx, form_id, start_date, end_date, token_value_last_response, stream_id):
    with singer.metrics.job_timer('form ' + form_id):
        start = time.monotonic()
        # we've really moved this functionality to the request in the http script
        # so we don't expect that this will actually have to run mult times
        while True:
            if (time.monotonic() - start) >= MAX_METRIC_JOB_TIME:
                raise Exception('Metric job timeout ({} secs)'.format(
                    MAX_METRIC_JOB_TIME))
            response = get_form(atx, form_id, start_date, end_date, token_value_last_response)
            data = response['items']
            if data != '':
                break
            else:
                time.sleep(METRIC_JOB_POLL_SLEEP)

    landings_data_rows = []
    answers_data_rows = []

    max_submitted_dt = start_date

    for row in data:
        if 'hidden' not in row:
            hidden = ''
        else:
            hidden = json.dumps(row['hidden'])

        max_submitted_dt = row['submitted_at']
        token_value_last_response = row['token']

        if stream_id == 'landings':
            # the schema here reflects what we saw through testing
            # the typeform documentation is subtly inaccurate
            landings_data_rows.append({
                "landing_id": row['landing_id'],
                "token": row['token'],
                "form_id": form_id,
                "landed_at": row['landed_at'],
                "submitted_at": row['submitted_at'],
                "user_agent": row['metadata']['user_agent'],
                "platform": row['metadata']['platform'],
                "referer": row['metadata']['referer'],
                "network_id": row['metadata']['network_id'],
                "browser": row['metadata']['browser'],
                "hidden": hidden
            })

        if stream_id == 'answers':
            if row['answers'] is not None:
                for answer in row['answers']:
                    data_type = answer.get('type')

                    if data_type in ['choice', 'choices', 'payment']:
                        answer_value = json.dumps(answer.get(data_type))
                    elif data_type in ['number', 'boolean']:
                        answer_value = str(answer.get(data_type))
                    else:
                        answer_value = answer.get(data_type)

                    answers_data_rows.append({
                        "landing_id": row.get('landing_id'),
                        "question_id": answer.get('field', {}).get('id'),
                        "type": answer.get('field', {}).get('type'),
                        "form_id": form_id,
                        "ref": answer.get('field', {}).get('ref'),
                        "data_type": data_type,
                        "answer": answer_value,
                        "submitted_at": row.get('submitted_at')
                    })


    if stream_id == 'landings':
        schemas.load_and_write_schema('landings')
        write_records(atx, "landings", landings_data_rows)

    if stream_id == 'answers':
        schemas.load_and_write_schema('answers')
        write_records(atx, "answers", answers_data_rows)

    return [response['total_items'], max_submitted_dt, token_value_last_response]


def write_forms_state(atx, form_id, date_to_resume, token_value_last_response):
    # write_bookmark(atx.state, form, 'date_to_resume', date_to_resume.to_datetime_string())
    write_bookmark(atx.state, form_id, 'date_to_resume', date_to_resume)
    if token_value_last_response is not None:
        write_bookmark(atx.state, form_id, 'last_synchronised_response_token', token_value_last_response)
    atx.write_state()


def getForms(atx, callback, **kwargs):
    if 'page' not in kwargs:
        kwargs['page'] = 1
        kwargs['page_size'] = 200
    type_forms = atx.client.get('forms', params=kwargs)['items']
    callback(atx, type_forms)
    if len(type_forms) == kwargs['page_size']:
        kwargs['page'] += 1
        getForms(atx, callback, **kwargs)


def syncTypeForms(atx, typeForms):
    schemas.load_and_write_schema('forms')
    write_records(atx, "forms", typeForms)


def sync(atx):
    for stream in atx.catalog.streams:
        if stream.tap_stream_id == "forms":
            getForms(atx, syncTypeForms)
        else:
            getForms(atx, get_forms_data)


def get_forms_data(atx, forms):
    for form in forms:
        for stream in atx.catalog.streams:

            form_id = form["id"]

            bookmark = atx.state.get('bookmarks', {}).get(form_id, {})

            LOGGER.info('form: {} '.format(form_id))

            # pull back the form question details
            if stream.tap_stream_id == 'questions':
                schemas.load_and_write_schema(stream.tap_stream_id)
                sync_form_definition(atx, form_id)

            # start_date is defaulted in the config file 2018-01-01
            # if there's no default date and it gets set to now, then start_date will have to be
            #   set to the prior business day/hour before we can use it.

            now = datetime.datetime.now()
            today = now.replace(hour=0, minute=0, second=0, microsecond=0).strftime(DATE_FORMAT)
            start_date = datetime.datetime.strptime(atx.config.get('start_date', today), DATE_FORMAT).replace(hour=0,
                                                                                                              minute=0,
                                                                                                              second=0,
                                                                                                              microsecond=0).strftime(
                DATE_FORMAT)
            end_date = today
            LOGGER.info('start_date: {} '.format(start_date))
            LOGGER.info('end_date: {} '.format(end_date))

            # if the state file has a date_to_resume, we use it as it is.
            # if it doesn't exist, we overwrite by start date
            last_date = bookmark.get('date_to_resume', start_date)
            LOGGER.info('last_date: {} '.format(last_date))

            token_value_last_response = bookmark.get('last_synchronised_response_token',
                                                     None)  # since it is the first call for the current form_id
            [responses, max_submitted_at, token_value_last_response] = sync_form_data(atx, form_id, last_date, end_date,
                                                                                      token_value_last_response,
                                                                                      stream.tap_stream_id)
            # if the max responses were returned, we have to make the call again
            # going to increment the max_submitted_at by 1 second so we don't get dupes,
            # but this also might cause some minor loss of data.
            # there's no ideal scenario here since the API has no other way than using
            # time ranges to step through data.

            while responses > RESPONSE_PAGE_SIZE:
                interim_next_date = max_submitted_at  # + datetime.timedelta(seconds=1) removed the =1 second because we are using the before token filter
                write_forms_state(atx, form_id, interim_next_date, token_value_last_response)
                [responses, max_submitted_at, token_value_last_response] = sync_form_data(atx, form_id, interim_next_date,
                                                                                          end_date,
                                                                                          token_value_last_response,
                                                                                          stream.tap_stream_id)

            # if the prior sync is successful it will write the date_to_resume bookmark
            write_forms_state(atx, form_id, max_submitted_at, token_value_last_response)
            # token_value_last_response = None
