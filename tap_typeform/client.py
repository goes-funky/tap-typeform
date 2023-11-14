import requests
import backoff
import singer

LOGGER = singer.get_logger()

class RateLimitException(Exception):
    pass

class MetricsRateLimitException(Exception):
    pass

class Client(object):

    # BASE_URL = 'https://api.typeform.com/forms/FORM_ID/responses'

    BASE_URL = 'https://api.typeform.com/forms'

    def __init__(self, config):
        self.token = 'Bearer ' + config.get('token')
        self.metric = config.get('metric')
        self.session = requests.Session()

    def url(self, form_id):
        #return self.BASE_URL.replace("FORM_ID", form_id)

        if form_id == 'forms': #return form list
            return self.BASE_URL  # return all form list
        else:  #return all responses for current form_id
            return self.BASE_URL + "/" + form_id + "/responses"

    @backoff.on_exception(backoff.expo,
                          RateLimitException,
                          max_tries=10,
                          factor=2)
    def request(self, method, form_id, **kwargs):
        # note that typeform response api doesn't return limit headers

        if 'headers' not in kwargs:
            kwargs['headers'] = {}
        if self.token:
            kwargs['headers']['Authorization'] = self.token

        # if we're just pulling the form definition, strip the rest of the url
        if 'params' not in kwargs:
            response = requests.request(method, self.url(form_id).replace('/responses', ''), **kwargs)
        else:
            response = requests.request(method, self.url(form_id), **kwargs)

        if response.status_code in [429, 502, 503]:
            raise RateLimitException()
        if response.status_code == 423:
            raise MetricsRateLimitException()
        try:
            response.raise_for_status()
        except:
            LOGGER.error('{} - {}'.format(response.status_code, response.text))
            raise
        if 'total_items' in response.json():
            LOGGER.info('raw data items= {}'.format(response.json()['total_items']))
        return response.json()

    def get(self, form_id, **kwargs):
        return self.request('get', form_id, **kwargs)
