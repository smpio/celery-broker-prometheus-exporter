import logging
import numbers
from urllib.parse import urlparse, urljoin, quote, unquote

import requests
try:
    import redis
except ImportError:
    redis = None


log = logging.getLogger(__name__)


class Broker(object):
    def __new__(cls, broker_url, **kwargs):
        scheme = urlparse(broker_url).scheme
        if scheme == 'amqp':
            return RabbitMQ(broker_url, **kwargs)
        elif scheme == 'redis':
            return Redis(broker_url, **kwargs)
        else:
            raise NotImplementedError


class BrokerBase(object):
    def __init__(self, broker_url, *args, **kwargs):
        purl = urlparse(broker_url)
        self.host = purl.hostname
        self.port = purl.port
        self.vhost = purl.path[1:]

        username = purl.username
        password = purl.password

        self.username = unquote(username) if username else username
        self.password = unquote(password) if password else password

    def queues(self, names):
        raise NotImplementedError


class RabbitMQ(BrokerBase):
    def __init__(self, broker_url, **kwargs):
        super(RabbitMQ, self).__init__(broker_url)
        self.host = self.host or 'localhost'
        self.port = 15672
        self.vhost = quote(self.vhost, '') or '/'
        self.username = self.username or 'guest'
        self.password = self.password or 'guest'

        http_api = kwargs.get('http_api')
        if not http_api:
            http_api = "http://{}:{}@{}:{}/api/{}".format(self.username, self.password,
                                                          self.host, self.port, self.vhost)

        self.http_api = http_api

    def queues(self, names):
        url = urljoin(self.http_api, 'queues/' + self.vhost)
        api_url = urlparse(self.http_api)
        username = unquote(api_url.username or '') or self.username
        password = unquote(api_url.password or '') or self.password

        response = requests.get(url, auth=(username, password))
        if response.status_code != 200:
            log.error("RabbitMQ management API call failed: %s", response)

        info = response.json()
        return [x for x in info if x['name'] in names]


class Redis(BrokerBase):
    SEP = '\x06\x16'
    DEFAULT_PRIORITY_STEPS = [0, 3, 6, 9]

    def __init__(self, broker_url, **kwargs):
        super(Redis, self).__init__(broker_url)
        self.host = self.host or 'localhost'
        self.port = self.port or 6379
        self.vhost = self._prepare_virtual_host(self.vhost)

        if not redis:
            raise ImportError('redis library is required')

        self.redis = redis.Redis(host=self.host, port=self.port,
                                 db=self.vhost, password=self.password)

        broker_options = kwargs.get('broker_options')

        if broker_options and 'priority_steps' in broker_options:
            self.priority_steps = broker_options['priority_steps']
        else:
            self.priority_steps = self.DEFAULT_PRIORITY_STEPS

    def _q_for_pri(self, queue, pri):
        if pri not in self.priority_steps:
            raise ValueError('Priority not in priority steps')
        return ''.join((queue, self.SEP, str(pri)) if pri else (queue, '', ''))

    def queues(self, names):
        queue_stats = []
        for name in names:
            priority_names = [self._q_for_pri(name, pri) for pri in self.priority_steps]
            queue_stats.append({
                'name': name,
                'messages': sum([self.redis.llen(x) for x in priority_names])
            })
        return queue_stats

    def _prepare_virtual_host(self, vhost):
        if not isinstance(vhost, numbers.Integral):
            if not vhost or vhost == '/':
                vhost = 0
            elif vhost.startswith('/'):
                vhost = vhost[1:]
            try:
                vhost = int(vhost)
            except ValueError:
                raise ValueError(
                    'Database is int between 0 and limit - 1, not {0}'.format(
                        vhost,
                    ))
        return vhost
