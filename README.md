# celery-broker-prometheus-exporter

A simple Prometheus-compatible metrics exporter for Celery brokers. It exports only one metric: `celery_queue_length`.


## Disclaimer

Actually tested only with redis, because there is [RabbitMQ exporter](https://github.com/kbudde/rabbitmq_exporter).
But it should work too.


## Thanks

This code is based on code from two projects:

* [flower](https://github.com/mher/flower/blob/master/flower/utils/broker.py)
* [celery-prometheus-exporter](https://github.com/zerok/celery-prometheus-exporter)
