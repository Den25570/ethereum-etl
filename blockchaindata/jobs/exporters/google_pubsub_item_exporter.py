import json
import logging

from google.cloud import pubsub_v1
from timeout_decorator import timeout_decorator


class GooglePubSubItemExporter:

    def __init__(self, item_type_to_topic_mapping, message_attributes=(),
            batch_max_bytes=1024 * 5, batch_max_latency=1, batch_max_messages=1000,
            enable_message_ordering=False):
        self.item_type_to_topic_mapping = item_type_to_topic_mapping

        self.batch_max_bytes = batch_max_bytes
        self.batch_max_latency = batch_max_latency
        self.batch_max_messages = batch_max_messages

        self.enable_message_ordering = enable_message_ordering

        self.publisher = self.create_publisher()

        self.message_attributes = message_attributes

    def open(self):
        pass

    def export_items(self, items):
        try:
            self._export_items_with_timeout(items)
        except timeout_decorator.TimeoutError as e:
            logging.info('Recreating Pub/Sub publisher.')
            self.publisher = self.create_publisher()
            raise e

    @timeout_decorator.timeout(300)
    def _export_items_with_timeout(self, items):
        futures = []
        for item in items:
            message_future = self.export_item(item)
            futures.append(message_future)

        for future in futures:
            # result() blocks until the message is published.
            future.result()

    def export_item(self, item):
        item_type = item.get('type')
        if item_type is not None and item_type in self.item_type_to_topic_mapping:
            topic_path = self.item_type_to_topic_mapping.get(item_type)
            data = json.dumps(item).encode('utf-8')

            ordering_key = 'all' if self.enable_message_ordering else ''
            message_future = self.publisher.publish(topic_path, data=data, ordering_key=ordering_key, **self.get_message_attributes(item))
            return message_future
        else:
            logging.warning('Topic for item type "{}" is not configured.'.format(item_type))

    def get_message_attributes(self, item):
        attributes = {}

        for attr_name in self.message_attributes:
            if item.get(attr_name) is not None:
                attributes[attr_name] = str(item.get(attr_name))

        return attributes

    def create_publisher(self):
        batch_settings = pubsub_v1.types.BatchSettings(
            max_bytes=self.batch_max_bytes,
            max_latency=self.batch_max_latency,
            max_messages=self.batch_max_messages,
        )

        publisher_options = pubsub_v1.types.PublisherOptions(enable_message_ordering=self.enable_message_ordering)
        return pubsub_v1.PublisherClient(batch_settings=batch_settings, publisher_options=publisher_options)

    def close(self):
        pass
