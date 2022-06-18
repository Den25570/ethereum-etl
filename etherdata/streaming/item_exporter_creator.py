

from blockchaindata.jobs.exporters.console_item_exporter import ConsoleItemExporter
from blockchaindata.jobs.exporters.multi_item_exporter import MultiItemExporter

def create_item_exporters(outputs, **kwargs):
    split_outputs = [output.strip() for output in outputs.split(',')] if outputs else ['console']

    item_exporters = [create_item_exporter(output, kwargs) for output in split_outputs]
    return MultiItemExporter(item_exporters)



def create_item_exporter(output, **kwargs):
    item_exporter_type = determine_item_exporter_type(output)
    if item_exporter_type == ItemExporterType.PUBSUB:
        from blockchaindata.jobs.exporters.google_pubsub_item_exporter import GooglePubSubItemExporter
        enable_message_ordering = 'sorted' in output or 'ordered' in output
        item_exporter = GooglePubSubItemExporter(
            item_type_to_topic_mapping={
                'block': output + '.blocks',
                'transaction': output + '.transactions',
                'log': output + '.logs',
                'token_transfer': output + '.token_transfers',
                'trace': output + '.traces',
                'contract': output + '.contracts',
                'token': output + '.tokens',
            },
            message_attributes=('item_id', 'item_timestamp'),
            batch_max_bytes=1024 * 1024 * 5,
            batch_max_latency=2,
            batch_max_messages=1000,
            enable_message_ordering=enable_message_ordering)
    elif item_exporter_type == ItemExporterType.POSTGRES:
        from blockchaindata.jobs.exporters.postgres_item_exporter import PostgresItemExporter
        from blockchaindata.streaming.postgres_utils import create_insert_statement_for_table
        from blockchaindata.jobs.exporters.converters.unix_timestamp_item_converter import UnixTimestampItemConverter
        from blockchaindata.jobs.exporters.converters.int_to_decimal_item_converter import IntToDecimalItemConverter
        from blockchaindata.jobs.exporters.converters.list_field_item_converter import ListFieldItemConverter
        from etherdata.streaming.postgres_tables import BLOCKS, TRANSACTIONS, LOGS, TOKEN_TRANSFERS, TRACES, TOKENS, \
            CONTRACTS

        item_exporter = PostgresItemExporter(
            output, item_type_to_insert_stmt_mapping={
                'block': create_insert_statement_for_table(BLOCKS),
                'transaction': create_insert_statement_for_table(TRANSACTIONS),
                'log': create_insert_statement_for_table(LOGS),
                'token_transfer': create_insert_statement_for_table(TOKEN_TRANSFERS),
                'trace': create_insert_statement_for_table(TRACES),
                'token': create_insert_statement_for_table(TOKENS),
                'contract': create_insert_statement_for_table(CONTRACTS),
            },
            converters=[UnixTimestampItemConverter(), IntToDecimalItemConverter(),
                        ListFieldItemConverter('topics', 'topic', fill=4)])
    elif item_exporter_type == ItemExporterType.GCS:
        from blockchaindata.jobs.exporters.gcs_item_exporter import GcsItemExporter
        bucket, path = get_bucket_and_path_from_gcs_output(output)
        item_exporter = GcsItemExporter(bucket=bucket, path=path)
    elif item_exporter_type == ItemExporterType.CONSOLE:
        item_exporter = ConsoleItemExporter()
    elif item_exporter_type == ItemExporterType.KAFKA:
        from blockchaindata.jobs.exporters.kafka_exporter import KafkaItemExporter
        item_exporter = KafkaItemExporter(output, item_type_to_topic_mapping={
            'block': 'blocks',
            'transaction': 'transactions',
            'log': 'logs',
            'token_transfer': 'token_transfers',
            'trace': 'traces',
            'contract': 'contracts',
            'token': 'tokens',
        })
    elif item_exporter_type == ItemExporterType.S3:
        from blockchaindata.jobs.exporters.converters.unix_timestamp_item_converter import UnixTimestampItemConverter
        from blockchaindata.jobs.exporters.converters.int_to_decimal_item_converter import IntToDecimalItemConverter
        from blockchaindata.jobs.exporters.converters.list_field_item_converter import ListFieldItemConverter
        from blockchaindata.jobs.exporters.s3_item_exporter import S3ItemExporter
        item_exporter = S3ItemExporter(bucket=output.split('//')[-1], converters=[UnixTimestampItemConverter()], filename_mapping={
            'block': 'blocks.csv',
            'transaction': 'transactions.csv',
            'log': 'logs.json',
            'token_transfer': 'token_transfers.csv',
            'trace': 'traces.csv',
            'contract':  'contracts.json',
            'token': 'tokens.json'})
    elif item_exporter_type == ItemExporterType.PULSAR:
        from blockchaindata.jobs.exporters.pulsar_exporter import PulsarItemExporter
        pulsar_output, pulsar_topic, pulsar_token = output.split('|')
        item_exporter = PulsarItemExporter(pulsar_output, pulsar_token, item_type_to_topic_mapping={
            'block': pulsar_topic + '/blocks',
            'transaction': pulsar_topic + '/transactions',
            'log': pulsar_topic + '/logs',
            'token_transfer': pulsar_topic + '/token-transfers',
            'trace': pulsar_topic + '/traces',
            'contract': pulsar_topic + '/contracts',
            'token': pulsar_topic + '/tokens',
        })
    else:
        raise ValueError('Unable to determine item exporter type for output ' + output)

    return item_exporter


def get_bucket_and_path_from_gcs_output(output):
    output = output.replace('gs://', '')
    bucket_and_path = output.split('/', 1)
    bucket = bucket_and_path[0]
    if len(bucket_and_path) > 1:
        path = bucket_and_path[1]
    else:
        path = ''
    return bucket, path


def determine_item_exporter_type(output):
    if output is not None and output.startswith('projects'):
        return ItemExporterType.PUBSUB
    if output is not None and output.startswith('kafka'):
        return ItemExporterType.KAFKA
    elif output is not None and output.startswith('postgresql'):
        return ItemExporterType.POSTGRES
    elif output is not None and output.startswith('pulsar'):
        return ItemExporterType.PULSAR
    elif output is not None and output.startswith('gs://'):
        return ItemExporterType.GCS
    elif output is not None and output.startswith('s3'):
        return ItemExporterType.S3
    elif output is None or output == 'console':
        return ItemExporterType.CONSOLE
    else:
        return ItemExporterType.UNKNOWN


class ItemExporterType:
    PUBSUB = 'pubsub'
    POSTGRES = 'postgres'
    GCS = 'gcs'
    CONSOLE = 'console'
    KAFKA = 'kafka'
    S3 = 's3'
    PULSAR = 'pulsar'
    UNKNOWN = 'unknown'
