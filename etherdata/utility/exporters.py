import csv
import io
import threading
from json import JSONEncoder
import six


class BaseItemExporter(object):

    def __init__(self, **kwargs):
        self._configure(kwargs)

    def _configure(self, options, dont_fail=False):
        self.encoding = options.pop('encoding', None)
        self.fields_to_export = options.pop('fields_to_export', None)
        self.export_empty_fields = options.pop('export_empty_fields', False)
        self.indent = options.pop('indent', None)
        if not dont_fail and options:
            raise TypeError("Unexpected options: %s" % ', '.join(options.keys()))

    def export_item(self, item):
        raise NotImplementedError

    def serialize_field(self, field, name, value):
        serializer = field.get('serializer', lambda x: x)
        return serializer(value)

    def start_exporting(self):
        pass

    def finish_exporting(self):
        pass

    def _get_serialized_fields(self, item, default_value=None, include_empty=None):
        if include_empty is None:
            include_empty = self.export_empty_fields
        if self.fields_to_export is None:
            if include_empty and not isinstance(item, dict):
                field_iter = six.iterkeys(item.fields)
            else:
                field_iter = six.iterkeys(item)
        else:
            if include_empty:
                field_iter = self.fields_to_export
            else:
                field_iter = (x for x in self.fields_to_export if x in item)

        for field_name in field_iter:
            if field_name in item:
                field = {} if isinstance(item, dict) else item.fields[field_name]
                value = self.serialize_field(field, field_name, item[field_name])
            else:
                value = default_value

            yield field_name, value


class CsvItemExporter(BaseItemExporter):

    def __init__(self, file, include_headers_line=True, join_multivalued=',', **kwargs):
        self._configure(kwargs, dont_fail=True)
        if not self.encoding:
            self.encoding = 'utf-8'
        self.include_headers_line = include_headers_line
        self.stream = io.TextIOWrapper(
            file,
            line_buffering=False,
            write_through=True,
            encoding=self.encoding
        ) if six.PY3 else file
        self.csv_writer = csv.writer(self.stream, **kwargs)
        self._headers_not_written = True
        self._join_multivalued = join_multivalued
        self._write_headers_lock = threading.Lock()

    def serialize_field(self, field, name, value):
        serializer = field.get('serializer', self._join_if_needed)
        return serializer(value)

    def _join_if_needed(self, value):
        if isinstance(value, (list, tuple)):
            try:
                return self._join_multivalued.join(str(x) for x in value)
            except TypeError:  # list in value may not contain strings
                pass
        return value

    def export_item(self, item):
        if self._headers_not_written:
            with self._write_headers_lock:
                if self._headers_not_written:
                    self._write_headers_and_set_fields_to_export(item)
                    self._headers_not_written = False

        fields = self._get_serialized_fields(item, default_value='',
                                             include_empty=True)
        values = list(self._build_row(x for _, x in fields))
        self.csv_writer.writerow(values)

    def _build_row(self, values):
        for s in values:
            try:
                yield to_native_str(s, self.encoding)
            except TypeError:
                yield s

    def _write_headers_and_set_fields_to_export(self, item):
        if self.include_headers_line:
            if not self.fields_to_export:
                if isinstance(item, dict):
                    # for dicts try using fields of the first item
                    self.fields_to_export = list(item.keys())
                else:
                    # use fields declared in Item
                    self.fields_to_export = list(item.fields.keys())
            row = list(self._build_row(self.fields_to_export))
            self.csv_writer.writerow(row)


class JsonLinesItemExporter(BaseItemExporter):

    def __init__(self, file, **kwargs):
        self._configure(kwargs, dont_fail=True)
        self.file = file
        kwargs.setdefault('ensure_ascii', not self.encoding)
        self.encoder = JSONEncoder(**kwargs)

    def export_item(self, item):
        itemdict = dict(self._get_serialized_fields(item))
        data = self.encoder.encode(itemdict) + '\n'
        self.file.write(to_bytes(data, self.encoding))


def to_native_str(text, encoding=None, errors='strict'):
    if six.PY2:
        return to_bytes(text, encoding, errors)
    else:
        return to_unicode(text, encoding, errors)


def to_bytes(text, encoding=None, errors='strict'):
    if isinstance(text, bytes):
        return text
    if not isinstance(text, six.string_types):
        raise TypeError('to_bytes must receive a unicode, str or bytes '
                        'object, got %s' % type(text).__name__)
    if encoding is None:
        encoding = 'utf-8'
    return text.encode(encoding, errors)


def to_unicode(text, encoding=None, errors='strict'):
    if isinstance(text, six.text_type):
        return text
    if not isinstance(text, (bytes, six.text_type)):
        raise TypeError('to_unicode must receive a bytes, str or unicode '
                        'object, got %s' % type(text).__name__)
    if encoding is None:
        encoding = 'utf-8'
    return text.decode(encoding, errors)
