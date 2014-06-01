import json
import logging
import os
import re
import sys
import traceback

from contextlib import contextmanager
from datetime import datetime
from tempfile import mkstemp

from dateutil import parser as date_parser

logger = logging.getLogger(__name__)

class FileAlreadyExistsException(ValueError):
    pass

class DataStoreException(Exception):
    def __init__(self, msg='', exc_info=None):
        def format_exc_info():
            if exc_info is not None:
                return traceback.format_exception(*exc_info)

            return ''

        exception_message = '%s, %s' % (msg, format_exc_info())
        logger.error(exception_message)

        super(DataStoreException, self).__init__(
            exception_message
        )


class ConcurrentModificationException(DataStoreException):
    pass

def atomic_rename(src, dst):
    try:
        if os.name == 'posix':
            os.link(src, dst)
            os.unlink(src)
        elif os.name == 'nt':
            os.rename(src, dst)
        else:
            raise NotImplementedError
    except NotImplementedError:
        raise
    except Exception as e:
        raise FileAlreadyExistsException(e)
    finally:
        if os.path.isfile(src):
            logger.info('removing leftover %s', src)
            os.unlink(src)

class DataStoreFS(object):
    def __init__(self, root_dir):
        self.root_dir = os.path.abspath(root_dir)
        self.temp_dir = os.path.join(self.root_dir, '__temp')
        if not os.path.exists(self.temp_dir):
            os.mkdir(self.temp_dir)

    def resolve(self, path):
        return os.path.join(self.root_dir, path)

    def listdirs(self):
        for fn in os.listdir(self.root_dir):
            path = os.path.join(self.root_dir, fn)
            if os.path.isdir(path):
                yield path


    def listfiles(self, dirpath):
        for fn in os.listdir(self.resolve(dirpath)):
            path = os.path.join(self.resolve(dirpath), fn)
            if os.path.isfile(path):
                yield path

    def open_for_reading(self, filepath):
        return open(self.resolve(filepath), mode='r')


    @contextmanager
    def open_new_file(self, filepath):
        fd, fp = mkstemp(dir=self.temp_dir)

        exc_info = None, None, None
        try:
            with os.fdopen(fd, 'w') as context:
                yield context
        except:
            exc_info = sys.exc_info()
            raise
        finally:
            if exc_info[0] is None:
                atomic_rename(fp, self.resolve(filepath))


class DataStore(object):
    DATE_STRING = "%Y%m%dT%H%M%S%z"

    def __init__(self, fs, datetime_now=None):
        if datetime_now is None:
           datetime_now = datetime.now

        self.fs = fs
        self.now = datetime_now

    def all_ids(self):
        for dn in self.fs.listdirs():
            basename = os.path.basename(dn)
            if basename not in ['__temp']:
                # test the item actually exists
                self.get(basename)
                yield basename

    def serialized_date(self, date):
        return date.strftime(self.DATE_STRING)

    def parse_date(self, serialized_date):
        return date_parser.parse(serialized_date)

    def dto(self, item):
        return json.loads(json.dumps(item, cls=self.json_encoder()))

    def get(self, id):
        def item_log():
            item_re = re.compile('^[a-zA-Z]+-([0-9]+).json$')

            for fp in self.fs.listfiles(id):
                item_match = item_re.match(os.path.basename(fp))
                if not item_match:
                    continue

                yield fp, int(item_match.group(1))

        try:
            latest_item = max(item_log(), key=lambda tuple: tuple[1])

            with self.fs.open_for_reading(latest_item[0]) as inf:
                content = json.load(inf, object_hook=self.json_decode)
                content['version'] = latest_item[1]
                return content

        except Exception as e:
            raise DataStoreException('for id %s' % id, sys.exc_info())

    def json_encoder(self):
        datastore = self
        class Encoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, datetime):
                    return {
                        '__datetime__': True,
                        'iso8601': datastore.serialized_date(obj)
                    }

                return json.JSONEncoder.default(self, obj)

        return Encoder

    def json_decode(self, dct):
        if '__datetime__' in dct:
            return self.parse_date(dct['iso8601'])
        return dct


    def put(self, type_tag, id, item):
        new_version_number = item['version'] + 1
        new_version = os.path.join(id, '%s-%d.json' % (type_tag, new_version_number))
        now = self.now()

        try:
            with self.fs.open_new_file(new_version) as outf:

                item['version'] = new_version_number
                if 'creation_date' not in item:
                    item['creation_date'] = now
                else:
                    item['updating_date'] = now
                json.dump(item, outf, cls=self.json_encoder())

                return new_version_number
        except FileAlreadyExistsException as e:
            raise ConcurrentModificationException(sys.exc_info())
