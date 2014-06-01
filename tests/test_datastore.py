import json
import os

from StringIO import StringIO
from contextlib import closing, contextmanager
from datetime import datetime
from mox import Mox, IgnoreArg
from shutil import rmtree
from tempfile import mkdtemp
from unittest import TestCase

from jsonlog import ConcurrentModificationException
from jsonlog import DataStore
from jsonlog import DataStoreException
from jsonlog import DataStoreFS
from jsonlog import FileAlreadyExistsException

from jsonlog.datastore import atomic_rename

class TestAtomicRename(TestCase):
    def setUp(self):
        self.tempdir = mkdtemp()
        self.fs = DataStoreFS(self.tempdir)

    def tearDown(self):
        rmtree(self.tempdir)

    def test_atomic_rename_fails_when_file_exists(self):
        existing_filename = os.path.join(self.tempdir, 'not-to-be-overwritten')
        source_filename = os.path.join(self.tempdir, 'new-version')

        with open(existing_filename, 'w') as outf:
            outf.write('this must be preserved')

        with open(source_filename, 'w') as outf:
            outf.write('this is the new version')

        self.assertRaises(
            FileAlreadyExistsException,
            atomic_rename,
            source_filename,
            existing_filename
        )


class TestDataStoreFS(TestCase):
    def setUp(self):
        self.tempdir = mkdtemp()
        self.fs = DataStoreFS(self.tempdir)

    def tearDown(self):
        rmtree(self.tempdir)

    @contextmanager
    def assert_tempdir_stays_empty(self):
        yield

        files_in_temp = list(os.listdir(os.path.join(self.tempdir, '__temp')))
        self.assertTrue(
            not files_in_temp,
            "we do not expect files in temp dir, found %s" % files_in_temp
        )

    def create_file(self, path, content):
        with open(os.path.join(self.tempdir, path), mode='w') as outf:
            outf.write(content)

    def create_empty_file(self, path):
        self.create_file(path, '')

    def test_it_list_files(self):
        self.create_empty_file('b')
        self.create_empty_file('c')
        self.assertEquals(2, len(list(self.fs.listfiles(''))))

    def test_it_list_top_dirs(self):
        dirpath = os.path.join(self.tempdir, 'd')

        os.mkdir(dirpath)
        self.assertEquals(set([
            os.path.join(self.tempdir, '__temp'),
            dirpath,
        ]), set(self.fs.listdirs()))

    def test_it_opens_file_for_reading(self):
        sentinel = 'abcdefg'
        self.create_file('b', sentinel)

        with self.fs.open_for_reading('b') as inf:
            self.assertEquals(sentinel, inf.read())

    def test_it_creates_files(self):
        sentinel = 'abcdefg'

        with self.assert_tempdir_stays_empty():
            with self.fs.open_new_file('c') as outf:
                outf.write(sentinel)

        with self.fs.open_for_reading('c') as inf:
            self.assertEquals(sentinel, inf.read())


    def test_it_create_files_iff_they_dont_exist_yet(self):
        with self.assert_tempdir_stays_empty():
            with self.fs.open_new_file('c') as outf:
                outf.write('A')

        has_entered = []

        def create_again():
            with self.fs.open_new_file('c') as outf:
                has_entered.append(True)

        with self.assert_tempdir_stays_empty():
            self.assertRaises(FileAlreadyExistsException, create_again)
        self.assertTrue(has_entered)


class TestDataStore(TestCase):
    def setUp(self):
        self.mox = Mox()

    def tearDown(self):
        self.mox.UnsetStubs()

    def test_it_queries_the_filesystem_for_the_latest_version(self):
        fs = self.mox.CreateMock(DataStoreFS)
        ds = DataStore(fs)

        pj = os.path.join
        fs.listfiles('my-id').AndReturn([
            pj('my-id', 'item-0.json'),
            pj('my-id', 'item-15.json'),
            pj('my-id', 'item-1.json'),
            pj('my-id', 'item-6.json'),
            pj('my-id', 'readme.txt'),
        ])
        fs.open_for_reading(os.path.join('my-id', 'item-15.json')).AndReturn(
            closing(StringIO('{"name":"the_name"}'))
        )
        self.mox.ReplayAll()

        item = ds.get('my-id')
        self.assertEquals(dict(name='the_name', version=15), item)
        self.mox.VerifyAll()

    def test_it_bails_out_when_no_item_is_found(self):
        fs = self.mox.CreateMock(DataStoreFS)
        ds = DataStore(fs)
        fs.listfiles('my-id').AndReturn([
            os.path.join('my-id', 'readme.txt')
        ])
        self.mox.ReplayAll()

        self.assertRaises(DataStoreException, ds.get, 'my-id')
        self.mox.VerifyAll()

    def test_it_bails_out_when_item_cannot_be_read(self):
        fs = self.mox.CreateMock(DataStoreFS)
        ds = DataStore(fs)
        fs.listfiles('my-id').AndReturn([
            os.path.join('my-id', 'item-0.json')
        ])
        fs.open_for_reading(os.path.join('my-id', 'item-0.txt')).AndRaise(
            Exception()
        )
        self.mox.ReplayAll()

        self.assertRaises(DataStoreException, ds.get, 'my-id')
        self.mox.VerifyAll()

    def test_it_creates_a_new_version_on_put(self):
        fs = self.mox.CreateMock(DataStoreFS)
        def clock_now():
            return datetime(1970, 1, 1)

        ds = DataStore(fs, clock_now)

        first_version = os.path.join('my-id', 'item-0.json')
        fs.listfiles('my-id').AndReturn([
            first_version
        ])
        fs.open_for_reading(first_version).AndReturn(
            closing(StringIO('{"name":"the name"}'))
        )

        new_version = os.path.join('my-id', 'item-1.json')
        new_content = StringIO()
        @contextmanager
        def not_closing_content():
            yield new_content

        fs.open_new_file(new_version).AndReturn(
            not_closing_content()
        )
        self.mox.ReplayAll()

        item = ds.get('my-id')
        item['name'] = 'the new name'

        ds.put('item', 'my-id', item)

        self.assertEquals(
            json.loads(new_content.getvalue()),
            dict(
                name='the new name',
                version=1,
                creation_date={ '__datetime__': True, 'iso8601': '19700101T000000' }
            )
        )
        self.mox.VerifyAll()


    def test_it_fails_when_two_concurrent_puts_happen(self):
        fs = self.mox.CreateMock(DataStoreFS)
        ds = DataStore(fs)

        fp = os.path.join('my-id', 'item-32.json')

        fs.open_new_file(fp).AndReturn(closing(StringIO()))
        fs.open_new_file(fp).AndRaise(FileAlreadyExistsException('file already exists'))

        self.mox.ReplayAll()

        ds.put('item', 'my-id', dict(name='hello', version=31))
        self.assertRaises(
            ConcurrentModificationException,
            ds.put,
            'item',
            'my-id',
            dict(name='hello', version=31)
        )
        self.mox.VerifyAll()

    def test_it_can_list_all_ids(self):
        fs = self.mox.CreateMock(DataStoreFS)
        ds = DataStore(fs)

        items = [
            os.path.join('item1', 'item-1.json'),
            os.path.join('item2', 'item-5.json')
        ]

        fs.listdirs().AndReturn(['__temp', 'item1', 'item2'])
        fs.listfiles('item1').AndReturn([items[0]])
        fs.open_for_reading(items[0]).AndReturn(
            closing(StringIO('{"name":"a"}'))
        )
        fs.listfiles('item2').AndReturn([items[1]])
        fs.open_for_reading(items[1]).AndReturn(
            closing(StringIO('{"name":"b"}'))
        )
        self.mox.ReplayAll()

        self.assertEquals(set(['item1', 'item2']), set(ds.all_ids()))

        self.mox.VerifyAll()
