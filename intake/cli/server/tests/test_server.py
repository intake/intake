import os
import os.path
import time

from tornado.ioloop import IOLoop
from tornado.testing import AsyncHTTPTestCase
import msgpack

from intake import Catalog
from intake.container.serializer import MsgPackSerializer, GzipCompressor
from intake.cli.server.server import IntakeServer


class TestServerV1Base(AsyncHTTPTestCase):
    def get_app(self):
        catalog_file = os.path.join(os.path.dirname(__file__), 'catalog1.yml')
        local_catalog = Catalog(catalog_file)
        self.server = IntakeServer(local_catalog)
        return self.server.make_app()

    def encode(self, msg):
        return msgpack.packb(msg, use_bin_type=True)

    def decode(self, bytestr):
        return msgpack.unpackb(bytestr, encoding='utf-8')


class TestServerV1Info(TestServerV1Base):
    def test_info(self):
        response = self.fetch('/v1/info')
        self.assertEqual(response.code, 200)

        info = self.decode(response.body)

        self.assert_('version' in info)

        expected = [
            {
                'container': 'dataframe',
                'direct_access': 'forbid',
                'description': 'example1 source plugin',
                'name': 'use_example1',
                'user_parameters': []},
            {
                'container': 'dataframe',
                'direct_access': 'forbid',
                'description': 'entry1 full',
                'name': 'entry1',
                'user_parameters': []},
            {
                'container': 'dataframe',
                'direct_access': 'allow',
                'description': 'entry1 part',
                'name': 'entry1_part',
                'user_parameters': [{
                    'allowed': ['1', '2'],
                    'default': '1',
                    'description': 'part of filename',
                    'name': 'part',
                    'type': 'str'
                }]
            }
        ]

        def sort_by_name(seq):
            return sorted(seq, key=lambda d: d['name'])

        for left, right in zip(sort_by_name(info['sources']),
                               sort_by_name(expected)):
            self.assertDictEqual(left, right)


class TestServerV1Source(TestServerV1Base):
    def make_post_request(self, msg, expected_status=200):
        request = self.encode(msg)
        response = self.fetch('/v1/source', method='POST', body=request,
                              headers={'Content-type':
                                       'application/vnd.msgpack'})
        self.assertEqual(response.code, expected_status)

        responses = []
        unpacker = msgpack.Unpacker(encoding='utf-8')
        unpacker.feed(response.body)

        for msg in unpacker:
            responses.append(msg)

        return responses

    def test_open(self):
        import pickle
        msg = dict(action='open', name='entry1', parameters={})
        resp_msg, = self.make_post_request(msg)

        self.assertEqual(resp_msg['container'], 'dataframe')
        self.assertEqual(resp_msg['shape'], [None, 3])
        expected_dtype = {'name': 'object', 'score': 'float64', 'rank': 'int64'}
        actual_dtype = resp_msg['dtype']
        self.assertEqual(expected_dtype, actual_dtype)
        self.assertEqual(resp_msg['npartitions'], 2)
        self.assertEqual(resp_msg['metadata'], dict(foo='bar', bar=[1, 2, 3], cache=[]))

        self.assert_(isinstance(resp_msg['source_id'], str))

    def test_open_direct(self):
        msg = dict(action='open', name='entry1_part', parameters=dict(part='2'),
                   available_plugins=['csv'])
        resp_msg, = self.make_post_request(msg)

        self.assertEqual(resp_msg['plugin'], 'csv')
        args = resp_msg['args']
        self.assertEquals(set(args.keys()), set(['urlpath', 'metadata']))
        self.assert_(args['urlpath'].endswith('/entry1_2.csv'))
        self.assertEquals(args['metadata'], dict(foo='baz', bar=[2, 4, 6], cache=[]))
        self.assertEqual(resp_msg['description'], 'entry1 part')

    def test_read_part_compressed(self):
        msg = dict(action='open', name='entry1', parameters={})
        resp_msg, = self.make_post_request(msg)
        source_id = resp_msg['source_id']

        msg2 = dict(action='read', source_id=source_id,
                    accepted_formats=['msgpack'], accepted_compression=['gzip'],
                    partition=0)
        resp_msgs = self.make_post_request(msg2)

        self.assertEqual(len(resp_msgs), 1)
        ser = MsgPackSerializer()
        comp = GzipCompressor()

        for chunk in resp_msgs:
            self.assertEqual(chunk['format'], 'msgpack')
            self.assertEqual(chunk['compression'], 'gzip')
            self.assertEqual(chunk['container'], 'dataframe')

            data = ser.decode(comp.decompress(chunk['data']),
                              container='dataframe')
            self.assertEqual(len(data), 4)

    def test_read_partition(self):
        msg = dict(action='open', name='entry1', parameters={})
        resp_msg, = self.make_post_request(msg)
        source_id = resp_msg['source_id']

        msg2 = dict(action='read', partition=1, source_id=source_id,
                    accepted_formats=['msgpack'])
        resp_msgs = self.make_post_request(msg2)

        self.assertEqual(len(resp_msgs), 1)
        ser = MsgPackSerializer()

        part = resp_msgs[0]
        self.assertEqual(part['format'], 'msgpack')
        self.assertEqual(part['compression'], 'none')
        self.assertEqual(part['container'], 'dataframe')

        data = ser.decode(part['data'], container='dataframe')
        self.assertEqual(len(data), 4)

    def test_bad_action(self):
        msg = dict(action='bad', name='entry1')
        response, = self.make_post_request(msg, expected_status=400)
        self.assertIn('bad', response['error'])

    def test_no_format(self):
        msg = dict(action='open', name='entry1', parameters={})
        resp_msg, = self.make_post_request(msg)
        source_id = resp_msg['source_id']

        msg2 = dict(action='read', source_id=source_id,
                    accepted_formats=['unknown_format'])
        response, = self.make_post_request(msg2, expected_status=400)
        self.assertIn('compatible', response['error'])

    def test_idle_timer(self):
        self.server.start_periodic_functions(close_idle_after=0.1,
                                             remove_idle_after=0.2)

        msg = dict(action='open', name='entry1', parameters={})
        resp_msg, = self.make_post_request(msg)
        source_id = resp_msg['source_id']

        # Let ioloop run once with do-nothing function to make sure source
        # isn't closed
        time.sleep(0.05)
        IOLoop.current().run_sync(lambda: None)

        # Cheat and look into internal state now
        source = self.server._cache.peek(source_id)
        assert source._dataframe is not None

        # now wait slightly over idle time, run periodic functions,
        # and check again
        time.sleep(0.06)
        IOLoop.current().run_sync(lambda: None)

        # should be closed
        source = self.server._cache.peek(source_id)
        assert source._dataframe is None

        # wait a little longer
        time.sleep(0.1)
        IOLoop.current().run_sync(lambda: None)

        # source should be gone
        with self.assertRaises(KeyError):
            self.server._cache.peek(source_id)
