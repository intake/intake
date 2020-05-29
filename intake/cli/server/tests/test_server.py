#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import os
import os.path

import pytest
import requests
import shutil
import subprocess
import time

from tornado.ioloop import IOLoop
from tornado.testing import AsyncHTTPTestCase
import msgpack

from intake import open_catalog
from intake.container.serializer import MsgPackSerializer, GzipCompressor
from intake.cli.server.server import IntakeServer
from intake.compat import unpack_kwargs, pack_kwargs
from intake.utils import make_path_posix

catalog_file = make_path_posix(
    os.path.join(os.path.dirname(__file__), 'catalog1.yml'))


class TestServerV1Base(AsyncHTTPTestCase):
    def get_app(self):
        local_catalog = open_catalog(catalog_file)
        self.server = IntakeServer(local_catalog)
        return self.server.make_app()

    def encode(self, msg):
        return msgpack.packb(msg, **pack_kwargs)

    def decode(self, bytestr):
        return msgpack.unpackb(bytestr, **unpack_kwargs)


class TestServerV1Info(TestServerV1Base):
    def test_info(self):
        response = self.fetch('/v1/info')
        self.assertEqual(response.code, 200)

        info = self.decode(response.body)

        self.assertTrue('version' in info)

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
            for k in right:
                assert left[k] == right[k]


class TestServerV1Source(TestServerV1Base):
    def make_post_request(self, msg, expected_status=200):
        request = self.encode(msg)
        response = self.fetch('/v1/source', method='POST', body=request,
                              headers={'Content-type':
                                       'application/vnd.msgpack'})
        self.assertEqual(response.code, expected_status)

        responses = []
        if expected_status < 400:
            unpacker = msgpack.Unpacker(**unpack_kwargs)
            unpacker.feed(response.body)

            for msg in unpacker:
                responses.append(msg)
        else:
            responses = [{'error': str(response.error)}]

        return responses

    def test_open(self):
        msg = dict(action='open', name='entry1', parameters={})
        resp_msg, = self.make_post_request(msg)

        self.assertEqual(resp_msg['container'], 'dataframe')
        self.assertEqual(resp_msg['shape'], [None, 3])
        expected_dtype = {'name': 'object', 'score': 'float64', 'rank': 'int64'}
        actual_dtype = resp_msg['dtype']
        self.assertEqual(expected_dtype, actual_dtype)
        self.assertEqual(resp_msg['npartitions'], 2)
        md = resp_msg['metadata']
        md.pop('catalog_dir', None)
        self.assertEqual(md, dict(foo='bar', bar=[1, 2, 3]))

        self.assertTrue(isinstance(resp_msg['source_id'], str))

    def test_open_direct(self):
        msg = dict(action='open', name='entry1_part', parameters=dict(part='2'),
                   available_plugins=['csv'])
        resp_msg, = self.make_post_request(msg)

        self.assertTrue('csv' in resp_msg['plugin'])
        args = resp_msg['args']
        assert 'urlpath' in args
        self.assertTrue(args['urlpath'].endswith('/entry1_2.csv'))
        md = resp_msg['metadata']
        md.pop('catalog_dir', None)
        self.assertEqual(md, dict(foo='baz', bar=[2, 4, 6]))
        self.assertEqual(resp_msg['description'], 'entry1 part')

    def test_read_part_compressed(self):
        # because the msgpack format actually depends on pyarrow
        pytest.importorskip('pyarrow')

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
        # because the msgpack format actually depends on pyarrow
        pytest.importorskip('pyarrow')
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


@pytest.fixture()
def multi_server(tmpdir):
    fn1 = make_path_posix(os.path.join(tmpdir, 'cat1.yaml'))
    shutil.copyfile(catalog_file, fn1)
    fn2 = make_path_posix(os.path.join(tmpdir, 'cat2.yaml'))
    shutil.copyfile(catalog_file, fn2)
    P = subprocess.Popen(['intake-server', fn1, fn2, '--no-flatten'])
    t = time.time()
    while True:
        try:
            requests.get('http://localhost:5000')
            yield 'intake://localhost:5000'
            break
        except:
            time.sleep(0.2)
            if time.time() - t > 10:
                break
    P.terminate()
    P.wait()
    shutil.rmtree(tmpdir)


def test_flatten_flag(multi_server):
    cat = open_catalog(multi_server)
    assert list(cat) == ['cat1', 'cat2']
    assert 'use_example1' in cat.cat1()


def free_port():
    import socket

    s = socket.socket()
    s.bind(('', 0))  # Bind to a free port provided by the host.
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.fixture(scope='function')
def port_server(tmpdir):
    fn1 = make_path_posix(os.path.join(tmpdir, 'cat1.yaml'))
    shutil.copyfile(catalog_file, fn1)
    port = free_port()

    P = subprocess.Popen(['intake-server', '--port', str(port), fn1])
    t = time.time()
    try:
        while True:
            try:
                requests.get('http://localhost:%s' % port)
                yield 'intake://localhost:%s' % port
                break
            except:
                time.sleep(0.2)
                if time.time() - t > 10:
                    break
    finally:
        P.terminate()
        P.wait()
        shutil.rmtree(tmpdir)


def test_port_flag(port_server):
    cat = open_catalog(port_server)
    assert 'use_example1' in list(cat)


@pytest.fixture()
def address_server(tmpdir):
    fn1 = make_path_posix(os.path.join(tmpdir, 'cat1.yaml'))
    shutil.copyfile(catalog_file, fn1)
    port = free_port()
    P = subprocess.Popen(['intake-server', '--port', str(port), '--address', '0.0.0.0', fn1])
    t = time.time()
    while True:
        try:
            requests.get('http://0.0.0.0:%s' % port)
            yield 'intake://0.0.0.0:%s' % port
            break
        except:
            time.sleep(0.2)
            if time.time() - t > 10:
                break
    P.terminate()
    P.wait()
    shutil.rmtree(tmpdir)


def test_address_flag(address_server):
    cat = open_catalog(address_server)
    assert 'use_example1' in list(cat)
