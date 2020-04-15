from collections import defaultdict
import itertools
import os
from pathlib import Path
import re

import numpy as np
from numpy.testing import assert_array_equal
import pytest
import tifffile

from bluesky.plans import count
import event_model
from ophyd.sim import DirectImage
from .. import export, get_prefixed_filename, Serializer


def create_expected(collector, stack_images):
    streamnames = {}
    events_dict = {}

    for name, doc in collector:
        if name == 'descriptor':
            streamnames[doc['uid']] = doc.get('name')
        elif name == 'event':
            streamname = streamnames[doc['descriptor']]
            if streamname not in events_dict.keys():
                events_dict[streamname] = []
            events_dict[streamname].append(doc)
        elif name == 'bulk_events':
            for key, events in doc.items():
                for event in events:
                    streamname = streamnames[event['descriptor']]
                    if streamname not in events_dict.keys():
                        events_dict[streamname] = []
                    events_dict[streamname].append(event)
        elif name == 'event_page':
            for event in event_model.unpack_event_page(doc):
                streamname = streamnames[event['descriptor']]
                if streamname not in events_dict.keys():
                    events_dict[streamname] = []
                events_dict[streamname].append(event)

        for stream_name, event_list in events_dict.items():
            expected_dict = {}
            if not stack_images:
                expected_dict[stream_name] = np.ones((10, 10))
                expected_dict['baseline'] = np.ones((10, 10))
            elif len(event_list) == 1:
                expected_dict[stream_name] = np.ones((10, 10))
                expected_dict['baseline'] = np.ones((2, 10, 10))
            else:
                expected_dict[stream_name] = np.ones(
                    (len(event_list), 10, 10))
                expected_dict['baseline'] = np.ones((3, 10, 10))

    return expected_dict


@pytest.mark.parametrize("file_prefix", ['test-', 'scan_{start[uid]}-',
                                         'scan_{descriptor[uid]}-',
                                         '{event[uid]}-',
                                         '{stream_name}_{field}-'])
def test_path_formatting(file_prefix, example_data, tmp_path):
    collector = example_data()
    artifacts = export(collector, tmp_path, file_prefix=file_prefix)

    class ExpectedFilePathCollector(event_model.DocumentRouter):
        def __init__(self):
            self._start_doc = None
            self._descriptors = dict()
            self.expected_file_paths = set()
            self._counter = defaultdict(lambda: defaultdict(itertools.count))

        def start(self, doc):
            self._start_doc = doc

        def descriptor(self, doc):
            self._descriptors[doc['uid']] = doc

        def bulk_events(self, doc):
            for key, events in doc.items():
                for event in events:
                    self.get_filename_for_event(event_doc=event)

        def event_page(self, doc):
            for event in event_model.unpack_event_page(doc):
                self.get_filename_for_event(event_doc=event)

        def event(self, doc):
            self.get_filename_for_event(doc)

        def get_filename_for_event(self, event_doc):
            descriptor = self._descriptors[event_doc['descriptor']]
            stream_name = descriptor["name"]
            for field in event_doc['data']:
                data_key = descriptor['data_keys'][field]
                ndim = len(data_key['shape'] or [])
                if data_key["dtype"] == 'array' and 1 < ndim < 4:
                    num = next(self._counter[stream_name][field])
                    filename = get_prefixed_filename(
                        file_prefix=file_prefix,
                        start_doc=self._start_doc,
                        descriptor_doc=descriptor,
                        event_doc=event_doc,
                        num=num,
                        stream_name=stream_name,
                        field=field
                    )
                    self.expected_file_paths.add(Path(tmp_path) / Path(filename))

    fp_collector = ExpectedFilePathCollector()
    for name, doc_ in collector:
        fp_collector(name, doc_)

    if artifacts:
        unique_actual = {Path(artifact) for artifact in artifacts['stream_data']}
        assert unique_actual == fp_collector.expected_file_paths


def test_export(tmp_path, example_data):
    ''' runs a test using the plan that is passed through to it

    ..note::

        Due to the `events_data` `pytest.fixture` this will run multiple tests
        each with a range of detectors and a range of event_types. see
        `suitcase.utils.conftest` for more info

    '''

    collector = example_data()
    artifacts = export(collector, tmp_path, file_prefix='')
    expected = create_expected(collector, stack_images=False)

    for filename in artifacts.get('stream_data', []):
        actual = tifffile.imread(str(filename))
        streamname = os.path.basename(filename).split('-')[0]
        assert_array_equal(actual, expected[streamname])


def test_1d_data(RE, tmp_path):
    """
    Expect 0 file written.
    """
    def return_1d_data():
        return np.zeros((3,))

    det = DirectImage(name="1d_image", func=return_1d_data)

    tiff_serializer = Serializer(directory=tmp_path)
    RE.subscribe(tiff_serializer)
    RE(count([det], num=5))

    assert "stream_data" not in tiff_serializer.artifacts

    filenames = os.listdir(path=tmp_path)
    assert len(filenames) == 0


def test_2d_data(RE, tmp_path):
    """
    Expect one file written for each detector trigger.
    """
    def return_2d_data():
        return np.zeros((3, 3))

    det = DirectImage(name="2d_image", func=return_2d_data)

    tiff_serializer = Serializer(directory=tmp_path)
    RE.subscribe(tiff_serializer)
    RE(count([det], num=5))

    assert len(tiff_serializer.artifacts["stream_data"]) == 5

    filenames = os.listdir(path=tmp_path)
    assert len(filenames) == 5
    assert all([re.search(r"-\d+\.tiff$", filename) for filename in filenames])


def test_3d_data(RE, tmp_path):
    """
    Expect 3 files written for each detector trigger.
    """
    def return_3d_data():
        return np.zeros((3, 3, 3))

    det = DirectImage(name="3d_image", func=return_3d_data)

    tiff_serializer = Serializer(directory=tmp_path)
    RE.subscribe(tiff_serializer)
    RE(count([det], num=5))

    assert len(tiff_serializer.artifacts["stream_data"]) == 15

    filenames = os.listdir(path=tmp_path)
    assert len(filenames) == 15
    assert all([re.search(r"-\d+\.tiff$", filename) for filename in filenames])


def test_4d_data(RE, tmp_path):
    """
    Expect 0 files written.
    """
    def return_4d_data():
        return np.zeros((3, 3, 3, 3))

    det = DirectImage(name="4d_image", func=return_4d_data)

    tiff_serializer = Serializer(directory=tmp_path)
    RE.subscribe(tiff_serializer)
    RE(count([det], num=5))

    assert "stream_data" not in tiff_serializer.artifacts

    filenames = os.listdir(path=tmp_path)
    assert len(filenames) == 0
