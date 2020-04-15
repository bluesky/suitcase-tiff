import os
from pathlib import Path
import tifffile

from numpy.testing import assert_array_equal

from event_model import DocumentRouter
from .. import export, get_prefixed_filename
from suitcase.tiff_series.tests.tests import create_expected


def test_export(tmp_path, example_data):
    '''Runs a test using the plan that is passed through to it.

    ..note::

        Due to the `example_data` `pytest.fixture` this will run multiple tests
        each with a range of detectors and a range of event_types. See
        `suitcase.utils.conftest` for more info.

    '''

    collector = example_data()
    artifacts = export(collector, tmp_path, file_prefix='')
    expected = create_expected(collector, stack_images=True)

    for filename in artifacts.get('stream_data', []):
        actual = tifffile.imread(str(filename))
        stream_name = os.path.basename(filename).split('-')[0]
        assert_array_equal(actual, expected[stream_name])


def test_file_prefix_formatting(file_prefix_list, example_data, tmp_path):
    '''Runs a test of the ``file_prefix`` formatting.

    ..note::

        Due to the `file_prefix_list` and `example_data` `pytest.fixture`'s
        this will run multiple tests each with a range of file_prefixes,
        detectors and event_types. See `suitcase.utils.conftest` for more info.

    '''
    collector = example_data()
    file_prefix = file_prefix_list()
    artifacts = export(collector, tmp_path, file_prefix=file_prefix)

    for name, doc in collector:
        if name == 'start':
            templated_file_prefix = file_prefix.format(
                start=doc).partition('-')[0]
            break

    if artifacts:
        unique_actual = set(str(artifact).split('/')[-1].partition('-')[0]
                            for artifact in artifacts['stream_data'])
        assert unique_actual == set([templated_file_prefix])


def test_file_prefix_stream_name_field_formatting(example_data, tmp_path):
    '''
    Runs a test of ``file_prefix`` formatting including ``field``
    and ``stream_name``.

    ..note::

        Due to the `example_data` `pytest.fixture` this will run multiple tests
        each with a range of detectors and event_types. See `suitcase.utils.conftest`
        for more info.

    '''
    collector = example_data()
    file_prefix = "test-{stream_name}-{field}/{start[uid]}-"
    artifacts = export(collector, tmp_path, file_prefix=file_prefix)

    class ExpectedFilePathCollector(DocumentRouter):
        def __init__(self):
            self._start_doc = None
            self._descriptors = dict()
            self.expected_file_paths = set()

        def start(self, doc):
            self._start_doc = doc

        def descriptor(self, doc):
            self._descriptors[doc['uid']] = doc

        def event_page(self, doc):
            descriptor = self._descriptors[doc['descriptor']]
            for field in doc['data']:
                data_key = descriptor['data_keys'][field]
                ndim = len(data_key['shape'] or [])
                if data_key['dtype'] == 'array' and 1 < ndim < 4:
                    filename = Path(
                        get_prefixed_filename(
                            file_prefix=file_prefix,
                            start_doc=self._start_doc,
                            field=field,
                            stream_name=descriptor['name']
                        )
                    )

                    self.expected_file_paths.add(str(tmp_path / filename))

    fp_collector = ExpectedFilePathCollector()
    for name, doc_ in collector:
        fp_collector(name, doc_)

    if artifacts:
        unique_actual = {str(artifact) for artifact in artifacts['stream_data']}
        assert unique_actual == fp_collector.expected_file_paths
