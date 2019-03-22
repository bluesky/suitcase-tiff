from .. import export
from suitcase.tiff_series.tests.tests import create_expected
from numpy.testing import assert_array_equal
import os
import tifffile


def test_export(tmp_path, example_data):
    ''' runs a test using the plan that is passed through to it.

    ..note::

        Due to the `example_data` `pytest.fixture` this will run multiple tests
        each with a range of detectors and a range of event_types. see
        `suitcase.utils.conftest` for more info.

    '''

    collector = example_data()
    artifacts = export(collector, tmp_path, file_prefix='')
    expected = create_expected(collector, stack_images=True)

    for filename in artifacts.get('stream_data', []):
        actual = tifffile.imread(str(filename))
        streamname = os.path.basename(filename).split('-')[0]
        assert_array_equal(actual, expected[streamname])


def test_file_prefix_formatting(file_prefix_list, example_data, tmp_path):
    ''' runs a test of the file_prefix formatting.

    ..note::

        Due to the `file_prefix_list` and `example_data` `pytest.fixture`'s
        this will run multiple tests each with a range of file_prefixes,
        detectors and event_types. see `suitcase.utils.conftest` for more info.

    '''
    collector = example_data()
    file_prefix = file_prefix_list()
    artifacts = export(collector, tmp_path, file_prefix=file_prefix)

    for name, doc in collector:
        if name == 'start':
            templated_file_prefix = file_prefix.format(**doc).partition('-')[0]

    if artifacts:
        unique_actual = set(str(artifact).split('/')[-1].partition('-')[0]
                            for artifact in artifacts['stream_data'])
        assert unique_actual == set([templated_file_prefix])


def test_directory_formatting(directory_list, example_data, tmp_path):
    ''' runs a test of the file_prefix formatting.

    ..note::

        Due to the `file_prefix_list` and `example_data` `pytest.fixture`'s
        this will run multiple tests each with a range of file_prefixes,
        detectors and event_types. see `suitcase.utils.conftest` for more info.

    '''
    collector = example_data()
    directory = str(tmp_path)+directory_list()
    artifacts = export(collector, directory)

    for name, doc in collector:
        if name == 'start':
            templated_directory = directory.format(**doc).partition('-')[0]

    if artifacts:
        unique_actual = set(str(artifact).rsplit('/', 1)[0].partition('-')[0]
                            for artifact in artifacts['stream_data'])
        assert unique_actual == set([templated_directory])
