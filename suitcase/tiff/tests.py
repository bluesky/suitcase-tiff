from . import export
import numpy
from numpy.testing import assert_array_equal
import pytest
import tifffile

expected = numpy.ones((10, 10))


@pytest.mark.parametrize('stack_images', [True, False])
def test_export(tmp_path, example_data, stack_images):
    ''' runs a test using the plan that is passed through to it

    ..note::

        Due to the `events_data` `pytest.fixture` this will run multiple tests
        each with a range of detectors and a range of event_types. see
        `suitcase.utils.conftest` for more info

    '''

    collector = example_data()
    artifacts = export(collector, tmp_path, file_prefix='',
                       stack_images=stack_images)

    for filename in artifacts.get('stream_data', []):
        actual = tifffile.imread(str(filename))
        if len(actual.shape) == 3:
            for img in actual:
                assert_array_equal(img, expected)
        else:
            assert_array_equal(actual, expected)
