from . import export
import numpy
from numpy.testing import assert_array_equal
import pytest
from suitcase.utils.conftest import (simple_plan,
                                     multi_stream_one_descriptor_plan,
                                     one_stream_multi_descriptors_plan)
import tifffile

expected = numpy.ones((10, 10))


@pytest.mark.parametrize('plan', [simple_plan,
                         multi_stream_one_descriptor_plan,
                         one_stream_multi_descriptors_plan])
@pytest.mark.parametrize('stack_images', [True, False])
def test_export(plan, tmp_path, events_data, stack_images):
    ''' runs a test using the plan that is passed through to it

    ..note::

        Due to the `events_data` `pytest.fixture` this will run multiple tests
        each with a range of detectors and a range of event_types. see
        `suitcase.utils.conftest` for more info

    '''

    collector = events_data(plan)
    artifacts = export(collector, tmp_path, file_prefix='',
                       stack_images=stack_images)

    for filename in artifacts['stream_data']:
        actual = tifffile.imread(str(filename))
        if len(actual.shape) == 3:
            for img in actual:
                assert_array_equal(img, expected)
        else:
            assert_array_equal(actual, expected)
