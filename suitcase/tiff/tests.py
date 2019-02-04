from . import export
import numpy
from numpy.testing import assert_array_equal
import pytest
from suitcase.utils.conftest import (simple_plan,
                                     multi_stream_one_descriptor_plan)
import tempfile
import tifffile

@pytest.fixture()
def expected():
    expected_array = numpy.ones((10, 10))

    return expected_array

@pytest.fixture(params=[True, False], scope='function')
def stack_images(request):
    return request.param


def test_simple_plan(events_data, expected, stack_images):
    ''' runs a test using a simple count plan with num=5
    '''
    run_plan_test(simple_plan, events_data, expected, stack_images)


def test_multi_stream_one_descriptor_plan(events_data, expected, stack_images):
    ''' runs a test using a simple count plan with num=5
    '''
    run_plan_test(multi_stream_one_descriptor_plan, events_data, expected,
                  stack_images)


def run_plan_test(plan, events_data, expected, stack_images):
    ''' runs a test using the plan that is passed through to it

    ..note::

        Due to the `events_data` `pytest.fixture` this will run multiple tests
        each with a range of detectors and a range of event_types. see
        `suitcase.utils.conftest` for more info

    '''

    collector = events_data(simple_plan)
    directory = tempfile.mkdtemp()
    artifacts = export(collector, directory, file_prefix='',
                       stack_images=stack_images)

    for filename in artifacts['stream_data']:
        # the following is required to convert from PosixPath to string
        if not type(filename) == str:
             filename = str(filename)
        actual = tifffile.imread(filename)
        if len(actual.shape) == 3:
            for img in actual:
                assert_array_equal(img, expected)
        else:
            assert_array_equal(actual, expected)
