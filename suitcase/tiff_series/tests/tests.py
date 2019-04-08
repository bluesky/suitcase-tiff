import event_model
from .. import export
import numpy
from numpy.testing import assert_array_equal
import os
import pytest
import tifffile


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
                expected_dict[stream_name] = numpy.ones((10, 10))
                expected_dict['baseline'] = numpy.ones((10, 10))
            elif len(event_list) == 1:
                expected_dict[stream_name] = numpy.ones((10, 10))
                expected_dict['baseline'] = numpy.ones((2, 10, 10))
            else:
                expected_dict[stream_name] = numpy.ones(
                    (len(event_list), 10, 10))
                expected_dict['baseline'] = numpy.ones((3, 10, 10))

    return expected_dict


@pytest.mark.parametrize("file_prefix", ['test-', 'scan_{start[uid]}-',
                                         'scan_{descriptor[uid]}-',
                                         '{event[uid]}-'])
def test_path_formatting(file_prefix, example_data, tmp_path):
    collector = example_data()
    artifacts = export(collector, tmp_path, file_prefix=file_prefix)

    def _name_templator(collector, file_prefix):
        events_list = []
        descriptors = {}
        for name, doc in collector:
            if name == 'start':
                start = doc
            elif name == 'descriptor':
                descriptors[doc['uid']] = doc
            elif name == 'event_page':
                for event in event_model.unpack_event_page(doc):
                    templated_file_prefix = file_prefix.format(
                        start=start, descriptor=descriptors[doc['descriptor']],
                        event=event)
                    events_list.append(templated_file_prefix.partition('-')[0])
            elif name == 'bulk_events':
                for key, events in doc.items():
                    for event in events:
                        templated_file_prefix = file_prefix.format(
                            start=start,
                            descriptor=descriptors[event['descriptor']],
                            event=event)
                        events_list.append(
                            templated_file_prefix.partition('-')[0])
            elif name == 'event':
                templated_file_prefix = file_prefix.format(
                    start=start, descriptor=descriptors[doc['descriptor']],
                    event=doc)
                events_list.append(templated_file_prefix.partition('-')[0])
        return events_list

    events_list = _name_templator(collector, file_prefix)

    if artifacts:
        unique_actual = set(str(artifact).split('/')[-1].partition('-')[0]
                            for artifact in artifacts['stream_data'])
        assert unique_actual == set(events_list)


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
