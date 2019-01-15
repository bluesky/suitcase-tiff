# Tests should generate (and then clean up) any files they need for testing. No
# binary files should be included in the repository.

# Tests should generate (and then clean up) any files they need for testing. No
# binary files should be included in the repository.
from bluesky.plans import count
import json
import tempfile
import tifffile
from suitcase.tiff import export
import event_model
from numpy.testing import assert_array_equal
import numpy


def test_export_events(RE, hw):
    '''Test to see if the suitcase.csv.export works on events.
    '''
    collector = []

    def collect(name, doc):
        collector.append((name, doc))

    RE.subscribe(collect)
    RE(count([hw.direct_img], 5))

    with tempfile.NamedTemporaryFile(mode='w') as f:
        # We don't actually need f itself, just a filepath to template on.
        meta, *tiffs = export(collector, f.name)
    tiff, = tiffs

    docs = (doc for name, doc in collector)
    start, descriptor, *events, stop = docs

    expected = {}
    expected_dict = {'data': {'img': [], 'seq_num': []}, 'time': []}
    for event in events:
        expected_dict['data']['img'].append(event['data']['img'])
        expected_dict['data']['seq_num'].append(event['seq_num'])
        expected_dict['time'].append(event['time'])

    expected['events'] = numpy.array(expected_dict['data']['img'])

    with open(meta) as f:
        actual = json.load(f)
    # This next section is used to convert lists to tuples for the assert below
    for dims in actual['start']['hints']['dimensions']:
        new_dims = []
        for dim in dims:
            if type(dim) is list:
                new_dims.append(tuple(dim))
            else:
                new_dims.append(dim)
        actual['start']['hints']['dimensions'] = [tuple(new_dims)]

    expected.update({'start': start, 'stop': stop,
                     'descriptors': {'primary':
                         {'meta': [descriptor],
                          'seq_num': expected_dict['data']['seq_num'],
                          'time' : expected_dict['time']}}})
    actual['events'] = tifffile.imread(tiff)
    assert actual.keys() == expected.keys()
    assert actual['start'] == expected['start']
    assert actual['descriptors'] == expected['descriptors']
    assert actual['stop'] == expected['stop']
    assert_array_equal(expected['events'], actual['events'])


def test_export_bulk_event(RE, hw):
    '''Test to see if suitcase.csv.export() works on bulk_events
    '''
    collector = []
    events = []

    def collect(name, doc):
        if name == 'event':
            events.append(doc)
        elif name == 'stop':
            collector.append(('bulk_event', {'primary': events}))
            collector.append((name, doc))
        else:
            collector.append((name, doc))

    RE.subscribe(collect)
    RE(count([hw.det], 5))

    with tempfile.NamedTemporaryFile(mode='w') as f:
        # We don't actually need f itself, just a filepath to template on.
        meta, *csvs = export(collector, f.name)
    csv, = csvs

    docs = (doc for name, doc in collector)
    start, descriptor, *bulk_events, stop = docs

    expected = {}
    expected_dict = {'data': {'det': [], 'seq_num': []}, 'time': []}
    for event in events:
        expected_dict['data']['det'].append(event['data']['det'])
        expected_dict['data']['seq_num'].append(event['seq_num'])
        expected_dict['time'].append(event['time'])

    expected['events'] = pandas.DataFrame(expected_dict['data'],
                                          index=expected_dict['time'])
    expected['events'].index.name = 'time'

    with open(meta) as f:
        actual = json.load(f)
    # This next section is used to convert lists to tuples for the assert below
    for dims in actual['start']['hints']['dimensions']:
        new_dims = []
        for dim in dims:
            if type(dim) is list:
                new_dims.append(tuple(dim))
            else:
                new_dims.append(dim)
        actual['start']['hints']['dimensions'] = [tuple(new_dims)]

    expected.update({'start': start, 'stop': stop,
                     'descriptors': {'primary': [descriptor]}})
    actual['events'] = pandas.read_csv(csv, index_col=0)
    assert actual.keys() == expected.keys()
    assert actual['start'] == expected['start']
    assert actual['descriptors'] == expected['descriptors']
    assert actual['stop'] == expected['stop']
    assert_frame_equal(expected['events'], actual['events'])


def test_export_event_page(RE, hw):
    collector = []
    events = []

    def collect(name, doc):
        if name == 'event':
            events.append(doc)
        elif name == 'stop':
            collector.append(('event_page',
                              event_model.pack_event_page(*events)))
            collector.append((name, doc))
        else:
            collector.append((name, doc))

    RE.subscribe(collect)
    RE(count([hw.det], 5))

    with tempfile.NamedTemporaryFile(mode='w') as f:
        # We don't actually need f itself, just a filepath to template on.
        meta, *csvs = export(collector, f.name)
    csv, = csvs

    docs = (doc for name, doc in collector)
    start, descriptor, *event_pages, stop = docs

    expected = {}
    expected_dict = {'data': {'det': [], 'seq_num': []}, 'time': []}
    for event in events:
        expected_dict['data']['det'].append(event['data']['det'])
        expected_dict['data']['seq_num'].append(event['seq_num'])
        expected_dict['time'].append(event['time'])

    expected['events'] = pandas.DataFrame(expected_dict['data'],
                                          index=expected_dict['time'])
    expected['events'].index.name = 'time'

    with open(meta) as f:
        actual = json.load(f)
    # This next section is used to convert lists to tuples for the assert below
    for dims in actual['start']['hints']['dimensions']:
        new_dims = []
        for dim in dims:
            if type(dim) is list:
                new_dims.append(tuple(dim))
            else:
                new_dims.append(dim)
        actual['start']['hints']['dimensions'] = [tuple(new_dims)]

    expected.update({'start': start, 'stop': stop,
                     'descriptors': {'primary': [descriptor]}})
    actual['events'] = pandas.read_csv(csv, index_col=0)
    assert actual.keys() == expected.keys()
    assert actual['start'] == expected['start']
    assert actual['descriptors'] == expected['descriptors']
    assert actual['stop'] == expected['stop']
    assert_frame_equal(expected['events'], actual['events'])
