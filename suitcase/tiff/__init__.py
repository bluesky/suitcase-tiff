# suitcase subpackages must follow strict naming and interface conventions. The
# public API should include some subset of the following. Any functions not
# implemented should be omitted, rather than included and made to raise
# NotImplementError, so that a client importing this library can immediately
# know which portions of the suitcase API it supports without calling any
# functions.
#
from collections import defaultdict
from pathlib import Path
import json
import tifffile
import event_model
import numpy
import suitcase.utils
from ._version import get_versions

__version__ = get_versions()['version']
del get_versions


class SuitcaseTiffError(Exception):
    ...


class NonSupportedDataShape(SuitcaseTiffError):
    '''used to indicate that non-supported data type is being saved
    '''
    ...


def export(gen, directory, file_prefix='', **kwargs):
    """
    Export a stream of documents to tiff file(s) and one JSON file of metadata.

    Creates {filepath}_meta.json and then {filepath}_{stream_name}.tiff
    for every Event stream.

    The structure of the json is::

            {'metadata': {'start': start_doc, 'stop': stop_doc,
                          'descriptors': {stream_name1: 'descriptor',
                                          stream_name2: ...}},
             stream_name1: {'seq_num': [], 'uid': [], 'time': [],
                         'timestamps': {det_name1:[], det_name2:[],...},
             stream_name2: ...}}

            .. note::

                This schema was chosen as the API is very similar to the
                intake-databroker API. The same schema is used for all json
                files created with our base suitcase export functions.

    Parameters
    ----------
    gen : generator
        expected to yield (name, document) pairs

    directory : string, Path or Wrapper
        The filepath and filename suffix to use in the output files or a file
        handle factory wrapper(see ADD LINK HERE). An empty string will place
        the file in the current directory.

    file_prefix : str
        An optional prefix for the file names that will be created, default is
        an empty string.A templated string may also be used, where curly
        brackets will be filled in with the attributes of the 'start'
        documents.
        e.g., `file_prefix`="scan_{start[scan_id]}-" will result in files with
        names `scan_XXX-'stream_name'.tiff`.

        .. note::

            The `stop` document is excluded as it has not been recieved yet
            when the files are created. The `descriptor` document is excluded
            because there is multiple 'descriptor' documents.

    **kwargs : kwargs
        kwargs to be passed to tifffile.TiffWriter.save.

    Returns
    -------
    dest : tuple
        filepaths of generated files
    """
    meta = defaultdict(dict)  # to be exported as JSON at the end
    meta['metadata']['descriptors'] = defaultdict(dict)
    stream_names = {}  # dict to capture stream_names for each descriptor uid
    files = {}  # map descriptor uid to file handle of tiff file
    filenames = {}  # map descriptor uid to file names of tiff files

    # Load up the correct wrapper.
    if isinstance(directory, (str, Path)):
        wrapper = suitcase.utils.MultiFileWrapper(directory)
        directory = Path(directory)
    else:
        wrapper = directory

    try:
        for name, doc in gen:
            if name == 'start':
                if 'start' in meta['metadata']:
                    raise RuntimeError("This exporter expects documents from "
                                       "one run only.")
                meta['metadata']['start'] = doc
                templated_file_prefix = file_prefix.format(start=doc)
            elif name == 'stop':
                meta['metadata']['stop'] = doc
            elif name == 'descriptor':
                stream_name = doc.get('name')
                sanitized_doc = event_model.sanitize_doc(doc)
                # The line above ensures json type compatibility
                filename = templated_file_prefix + f'{stream_name}.tiff'
                meta['metadata']['descriptors'][stream_name] = sanitized_doc
                f = wrapper.open('stream_data', filename, 'xb')
                files[doc['uid']] = tifffile.TiffWriter(f, bigtiff=True)
                filenames[doc['uid']] = f.name
                stream_names[doc['uid']] = stream_name
                # set up a few parameters to be included in the json file
                meta[stream_name]['seq_num'] = []
                meta[stream_name]['time'] = []
                meta[stream_name]['timestamps'] = {}
                meta[stream_name]['uid'] = []

            elif name in ('event', 'bulk_event', 'event_page'):
                if name == 'event':  # convert event to an event_pages list
                    event_pages = [event_model.pack_event_page(doc)]
                elif name == 'bulk_event':  # convert bulk_event to event_pages
                    event_pages = event_model.bulk_events_to_event_pages(doc)
                else:  # convert an event_page to an event_pages list.
                    event_pages = [doc]

                for event_page in event_pages:
                    event_model.verify_filled(event_page)
                    stream_name = stream_names[event_page['descriptor']]
                    for field in event_page['data']:
                        for img in event_page['data'][field]:
                            # check that the data is 2D, if not raise exception
                            if numpy.asarray(img).ndim == 2:
                                files[event_page['descriptor']].save(img,
                                                                     *kwargs)
                            else:
                                n_dim = numpy.asarray(img).ndim
                                raise NonSupportedDataShape(
                                    f'one or more of the entries for the field'
                                    ' "{}" is not 2 dimensional, at least one '
                                    'was found to be {} dimensional'
                                    ''.format(field, n_dim))
                        if field not in meta[stream_name]['timestamps']:
                            meta[stream_name]['timestamps'][field] = []
                        meta[stream_name]['timestamps'][field].extend(
                            event_page['timestamps'][field])
                    meta[stream_name]['seq_num'].extend(event_page['seq_num'])
                    meta[stream_name]['time'].extend(event_page['time'])
                    meta[stream_name]['uid'].extend(event_page['uid'])

    finally:
        for f in files.values():
            f.close()

    with wrapper.open('run_metadata', f'{templated_file_prefix}meta.json',
                      'x') as f:
        json.dump(meta, f)
    return wrapper._artifacts
