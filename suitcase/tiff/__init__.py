# suitcase subpackages must follow strict naming and interface conventions. The
# public API should include some subset of the following. Any functions not
# implemented should be omitted, rather than included and made to raise
# NotImplementError, so that a client importing this library can immediately
# know which portions of the suitcase API it supports without calling any
# functions.
#
from collections import defaultdict
import itertools
import json
import tifffile
import event_model
from ._version import get_versions

__version__ = get_versions()['version']
del get_versions


def export(gen, filepath, **kwargs):
    """
    Export a stream of documents to tiff file(s) and one JSON file of metadata.

    Creates {filepath}_meta.json and then {filepath}_{stream_name}.tiff
    for every Event stream.

    The structure of the json is::

        {'start': {...},
        'descriptors':
            {'<stream_name>':{'meta': [{...}, {...}, ...],
                              'seq_num': [...],
                              'time': [...]}
            ...},
        'stop': {...}}

    Parameters
    ----------
    gen : generator
        expected to yield (name, document) pairs

    filepath : str
        the filepath and filename suffix to use in the output files.

    **kwargs : kwargs
        kwargs to be passed to tifffile.TiffWriter.save.

    Returns
    -------
    dest : tuple
        filepaths of generated files
    """
    meta = {}  # to be exported as JSON at the end
    meta['descriptors'] = defaultdict(lambda: defaultdict(list))
    desc_counters = defaultdict(itertools.count)
    stream_names = {}  # dict to capture stream_names for each descriptor uid
    files = {}  # map descriptor uid to file handle of tiff file
    filenames = {}  # map descriptor uid to file names of tiff files

    try:
        for name, doc in gen:
            if name == 'start':
                if 'start' in meta:
                    raise RuntimeError("This exporter expects documents from "
                                       "one run only.")
                meta['start'] = doc
            elif name == 'stop':
                meta['stop'] = doc
            elif name == 'descriptor':
                stream_name = doc.get('name')
                sanitized_doc = event_model.sanitize_doc(doc)
                # The line above ensures json type compatibility
                meta['descriptors'][stream_name]['meta'].append(sanitized_doc)
                filepath_ = (f"{filepath}_{stream_name}_"
                             f"{next(desc_counters[doc['uid']])}.tiff")
                files[doc['uid']] = tifffile.TiffWriter(filepath_,
                                                        bigtiff=True,
                                                        append=True)
                filenames[doc['uid']] = filepath_
                stream_names[doc['uid']] = stream_name
            elif (name == 'event' or name == 'bulk_event' or
                  name == 'event_page'):
                if name == 'event':  # convert event to an event_pages list
                    event_pages = [event_model.pack_event_page(doc)]
                elif name == 'bulk_event':  # convert bulk_event to event_pages
                    event_pages = event_model.bulk_events_to_event_pages(doc)
                else:  # convert an event_page to an event_pages list.
                    event_pages = [doc]

                for event_page in event_pages:
                    if event_model.verify_filled(event_page):
                        for field in event_page['data']:
                            for img in event_page['data'][field]:
                                files[event_page['descriptor']].\
                                    save(img, *kwargs)
                        stream_name = stream_names[event_page['descriptor']]
                        meta['descriptors'][stream_name]['seq_num'].\
                            extend(event_page['seq_num'])
                        meta['descriptors'][stream_name]['time'].\
                            extend(event_page['time'])

    finally:
        for f in files.values():
            f.close()

    with open(f"{filepath}_meta.json", 'w') as f:
        json.dump(meta, f)
    return (f.name,) + tuple(filenames[key] for key in filenames)
