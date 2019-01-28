# Suitcase subpackages must follow strict naming and interface conventions. The
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
import collections
from ._version import get_versions

__version__ = get_versions()['version']
del get_versions


class SuitcaseTiffError(Exception):
    ...


class NonSupportedDataShape(SuitcaseTiffError):
    '''used to indicate that non-supported data shape is being saved
    '''
    ...


def export(gen, directory, file_prefix='{uid}', **kwargs):
    """
    Export a stream of documents to TIFF stack(s) and one JSON file.

    This creates one file named ``<filepath>_meta.json`` and a file named
    ``<filepath>_{stream_name}.tiff`` for every Event stream.

    The structure of the JSON is::

        {'metadata': {'start': start_doc, 'stop': stop_doc,
                        'descriptors': {stream_name1: 'descriptor',
                                        stream_name2: ...}},
            'streams': {stream_name1: {'seq_num': [], 'uid': [], 'time': [],
                                    'timestamps': {det_name1:[],
                                                    det_name2:[],...},}
                        stream_name2: ...}}

    .. note::

        This can alternatively be used to write data to generic buffers rather
        than creating files on disk. See the documentation for the
        ``directory`` parameter below.

    Parameters
    ----------
    gen : generator
        xpected to yield ``(name, document)`` pairs

    directory : string, Path or Manager.
        For basic uses, this should be the path to the output directory given
        as a string or Path object. Use an empty string ``''`` to place files
        in the current working directory.

        In advanced applications, this may direct the serialized output to a
        memory buffer, network socket, or other writable buffer. It should be
        an instance of ``suitcase.utils.MemoryBufferManager`` and
        ``suitcase.utils.MultiFileManager`` or any object implementing that
        inferface. See the suitcase documentation (LINK ONCE WRITTEN) for
        details.

    file_prefix : str, optional
        The first part of the filename of the generated output files. This
        string may include templates as in ``{proposal_id}-{sample_name}``,
        which are populated from the RunStart document. The default value is
        ``{uid}`` which is guaranteed to be present and unique. A more
        descriptive value depends on the application and is therefore left to
        the user.

    **kwargs : kwargs
        kwargs to be passed to ``tifffile.TiffWriter.save``.

    Returns
    -------
    dest : dict
        dict mapping the 'labels' to lists of file names

    Examples
    --------

    Generate files with unique-identifer names in the current directory.

    >>> export(gen, '')

    Generate files with more readable metadata in the file names.

    >>> export(gen, '', '{plan_name}-{motors}-')

    Include the experiment's start time formatted as YY-MM-DD_HH-MM.

    >>> export(gen, '', '{time:%%Y-%%m-%%d_%%H:%%M}')

    Place the files in a different directory, such as on a mounted USB stick.

    >>> export(gen, '/path/to/my_usb_stick')
    """
    serializer = Serializer(directory, file_prefix, **kwargs)
    try:
        for item in gen:
            serializer(*item)
    finally:
        serializer.close()

    return serializer.artifacts


class Serializer(event_model.DocumentRouter):
    """
    Serialize a stream of documents to TIFF stack(s) and one JSON file.

    This creates one file named ``<filepath>_meta.json`` and a file named
    ``<filepath>_{stream_name}.tiff`` for every Event stream.

    The structure of the JSON is::

        {'metadata': {'start': start_doc, 'stop': stop_doc,
                        'descriptors': {stream_name1: 'descriptor',
                                        stream_name2: ...}},
            'streams': {stream_name1: {'seq_num': [], 'uid': [], 'time': [],
                                    'timestamps': {det_name1:[],
                                                    det_name2:[],...},}
                        stream_name2: ...}}

    .. note::

        This can alternatively be used to write data to generic buffers rather
        than creating files on disk. See the documentation for the
        ``directory`` parameter below.

    Parameters
    ----------
    gen : generator
        xpected to yield ``(name, document)`` pairs

    directory : string, Path or Manager.
        For basic uses, this should be the path to the output directory given
        as a string or Path object. Use an empty string ``''`` to place files
        in the current working directory.

        In advanced applications, this may direct the serialized output to a
        memory buffer, network socket, or other writable buffer. It should be
        an instance of ``suitcase.utils.MemoryBufferManager`` and
        ``suitcase.utils.MultiFileManager`` or any object implementing that
        inferface. See the suitcase documentation (LINK ONCE WRITTEN) for
        details.

    file_prefix : str, optional
        The first part of the filename of the generated output files. This
        string may include templates as in ``{proposal_id}-{sample_name}``,
        which are populated from the RunStart document. The default value is
        ``{uid}`` which is guaranteed to be present and unique. A more
        descriptive value depends on the application and is therefore left to
        the user.

    **kwargs : kwargs
        kwargs to be passed to ``tifffile.TiffWriter.save``.

    Returns
    -------
    dest : dict
        dict mapping the 'labels' to lists of file names
    """
    def __init__(self, directory, file_prefix='{uid}', **kwargs):

        if isinstance(directory, (str, Path)):
            self.manager = suitcase.utils.MultiFileManager(directory)
        else:
            self.manager = directory

        self.artifacts = self.manager._artifacts
        self._meta = defaultdict(dict)  # to be exported as JSON at the end
        self._meta['metadata']['descriptors'] = defaultdict(dict)
        self._meta['streams'] = defaultdict(dict)
        self._stream_names = {}  # maps stream_names to each descriptor uids
        self._files = {}  # map descriptor uid to file handle of tiff file
        self._filenames = {}  # map descriptor uid to file names of tiff files
        self._file_prefix = file_prefix
        self._templated_file_prefix = ''
        self._kwargs = kwargs


    def start(self, doc):
        '''Add `start` document information to the metadata dictionary.

        This method adds the start document information to the metadata
        dictionary. In addition it checks that only one `start` document is
        seen.

        Parameters:
        -----------
        doc : dict
            RunStart document
        '''

        # raise an error if this is the second `start` document seen.
        if 'start' in self._meta['metadata']:
            raise RuntimeError(
                "The serializer in suitcase.tiff expects documents from one "
                "run only. Two `start` documents where sent to it")

        # add the start doc to self._meta and format self._file_prefix
        self._meta['metadata']['start'] = doc
        self._templated_file_prefix = self._file_prefix.format(**doc)

        # return the start document
        return doc


    def stop(self, doc):
        '''Add `stop` document information to the metadata dictionary.

        This method adds the stop document information to the metadata
        dictionary. In addition it also creates the metadata '.json' file and
        exports the metadata dictionary to it.

        Parameters:
        -----------
        doc : dict
            RunStop document
        '''
        # add the stop doc to self._meta.
        self._meta['metadata']['stop'] = doc

        # open a json file for the metadata and add self._meta to it.
        f = self.manager.open('run_metadata',
                              f'{self._templated_file_prefix}meta.json', 'xt')
        json.dump(self._meta, f)
        self.artifacts['run_metadata'].append(f)
        self._files['meta'] = f

        # return the stop document
        return doc


    def descriptor(self, doc):
        '''Add `descriptor` document information to the metadata dictionary.

        This method adds the descriptor document information to the metadata
        dictionary. In addition it also creates the file for data with the
        stream_name given by the descriptor doc for later use.

        Parameters:
        -----------
        doc : dict
            EventDescriptor document
        '''
        # extract some useful info from the doc
        stream_name = doc.get('name')
        filename = f'{self._templated_file_prefix}{stream_name}.tiff'
        # replace numpy objects with python ones to ensure json compatibility
        sanitized_doc = event_model.sanitize_doc(doc)
        # Add the doc to self._meta
        self._meta['metadata']['descriptors'][stream_name] = sanitized_doc
        # initialize some items in self._meta for use by event_page later
        self._meta['streams'][stream_name]['seq_num'] = []
        self._meta['streams'][stream_name]['time'] = []
        self._meta['streams'][stream_name]['timestamps'] = {}
        self._meta['streams'][stream_name]['uid'] = []
        # open the file handle to write the event_page data to later
        f = self.manager.open('stream_data', filename, 'xb')
        # use the file handle to create the tiff file writing object
        self._files[doc['uid']] = tifffile.TiffWriter(f, bigtiff=True)
        # record the filenames and stream names in the associated dictionaries
        self._filenames[doc['uid']] = f.name
        self._stream_names[doc['uid']] = stream_name

        # return the descriptor doc
        return doc


    def event_page(self, doc):
        '''Add event page document information to the ".tiff" file.

        This method adds event_page document information to the ".tiff" file
        and adds the extra information in the document to the metadata ".json"
        file.

        .. note::

            The data in Events might be structured as an Event, an EventPage,
            or a "bulk event" (deprecated). The DocumentRouter base class takes
            care of routing all three representations through here, so no
            further action is required in this class.

        Parameters:
        -----------
        doc : dict
            EventPage document
        '''
        event_model.verify_filled(doc)
        stream_name = self._stream_names[doc['descriptor']]
        for field in doc['data']:
            for img in doc['data'][field]:
                # check that the data is 2D, if not raise exception
                if numpy.asarray(img).ndim == 2:
                    self._files[doc['descriptor']].save(img, *self._kwargs)
                else:
                    n_dim = numpy.asarray(img).ndim
                    raise NonSupportedDataShape(
                        'one or more of the entries for the field "{}" is not'
                        '2 dimensional, at least one was found to be {} '
                        'dimensional'.format(field, n_dim))
            if field not in self._meta['streams'][stream_name]['timestamps']:
                self._meta['streams'][stream_name]['timestamps'][field] = []
            self._meta['streams'][stream_name]['timestamps'][field].extend(
                doc['timestamps'][field])
        self._meta['streams'][stream_name]['seq_num'].extend(doc['seq_num'])
        self._meta['streams'][stream_name]['time'].extend(doc['time'])
        self._meta['streams'][stream_name]['uid'].extend(doc['uid'])

        # return the event_page document
        return doc

    def close(self):
        '''close all of the files opened by this serializer
        '''
        for file in self._files.values():
            file.close()
