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
    '''used to indicate that non-supported data type is being saved
    '''
    ...


def export(gen, directory, file_prefix='{uid}', **kwargs):
    """
    Export a stream of documents to tiff file(s) and one JSON file of metadata.

    Creates {filepath}_meta.json and then {filepath}_{stream_name}.tiff
    for every Event stream. It can also serialize the data to any file handle.

    The structure of the json is::

            {'metadata': {'start': start_doc, 'stop': stop_doc,
                          'descriptors': {stream_name1: 'descriptor',
                                          stream_name2: ...}},
             'streams': {stream_name1: {'seq_num': [], 'uid': [], 'time': [],
                                        'timestamps': {det_name1:[],
                                                       det_name2:[],...},}
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
    dest : dict
        dict mapping the 'labels' to the file names
    """

    serializer = Serializer(directory, file_prefix, **kwargs)
    try:
        for item in gen:
            serializer(*item)
    finally:
        serializer.close()

    return serializer.artifacts


class Serializer(event_model.DocumentRouter):
    """ Serialize a set of (name, document) tuples to tiff format(s) and one a
    JSON format for the metadata.

    The structure of the json is:

            {'metadata': {'start': start_doc, 'stop': stop_doc,
                          'descriptors': {stream_name1: 'descriptor',
                                          stream_name2: ...}},
             'streams': {stream_name1: {'seq_num': [], 'uid': [], 'time': [],
                                        'timestamps': {det_name1:[],
                                                       det_name2:[],...},}
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

    .. note::

        It is the resonsibility of whatever creates this class to close the
        used file handles when done. To do this use the lines below when
        everything is complete:

        .. code::
            for artifact in serializer.artifacts:
                artifacts.close()
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
            The document dictionary associated with the start message.
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
            The document dictionary associated with the stop message.
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
            The document dictionary associated with the descriptor message.

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

            event documents and bulk_events documents are processed by the
            `event_model.document_router` methods `event` and `bulk_events`.
            Both these methods convert the documents to `event_page` syntax and
            call this `event_page` method, hence adding them to the files.

        Parameters:
        -----------
        doc : dict
            The document dictionary associated with the event_page message.

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
