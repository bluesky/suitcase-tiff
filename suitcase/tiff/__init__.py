# Suitcase subpackages must follow strict naming and interface conventions. The
# public API should include some subset of the following. Any functions not
# implemented should be omitted, rather than included and made to raise
# NotImplementError, so that a client importing this library can immediately
# know which portions of the suitcase API it supports without calling any
# functions.
from collections import defaultdict
from pathlib import Path
from tifffile import TiffWriter
import event_model
import numpy
import suitcase.utils
from ._version import get_versions

__version__ = get_versions()['version']
del get_versions


def export(gen, directory, file_prefix='{uid}-', stack_images=True, **kwargs):
    """
    Export a stream of documents to TIFF stack(s).

    This creates a file named:
    ``<directory>/<file_prefix>{stream_name}-{field}.tiff``
    for every Event stream and field that contains 2D 'image like' data.

    .. warning::

        This process explicitly ignores all data that is not 2D and does not
        include any metadata in the output file.

    .. note::

        This can alternatively be used to write data to generic buffers rather
        than creating files on disk. See the documentation for the
        ``directory`` parameter below.

    Parameters
    ----------
    gen : generator
        expected to yield ``(name, document)`` pairs

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
        string may include templates as in ``{proposal_id}-{sample_name}-``,
        which are populated from the RunStart document. The default value is
        ``{uid}-`` which is guaranteed to be present and unique. A more
        descriptive value depends on the application and is therefore left to
        the user.

    stack_images : Boolean
        This indicates if we want one image per file (`stack_images` = `False`)
        or many images per file (`stack_images` = `True`). If using
        `stack_images` = `False` then an additional image number is added to
        the file name.

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

    >>> export(gen, '', '{time:%%Y-%%m-%%d_%%H:%%M}-')

    Place the files in a different directory, such as on a mounted USB stick.

    >>> export(gen, '/path/to/my_usb_stick')
    """
    serializer = Serializer(directory, file_prefix,
                            stack_images=stack_images, **kwargs)
    try:
        for item in gen:
            serializer(*item)
    finally:
        serializer.close()

    return serializer.artifacts


class Serializer(event_model.DocumentRouter):
    """
    Serialize a stream of documents to TIFF stack(s).

    This creates a file named:
    ``<directory>/<file_prefix>{stream_name}-{field}.tiff``
    for every Event stream and field that contains 2D 'image like' data.

    .. warning::

        This process explicitly ignores all data that is not 2D and does not
        include any metadata in the output file.


    .. note::

        This can alternatively be used to write data to generic buffers rather
        than creating files on disk. See the documentation for the
        ``directory`` parameter below.

    Parameters
    ----------
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
        string may include templates as in ``{proposal_id}-{sample_name}-``,
        which are populated from the RunStart document. The default value is
        ``{uid}-`` which is guaranteed to be present and unique. A more
        descriptive value depends on the application and is therefore left to
        the user.

    stack_images : Boolean
        This indicates if we want one image per file (`stack_images` = `False`)
        or many images per file (`stack_images` = `True`). If using
        `stack_images` = `False` then an additional image number is added to
        the file name.

    **kwargs : kwargs
        kwargs to be passed to ``tifffile.TiffWriter.save``.
    """
    def __init__(self, directory, file_prefix='{uid}-', stack_images=True,
                 **kwargs):

        if isinstance(directory, (str, Path)):
            self.manager = suitcase.utils.MultiFileManager(directory)
        else:
            self.manager = directory

        self._streamnames = defaultdict(dict)  # stream_names to desc  uids
        # Map stream name to dict that maps field names to TiffWriter objects.
        self._tiff_writers = defaultdict(dict)
        self._file_prefix = file_prefix
        self._templated_file_prefix = ''
        self._kwargs = kwargs
        self._start_found = False
        self._stack_images = stack_images
        self._counter = defaultdict(dict)  # map stream_name to field/# dict

    @property
    def artifacts(self):
        # The manager's artifacts attribute is itself a property, and we must
        # access it a new each time to be sure to get the latest content.
        return self.manager.artifacts

    def start(self, doc):
        '''Extracts `start` document information for formatting file_prefix.

        This method checks that only one `start` document is seen and formats
        `file_prefix` based on the contents of the `start` document.

        Parameters:
        -----------
        doc : dict
            RunStart document
        '''

        # raise an error if this is the second `start` document seen.
        if self._start_found:
            raise RuntimeError(
                "The serializer in suitcase.tiff expects documents from one "
                "run only. Two `start` documents where sent to it")
        else:
            self._start_found = True

        # format self._file_prefix
        self._templated_file_prefix = self._file_prefix.format(**doc)

    def descriptor(self, doc):
        '''Use `descriptor` doc to map stream_names to descriptor uid's.

        This method usess the descriptor document information to map the
        stream_names to descriptor uid's.

        Parameters:
        -----------
        doc : dict
            EventDescriptor document
        '''
        # extract some useful info from the doc
        streamname = doc.get('name')
        self._streamnames[doc['uid']] = streamname

    def event_page(self, doc):
        '''Add event page document information to a ".tiff" file.

        This method adds event_page document information to a ".tiff" file,
        creating it if nesecary.

        .. warning::

            All non 2D 'image like' data is explicitly ignored.

        .. note::

            The data in Events might be structured as an Event, an EventPage,
            or a "bulk event" (deprecated). The DocumentRouter base class takes
            care of first transforming the other repsentations into an
            EventPage and then routing them through here, so no further action
            is required in this class. We can assume we will always receive an
            EventPage.

        Parameters:
        -----------
        doc : dict
            EventPage document
        '''
        event_model.verify_filled(doc)
        streamname = self._streamnames[doc['descriptor']]
        for field in doc['data']:
            for img in doc['data'][field]:
                # check that the data is 2D, if not ignore it
                if numpy.asarray(img).ndim == 2:
                    if self._stack_images:
                        # create a file for this stream and field if required
                        if not self._tiff_writers.get(streamname, {}).get(field):
                            filename = (f'{self._templated_file_prefix}'
                                        f'{streamname}-{field}.tiff')
                            file = self.manager.open(
                                'stream_data', filename, 'xb')
                            tw = TiffWriter(file, bigtiff=True)
                            self._tiff_writers[streamname][field] = tw
                        # append the image to the file
                        tw = self._tiff_writers[streamname][field]
                        tw.save(img, *self._kwargs)
                    else:
                        if not (self._counter.get(streamname, {}).get(field) or
                                self._counter.get(streamname, {}).get(field)
                                == 0):
                            self._counter[streamname][field] = 0
                        else:
                            self._counter[streamname][field] += 1
                        num = self._counter[streamname][field]
                        filename = (f'{self._templated_file_prefix}'
                                    f'{streamname}-{field}-{num}.tiff')
                        file = self.manager.open('stream_data', filename, 'xb')
                        tw = TiffWriter(file, bigtiff=True)
                        self._tiff_writers[streamname][field+f'-{num}'] = tw
                        tw.save(img, *self._kwargs)

    def close(self):
        '''Close all of the files opened by this Serializer.
        '''
        # Close all the TiffWriter instances, which do some work on cleanup.
        for tw_by_stream in self._tiff_writers.values():
            for tw in tw_by_stream.values():
                tw.close()
        # Then let the manager (perhaps redundantly) close the underlying
        # files.
        self.manager.close()
