from collections import defaultdict
import itertools
from pathlib import Path
import warnings

import numpy
from tifffile import TiffWriter

import event_model
from suitcase import tiff_stack

from ._version import get_versions

__version__ = get_versions()['version']
del get_versions


def export(gen, directory, file_prefix='{start[uid]}-', astype='uint16',
           bigtiff=False, byteorder=None, imagej=False, **kwargs):
    """
    Export a stream of documents to a series of TIFF files.

    This creates a file named:
    ``<directory>/<file_prefix>{stream_name}-{field}-{image_number}.tiff``
    for every Event stream and field that contains 2D 'image-like' data.

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
        interface. See the suitcase documentation
        (http://nsls-ii.github.io/suitcase) for details.

    astype : numpy dtype
        The image array is converted to this type before being passed to
        tifffile. The default is 16-bit integer (``'uint16'``) since many image
        viewers cannot open higher bit depths. This parameter may be given as a
        numpy dtype object (``numpy.uint32``) or the equivalent string
        (``'uint32'``).

    file_prefix : str, optional
        The first part of the filename of the generated output files. This
        string may include templates as in
        ``{start[proposal_id]}-{start[sample_name]}-``, which are populated
        from the RunStart (start), EventDescriptor (descriptor) or Event
        (event) documents. The default value is ``{start[uid]}-`` which is
        guaranteed to be present and unique. A more descriptive value depends
        on the application and is therefore left to the user.
        Two additional template parameters ``{stream_name}`` and ``{field}``
        are supported. These will be replaced with stream name and detector
        name, respectively.

    bigtiff : boolean, optional
        Passed into ``tifffile.TiffWriter``. Default False.

    byteorder : string or None, optional
        Passed into ``tifffile.TiffWriter``. Default None.

    imagej: boolean, optional
        Passed into ``tifffile.TiffWriter``. Default False.

    **kwargs : kwargs
        kwargs to be passed to ``tifffile.TiffWriter.write``.

    Returns
    -------
    artifacts : dict
        Maps 'labels' to lists of artifacts (e.g. filepaths)

    Examples
    --------

    Generate files with unique-identifier names in the current directory.

    >>> export(gen, '')

    Generate files with more readable metadata in the file names.

    >>> export(gen, '', '{start[plan_name]}-{start[motors]}-')

    Include the experiment's start time formatted as YYYY-MM-DD_HH-MM.

    >>> export(gen, '', '{start[time]:%Y-%m-%d_%H:%M}-')

    Place the files in a different directory, such as on a mounted USB stick.

    >>> export(gen, '/path/to/my_usb_stick')
    """
    with Serializer(directory, file_prefix,
                    astype=astype,
                    bigtiff=bigtiff,
                    byteorder=byteorder,
                    imagej=imagej,
                    **kwargs) as serializer:
        for item in gen:
            serializer(*item)

    return serializer.artifacts


# Note that this is a subclass of tiff_stack.Serializer to reduce code
# duplication.


class Serializer(tiff_stack.Serializer):
    """
    Serialize a stream of documents to a series of TIFF files.

    This creates a file named:
    ``<directory>/<file_prefix>{stream_name}-{field}-{image_number}.tiff``
    for every Event stream and field that contains 2D 'image-like' data.

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
        interface. See the suitcase documentation
        (http://nsls-ii.github.io/suitcase) for details.

    file_prefix : str, optional
        The first part of the filename of the generated output files. This
        string may include templates as in
        ``{start[proposal_id]}-{start[sample_name]}-``, which are populated
        from the RunStart (start), EventDescriptor (descriptor) or Event
        (event) documents. The default value is ``{start[uid]}-`` which is
        guaranteed to be present and unique. A more descriptive value depends
        on the application and is therefore left to the user.
        Two additional template parameters ``{stream_name}`` and ``{field}``
        are supported. These will be replaced with stream name and detector
        name, respectively.

    astype : numpy dtype
        The image array is converted to this type before being passed to
        tifffile. The default is 16-bit integer (``'uint16'``) since many image
        viewers cannot open higher bit depths. This parameter may be given as a
        numpy dtype object (``numpy.uint32``) or the equivalent string
        (``'uint32'``).

    bigtiff : boolean, optional
        Passed into ``tifffile.TiffWriter``. Default False.

    byteorder : string or None, optional
        Passed into ``tifffile.TiffWriter``. Default None.

    imagej: boolean, optional
        Passed into ``tifffile.TiffWriter``. Default False.

    event_num_pad : int, optional
        The number of 0s to left-pad the event number to in the filename.

    **kwargs : kwargs
        kwargs to be passed to ``tifffile.TiffWriter.write``.
    """
    def __init__(
            self, directory, file_prefix='{start[uid]}-', astype='uint16',
            bigtiff=False, byteorder=None, imagej=False, *,
            event_num_pad=5, **kwargs
    ):
        super().__init__(directory, file_prefix=file_prefix, astype=astype,
                         bigtiff=bigtiff, byteorder=byteorder, imagej=imagej,
                         **kwargs)
        # maps stream name to dict that map field name to index (#)
        self._counter = defaultdict(lambda: defaultdict(itertools.count))
        self._event_num_pad = event_num_pad

    def event_page(self, doc):
        '''Converts an 'event_page' doc to 'event' docs for processing.

        Parameters:
        -----------
        doc : dict
            Event_Page document
        '''

        events = event_model.unpack_event_page(doc)
        for event_doc in events:
            self.event(event_doc)

    def event(self, doc):
        '''Add event document information to a ".tiff" file.

        This method adds event document information to a ".tiff" file,
        creating it if necessary.

        .. warning::

            All non 2D 'image-like' data is explicitly ignored.

        .. note::

            The data in Events might be structured as an Event, an EventPage,
            or a "bulk event" (deprecated). The DocumentRouter base class takes
            care of first transforming the other representations into an
            EventPage and then routing them through here, as we require Event
            documents _in this case_ we overwrite both the `event` method and
            the `event_page` method so we can assume we will always receive an
            Event.

        Parameters:
        -----------
        doc : dict
            Event document
        '''
        event_model.verify_filled(event_model.pack_event_page(*[doc]))
        descriptor = self._descriptors[doc['descriptor']]
        stream_name = descriptor.get('name')
        for field in doc['data']:
            img = doc['data'][field]
            # Check that the data is 2D or 3D; if not ignore it.
            data_key = descriptor['data_keys'][field]
            ndim = len(data_key['shape'] or [])
            if data_key['dtype'] == 'array' and 1 < ndim < 4:
                img_asarray = numpy.asarray(img, dtype=self._astype)
                if tuple(data_key['shape']) != img_asarray.shape:
                    warnings.warn(
                        f"The descriptor claims the data shape is {data_key['shape']} "
                        f"but the data is actual data shape is {img_asarray.shape}! "
                        f"This will be an error in the future."
                    )
                    ndim = img_asarray.ndim

                if ndim == 2:
                    # handle 2D data just like 3D data
                    # by adding a 3rd dimension
                    img_asarray = numpy.expand_dims(img_asarray, axis=0)
                for i in range(img_asarray.shape[0]):
                    img_asarray_2d = img_asarray[i, :]
                    num = next(self._counter[stream_name][field])
                    filename = get_prefixed_filename(
                        file_prefix=self._file_prefix,
                        start_doc=self._start,
                        descriptor_doc=descriptor,
                        event_doc=doc,
                        num=num,
                        stream_name=stream_name,
                        field=field,
                        pad=self._event_num_pad
                    )
                    fname = self._manager.reserve_name('stream_data', filename)
                    Path(fname).parent.mkdir(parents=True, exist_ok=True)
                    tw = TiffWriter(fname, **self._init_kwargs)
                    self._tiff_writers[stream_name][field+f'-{num}'] = tw
                    tw.write(img_asarray_2d, *self._kwargs)


def get_prefixed_filename(
        file_prefix,
        start_doc,
        descriptor_doc,
        event_doc,
        num,
        stream_name,
        field,
        pad):
    '''Assemble the prefixed filename.'''
    templated_file_prefix = file_prefix.format(
        start=start_doc,
        descriptor=descriptor_doc,
        event=event_doc,
        stream_name=stream_name,
        field=field
    )
    filename = (f'{templated_file_prefix}'
                f'{stream_name}-{field}-{num:0{pad}d}.tiff')
    return filename
