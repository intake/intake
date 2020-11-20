#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

from .dataframe import RemoteDataFrame
from .ndarray import RemoteArray
from .semistructured import RemoteSequenceSource
from ..catalog.remote import RemoteCatalog
from ..utils import ContainerRegistryView

# each container type is represented in the remote by one of the classes in
# this dictionary
_container_map = {
    'dataframe': RemoteDataFrame,
    'python': RemoteSequenceSource,
    'ndarray': RemoteArray,
    'numpy': RemoteArray,
    'catalog': RemoteCatalog
}
container_map = ContainerRegistryView(_container_map)  # public, read-only view

__all__ = ['container_map', 'register_container', 'unregister_container']


def register_container(name, container, overwrite=False):
    """
    Add to the container registry, ``intake.container.container_map``.

    Parameters
    ----------
    name: string
    container: DataSource
    overwrite: bool, optional
        False by default.

    Raises
    ------
    ValueError
        If name collides with an existing name in the container registry and
        overwrite is False.
    """
    if name in _container_map and not overwrite:
        # If we are re-registering the same object, there is no problem.
        original = _container_map[name]
        if original is container:
            return
        raise ValueError(
            f"The container {container} could not be registered for the "
            f"name {name} because {_container_map[name]} is already "
            f"registered for that name. Use overwrite=True to force it.")
    _container_map[name] = container


def unregister_container(name):
    """
    Ensure that a given name in the container registry is cleared.

    This function is idempotent: if the name does not exist in
    ``intake.container.container_map``, nothing is done, and the function
    returns None

    Parameters
    ----------
    name: string

    Returns
    -------
    container: DataSource or None
        Whatever was registered for ``name``, or ``None``
    """
    return _container_map.pop(name, None)



def upload(data, path, **kwargs):
    """Given a concrete data object, store it at given location return Source

    Use this function to publicly share data which you have created in your
    python session. Intake will try each of the container types, to see if
    one of them can handle the input data, and write the data to the path
    given, in the format most appropriate for the data type, e.g., parquet for
    pandas or dask data-frames.

    With the DataSource instance you get back, you can add this to a catalog,
    or just get the YAML representation for editing (``.yaml()``) and
    sharing.

    Parameters
    ----------
    data : instance
        The object to upload and store. In many cases, the dask or in-memory
        variant are handled equivalently.
    path : str
        Location of the output files; can be, for instance, a network drive
        for sharing over a VPC, or a bucket on a cloud storage service
    kwargs : passed to the writer for fine control

    Returns
    -------
    DataSource instance
    """
    for cls in container_map.values():
        try:
            s = cls._data_to_source(data, path, **kwargs)
            if s is not None:
                return s
        except NotImplementedError:
            pass
    raise TypeError('No class found to handle given data')
