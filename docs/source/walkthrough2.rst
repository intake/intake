Walkthrough
===========

Preamble
--------

Intake is a bunch of _readers_. A reader is a class that encodes how to load some
particular data, including all the arguments that will be passed to some third-party
package. Thus, you can encode anything from the simplest ``pd.read_csv`` call to
far more complex things. You can also act on readers, to record a set of
operations known as a Pipeline. A Pipeline is just a special kind of reader.

Catalogs contain readers and data definitions. Readers can and should be saved in Catalogs.
Catalogs and readers

.. note::

    "Load" means make a machine representations: many data containers may be lazy,
    where you get an object on which you can act, but the data is only read on
    demand for actions that rerquire it.
