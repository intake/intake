## Intake Take2

Intake has been extensively rewritten to produce Intake Take2,
https://github.com/intake/intake/pull/737 .
This will now become the version of the ``main`` branch and be released as v2.0.0. The
main documentation will move to describing V2, and V1 will not be further developed.
Existing users of the legacy version ("v1") may find their code breaks and will need
a version pin, although we aim to support most legacy workflows via backward compatibility.

To install, you would do the following

```shell
> pip install intake
or
> conda install intake
```

To get v1:

```shell
> pip install "intake<2"
or
> conda install "intake<2"
```

This README is being kept to describe why the rewrite was done and considerations that
went into it.

### Motivation for the rewrite.

The main way to get the most out of Intake v1 has been by editing YAML files. This is
how the documentation is structured. Yes, you could use intake.open_* to seed them, but then
you will find a strong discontinuity between the documentation of the driver and the third
party library that actually does the reading.

This made is very unlikely to convert a novice data-oriented python user into someone
that can create even the simplest catalogs. They will certainly never use more advanced
features like parametrisation or derived datasets. The new model eases users in and lends
itself to being overlaid with graphical/wizard interfaces (i.e., in jupyterlab or in
preparation for use with
[anaconda.cloud](https://docs.anaconda.com/free/anaconda-notebooks/notebook-data-catalog/)).

### Main changes

This is a total rewrite. Backward compatibility is desired and some v1 sources have been
rewritten to use the v2 readers.

#### Simplification

We are dropping features that added complexity but were only rarely used.

- the server; the Intake server was never production-ready, and most
 use-cases can be provided by [tiled](https://blueskyproject.io/tiled/)
- the caching/persist stuff; files can be persisted by fsspec, and we maintain the ability to
 write to various formats
- explicit dependence on dask; dask is just one of many possible compute engines and
 an we should not be tied to one
- less added functionality in the readers (like file pattern stuff)
- explicit dependence on hvplot (but you can still choose to use it)
- the CLI


#### New structure

Many new classes have appeared. From an intake-savy point of view, the biggest change is
the splitting of "drivers" into "data" and "reader". I view them as the objective description
of what the dataset is (e.g., "this is CSV at this URL") versus how you might load it
("call pandas with these arguments"). This strongly implies that you might want to read the
same data in different ways. Crucially, it makes the readers much easier to write.

Here is the Awkward reader for parquet files. Particularly for files, often all you need to do
is specify which function will do the read and what keyword accepts the URL.
```python
class AwkwardParquet(Awkward):
    implements = {datatypes.Parquet}
    imports = {"awkward", "pyarrow"}
    func = "awkward:from_parquet"
    url_arg = "path"
```

The imports are declared and deferred until needed, so there is no need to make all those intake-*
repos with their own dependencies. (Of course, you might still want to declare packages
and requirements; considering whether catalogs should have requirements, but this is better
suited for something like conda-project). The arguments accepted are the same as for the
target function, and the method `.doc()` will show this.


### New features

- recommendation system to try to guess the right data type from a URL or existing function call,
 and readers that can use that type (and for each, tells you the instance it makes and provides docs).
 Can be extended to "I have this type but I want this other type, what
 set of steps get me there"
- embracing any compute engines as first-class (e.g., duck, dask, ray, spark) or none
- no constraints on the types of data that can/should be returned
- pipeline building tools, including explicit conversion, types operations, generalised getattr and
 getitem (like dask delayed) and apply. Most of these available as "transform" attributes, including
 new namespaces like "reader.np.max(..)" will call numpy on whatever the reader makes, but lazily.
- output functions, as a special type of "conversion", returning a new data description for further
 manipulation. This is effectively caching (would like to add conditions to the pipeline, only load and
 convert if converted version doesn't already exist).
- generalised derived datasets, including functions of multiple intake inputs. A data or any reader
 output might be the input of any other reader, forming a graph. Picking a specific output from those
 possible gives you the pipeline, ready for execution. Any such pipelines could be encoded in a catalog.
- user parameters are similar to before, but are also plugable; a few types are provided.
 Some helper methods have been made
 to walk data/reader kwargs and extract default values as parameters, replacing their original value
 with a reference to the parameter. The parameters are hierarchical catalog->data->reader

Some examples of each of these exist in the current state of the code. There are many many more to
write, but the functions themselves are really simple. This is aiming for composition and easy crowd
sourcing, high bus factor.

### Work to follow

- thorough search capability, which will need some thoughts in this context
- compatibility with remaining existing intake plugins
- the catalog serialisation currently uses custom YAML tags, but this should not be necessary
- add those magic methods that make pipelines work on descriptions on catalogs, not just
 materialised readers.
- metadata conventions, to persist basic dataset properties (e.g., based on frictionlessdata spec)
 and validation as a pipeline operation you can do to any data entry using any available reader that
 can produce the info
- probably much more - I will need help!

### Unanswered questions

- actual functions and classes are now embedded into any YAML serialised catalog as strings. These
 are imported/instantiated when the reader is instantiated from its description. So arbitrary
 code execution is possible, but not at catalog parse time. We only have a loose permissions config
 story around this
- this implementation maintains the distinction between "descriptions" (which have templated values
 and user parameters) and readers (which only have concrete values and real instances). Is this a
 major confusion we somehow want to eliminate in V2?
