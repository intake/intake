#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import os
from intake.utils import make_path_posix


def unzip(f, outpath):
    import zipfile
    z = zipfile.ZipFile(f, 'r')
    z.extractall(outpath)
    out = [make_path_posix(os.path.join(outpath, fn.filename))
           for fn in z.filelist]
    z.close()
    return out


def untargz(f, outpath):
    import tarfile
    tar = tarfile.open(f, "r:gz")
    out = [make_path_posix(os.path.join(outpath, fn.name))
           for fn in tar.getmembers()]
    tar.extractall(outpath)
    tar.close()
    return out


def untarbz(f, outpath):
    import tarfile
    tar = tarfile.open(f, "r:bz2")
    out = [make_path_posix(os.path.join(outpath, fn.name))
           for fn in tar.getmembers()]
    tar.extractall(outpath)
    tar.close()
    return out


def untar(f, outpath):
    import tarfile
    tar = tarfile.open(f, "r:")
    out = [make_path_posix(os.path.join(outpath, fn.name))
           for fn in tar.getmembers()]
    tar.extractall(outpath)
    tar.close()
    return out


def ungzip(f, outpath):
    import gzip
    z = gzip.open(f)
    fn = os.path.basename(f)[:-3]
    with open(os.path.join(outpath, fn), 'wb') as fout:
        data = True
        while data:
            data = z.read(2**15)
            fout.write(data)
    return [make_path_posix(os.path.join(outpath, fn))]


def unbzip(f, outpath):
    import bz2
    z = bz2.open(f)
    fn = os.path.basename(f)[:-3]
    with open(os.path.join(outpath, fn), 'wb') as fout:
        data = True
        while data:
            data = z.read(2 ** 15)
            fout.write(data)
    return [make_path_posix(os.path.join(outpath, fn))]


decomp = {'zip': unzip,
          'tgz': untargz,
          'tbz': untarbz,
          'tar': untar,
          'gz': ungzip,
          'bz': unbzip}
