"""
A set of I/O utils that allow us to open files on remote storage as if they were present locally.
Currently only includes wrappers for Google's GCS, but this can easily be expanded for AWS S3\
buckets.
"""
import os
from glob import glob
from pathy import Pathy  # https://github.com/justindujardin/pathy


def is_remote_path(path):
    """
    Returns True iff the path is one of the remote formats that this
    module supports
    """
    return path.startswith('gs://') or path.startswith('hdfs://')


def path_exists_remote(path):
    """
    Wrapper that allows existance check of local and remote paths like
    `gs://...`
    """
    if is_remote_path(path):
        return Pathy(path).exists()
    return os.path.exists(path)


def open_remote(path, mode='r', buffering=-1, encoding=None, newline=None, closefd=True, opener=None):
    """
    Wrapper around open() method that can handle remote paths like `gs://...`
    off Google Cloud using Tensorflow's IO helpers.

    buffering, encoding, newline, closefd, and opener are ignored for remote files

    This enables us to do:
    with open_remote('gs://.....', mode='w+') as f:
        do something with the file f, whether or not we have local access to it
    """
    if is_remote_path(path):
        return Pathy(path).open(
            mode=mode,
            buffering=(8192 if buffering == -1 else buffering),
            encoding=encoding,
            newline=newline
        )
    return open(
        path, mode,
        buffering=buffering,
        encoding=encoding,
        newline=newline,
        closefd=closefd,
        opener=opener
    )


def isdir_remote(path):
    """
    Wrapper to check if remote and local paths are directories
    """
    if is_remote_path(path):
        return Pathy(path).is_dir()
    return os.path.isdir(path)


def listdir_remote(path):
    """
    Wrapper to list paths in local dirs (alternative to using a glob, I suppose)
    """
    if is_remote_path(path):
        return [blob.name for blob in Pathy(path).ls()]
    return os.listdir(path)


def stat_remote(path):
    """
    Returns FileStatistics struct that contains information about the path
    """
    return Pathy(path).stat()


def size_remote(path):
    """
    Returns file size in bytes.
    """
    if is_remote_path(path):
        return Pathy(path).stat().size
    return os.path.getsize(path)


def glob_remote(pattern, str_cast=True):
    """
    Wrapper that provides globs on local and remote paths like `gs://...`
    Returns list of paths or Pathy objects based on toggle.
    """
    basepath, subpattern = pattern.split('*', 1)
    if is_remote_path(basepath):
        paths = Pathy(basepath).glob('*%s' % subpattern)
        if str_cast:
            return list(map(lambda x: str(x), paths))
        return paths
    else:
        return glob(pattern)

