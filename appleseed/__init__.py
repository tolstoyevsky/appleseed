import gzip
import lzma
import os
import os.path
import re
import shutil
import urllib.parse
import urllib.request
import uuid

from appleseed import apt_pkg
from appleseed.deb822 import Deb822, Deb822Dict, TagSectionWrapper


ALLOWED_DISTROS = ('debian', 'devuan', 'raspbian', 'kali', 'ubuntu', )

_MAP = {
    'A': 'Architecture',
    'I': 'Installed-Size',
    'm': 'Maintainer',
    'P': 'Package',
    'S': 'Size',
    'T': 'Description',
    'U': 'Homepage',
    'V': 'Version',
}


class UnknownDistro(Exception):
    pass


class IndexFile:
    def __init__(self, distro, suite, section, arch, mirror, parent_temp_dir='/tmp'):
        self._parent_temp_dir = parent_temp_dir
        self._temp_dir = None

        if distro not in ALLOWED_DISTROS:
            raise UnknownDistro

        uri = f'dists/{suite}/{section}/binary-{arch}/Packages'

        self._index_file_path = None  # will be known later
        self._url = urllib.parse.urljoin(mirror, uri)

    def get_url(self):
        raise NotImplemented

    def iter_paragraphs(self):
        raise NotImplemented

    def download(self):
        self._index_file_path = os.path.join(self._temp_dir, os.path.basename(self._url))
        with urllib.request.urlopen(self._url) as response:
            with open(self._index_file_path, 'b+w') as outfile:
                outfile.write(response.read())

    def __enter__(self):
        self._temp_dir = os.path.join(self._parent_temp_dir, str(uuid.uuid4()))
        os.mkdir(self._temp_dir, mode=0o700)

        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        shutil.rmtree(self._temp_dir)


class DebianIndexFile(IndexFile):
    def __init__(self, *args, **kwargs):
        self._debian_packages_ext = {'.xz': lzma.open, '.gz': gzip.open, }
        self._ext = None  # index file extension (one of _debian_packages_ext)

        super().__init__(*args, **kwargs)

    def get_url(self):
        url_bck = self._url
        for self._ext in list(self._debian_packages_ext.keys()) + ['']:
            # An empty list means an uncompressed index file.

            self._url = url_bck + self._ext
            yield self._url

    def iter_paragraphs(self):
        func = self._debian_packages_ext[self._ext] if self._ext else open
        kwargs = {'encoding': 'utf-8'} if func == open else {}
        with func(self._index_file_path, **kwargs) as infile:
            parser = apt_pkg.TagFile(infile, bytes=False)
            for section in parser:
                paragraph = Deb822(_parsed=TagSectionWrapper(section))
                if paragraph:
                    yield paragraph
