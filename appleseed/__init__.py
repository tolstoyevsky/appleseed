import gzip
import os
import os.path
import re
import shutil
import tarfile
import urllib.parse
import urllib.request
import uuid

from appleseed import apt_pkg
from appleseed.deb822 import Deb822, Deb822Dict, TagSectionWrapper


ALLOWED_DISTROS = ('alpine', 'debian', 'devuan', 'raspberrypios', 'kali', 'ubuntu', )

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


class MirrorUrlNotSpecified(Exception):
    pass


class SectionNotSpecified(Exception):
    pass


class UnknownDistro(Exception):
    pass


class IndexFile:
    def __init__(self, distro, suite, arch, location, section=None, parent_temp_dir='/tmp'):
        self._parent_temp_dir = parent_temp_dir
        self._temp_dir = None

        if distro not in ALLOWED_DISTROS:
            raise UnknownDistro

        self._url = None
        if os.path.exists(location):
            self._index_file_path = location
        else:
            if not section:
                raise SectionNotSpecified

            if distro == 'alpine':
                uri = f'v{suite}/{section}/{arch}/APKINDEX.tar.gz'
            else:
                uri = f'dists/{suite}/{section}/binary-{arch}/Packages'

            self._index_file_path = None  # will be known later
            self._url = urllib.parse.urljoin(location, uri)

    def get_url(self):
        if not self._url:
            raise MirrorUrlNotSpecified

    def iter_paragraphs(self):
        raise NotImplemented

    def download(self):
        if not self._url:
            raise MirrorUrlNotSpecified

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


class AlpineIndexFile(IndexFile):
    def get_url(self):
        super().get_url()

        yield self._url

    def iter_paragraphs(self):
        index_file_path = self._index_file_path
        if index_file_path.endswith('.tar.gz'):
            with tarfile.open(index_file_path) as infile:
                infile.extractall(path=os.path.dirname(index_file_path))

            index_file_path = index_file_path[:-len('.tat.gz')]

        with open(index_file_path, encoding='utf-8') as infile:
            paragraph = Deb822Dict()
            for line in infile.readlines():
                if line == '\n':
                    yield paragraph
                    paragraph = Deb822Dict()
                    continue

                _empty, key, val = re.split(r'^(\w):', line, flags=re.IGNORECASE)

                try:
                    key = _MAP[key]
                except KeyError:
                    pass

                paragraph[key] = val.strip()


class DebianIndexFile(IndexFile):
    def __init__(self, *args, **kwargs):
        self._debian_packages_ext = {'.gz': gzip.open, }
        self._ext = None  # index file extension (one of _debian_packages_ext)

        super().__init__(*args, **kwargs)

    def get_url(self):
        super().get_url()

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
