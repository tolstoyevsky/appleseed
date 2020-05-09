import re

from appleseed import apt_pkg
from appleseed.deb822 import Deb822, Deb822Dict, TagSectionWrapper


ALPINE = 0

DEBIAN_BASED = 1

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


class IndexFile:
    def __init__(self, sequence, distro):
        self._sequence = sequence

        if distro == ALPINE:
            self.iter_paragraphs = self._go_through_alpine_index_file()
        elif distro == DEBIAN_BASED:
            self.iter_paragraphs = self._go_through_debian_index_file()

    def _go_through_alpine_index_file(self):
        paragraph = Deb822Dict()
        for line in self._sequence.readlines():
            if line == '\n':
                yield paragraph
                paragraph = Deb822Dict()
                continue

            _empty, key, val = re.split(r'^(\w):', line, flags=re.IGNORECASE)

            try:
                key = _MAP[key]
            except KeyError:
                pass

            paragraph[key] = val

    def _go_through_debian_index_file(self):
        parser = apt_pkg.TagFile(self._sequence, bytes=False)
        for section in parser:
            paragraph = Deb822(_parsed=TagSectionWrapper(section))
            if paragraph:
                yield paragraph
