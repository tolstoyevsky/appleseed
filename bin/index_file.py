#!/usr/bin/env python3
import argparse
import gzip
import shutil
import sys
import lzma
import os
import os.path
import urllib.parse
import urllib.request
import uuid
from urllib.error import HTTPError

from pymongo import MongoClient

from appleseed import IndexFile, DEBIAN_BASED

ALLOWED_DISTROS = ('debian', 'devuan', 'raspbian', 'kali', 'ubuntu', )

PACKAGES_EXTENSIONS = {'.xz': lzma.open, '.gz': gzip.open, }

BLACKLIST = [
    # The following packages are very big
    '0ad', '0ad-data', '0ad-data-common', '0ad-dbg',
    'flightgear', 'flightgear-data-ai', 'flightgear-data-aircrafts',
    'flightgear-data-all', 'flightgear-data-base', 'flightgear-data-models',
    'flightgear-dbgsym', 'flightgear-phi',
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--arch', default='armhf', help='The architecture of the distribution')
    parser.add_argument('--distro', default='raspbian',
                        help=f'The distribution name. The option takes the following values: '
                             f'{", ".join(ALLOWED_DISTROS)}')
    parser.add_argument('--mirror', default='http://archive.raspbian.org/raspbian/',
                        help='The address of the repository where the packages of the '
                             'distribution can be found')
    parser.add_argument('--mongodb-host', default='127.0.0.1', help='The MongoDB host')
    parser.add_argument('--mongodb-port', type=int, default=27017,
                        help='The MongoDB port the server listens on')
    parser.add_argument('--section', default='main',
                        help='The section name of the distribution (e.g. main, universe, etc.)')
    parser.add_argument('--suite', default='buster',
                        help='The distribution code name of version (e.g. Buster, Focal, etc.)')
    parser.add_argument('--temp-dir', default='/tmp',
                        help='A temporary directory where the Packages.xz and Packages files will '
                             'be located')

    args = parser.parse_args()

    args.mirror = os.path.join(args.mirror, '')  # add trailing slash

    if args.distro in ALLOWED_DISTROS:
        if args.distro == 'kali':
            args.suite = 'kali-rolling'
            sys.stderr.write(f'--suite is overridden. Now it is {args.suite}\n')

        address = urllib.parse.urljoin(
            args.mirror,
            f'dists/{args.suite}/{args.section}/binary-{args.arch}/Packages'
        )
    else:
        sys.stderr.write('Unknown distribution name\n')
        sys.exit(1)

    temp_dir = os.path.join(args.temp_dir, str(uuid.uuid4()))
    os.mkdir(temp_dir, mode=0o700)

    packages_file = os.path.join(temp_dir, 'Packages')

    for ext in list(PACKAGES_EXTENSIONS.keys()) + ['']:
        sys.stderr.write('Downloading {}...\n'.format(address + ext))
        try:
            response = urllib.request.urlopen(address + ext)
            break
        except HTTPError as exc:
            sys.stderr.write(f'Could not download an index file: {exc}\n')
            continue
    else:
        sys.exit(1)

    packages_file_ext = packages_file + ext
    with open(packages_file_ext, 'b+w') as outfile:
        outfile.write(response.read())

    if ext:
        func = PACKAGES_EXTENSIONS[ext]

        sys.stderr.write(f'Decompressing {packages_file_ext}...\n')
        with func(packages_file_ext) as infile:
            packages_content = infile.read()

        with open(packages_file, 'b+w') as outfile:
            outfile.write(packages_content)

    collection_name = '{}-{}-{}'.format(args.distro, args.suite,
                                        args.arch)
    db_name = 'cusdeb'

    client = MongoClient(args.mongodb_host, args.mongodb_port)
    db = client[db_name]
    packages_collection = db[collection_name]

    n = 0
    packages_list = []
    # If the encoding parameter isn't specified and the program is running in a
    # docker container, the interpreter will throw the UnicodeDecodeError
    # exception, executing the next line.
    with open(packages_file, encoding='utf-8') as infile:
        index_file = IndexFile(infile, DEBIAN_BASED)
        for paragraph in index_file.iter_paragraphs:
            if paragraph['package'] not in BLACKLIST:
                packages_list.append({
                    'package': paragraph['package'],
                    'dependencies': paragraph.get('depends', ''),
                    'description': paragraph['description'],
                    'version': paragraph['version'],
                    'size': paragraph['size'],
                    'type': ''
                })
                n += 1
            sys.stderr.write('\rPackages processed: {}'.format(n))
            sys.stderr.flush()

    sys.stderr.write('\n')

    sys.stderr.write('{} packages have been processed\n'.format(n))

    # From then on we don't need the temporary directory.
    shutil.rmtree(temp_dir)

    sys.stderr.write('Inserting the packages metadata into the {} '
                     'collection...\n'.format(collection_name))
    packages_collection.insert_many(packages_list)
    sys.stderr.write('Creating indices...\n')
    packages_collection.ensure_index(
        [('package', 'text')], name='search_index', weights={'package': 100}
    )

if __name__ == "__main__":
    main()
