#!/usr/bin/env python3
import argparse
import sys
import os
import os.path
from urllib.error import HTTPError

from pymongo import MongoClient

from appleseed import ALLOWED_DISTROS, DebianIndexFile


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

    with DebianIndexFile(args.distro, args.suite, args.section, args.arch, args.mirror,
                         args.temp_dir) as index_file:
        for url in index_file.get_url():
            sys.stderr.write(f'Downloading {url}...\n')

            try:
                index_file.download()
                break
            except HTTPError as exc:
                sys.stderr.write(f'Could not download an index file: {exc}\n')
                continue
        else:
            sys.exit(1)

        n = 0
        packages_list = []
        for paragraph in index_file.iter_paragraphs():
            if paragraph['package'] not in BLACKLIST:
                packages_list.append({
                    'package': paragraph['package'],
                    'description': paragraph['description'],
                    'version': paragraph['version'],
                    'size': paragraph['size'],
                })
                n += 1
            sys.stderr.write('\rPackages processed: {}'.format(n))
            sys.stderr.flush()

    sys.stderr.write('\n')

    collection_name = '{}-{}-{}'.format(args.distro, args.suite,
                                        args.arch)
    db_name = 'cusdeb'

    client = MongoClient(args.mongodb_host, args.mongodb_port)
    db = client[db_name]
    packages_collection = db[collection_name]

    sys.stderr.write('{} packages have been processed\n'.format(n))

    sys.stderr.write('Inserting the packages metadata into the {} '
                     'collection...\n'.format(collection_name))
    packages_collection.insert_many(packages_list)
    sys.stderr.write('Creating indices...\n')
    packages_collection.ensure_index(
        [('package', 'text')], name='search_index', weights={'package': 100}
    )

if __name__ == "__main__":
    main()
