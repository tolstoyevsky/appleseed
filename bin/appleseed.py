#!/usr/bin/env python3
import argparse
import shutil
import logging
import lzma
import os
import os.path
import urllib.parse
import urllib.request
import uuid
from sys import stdout

from debian import deb822
from pymongo import MongoClient


BLACKLIST = [
    # The following packages are very big
    '0ad', '0ad-data', '0ad-data-common', '0ad-dbg',
    'flightgear', 'flightgear-data-ai', 'flightgear-data-aircrafts',
    'flightgear-data-all', 'flightgear-data-base', 'flightgear-data-models',
    'flightgear-dbgsym', 'flightgear-phi',
]

LOGGER = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--arch', default='armhf', help='The architecture of the distribution')
    parser.add_argument('--distro', default='raspbian',
                        help='The distribution name (e.g. Debian, Raspbian, etc.)')
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

    temp_dir = os.path.join(args.temp_dir, str(uuid.uuid4()))
    os.mkdir(temp_dir, mode=0o700)

    packages_file = os.path.join(temp_dir, 'Packages')
    packages_xz_file = os.path.join(temp_dir, 'Packages.xz')

    address = urllib.parse.urljoin(args.mirror, 'dists/{}/{}/binary-{}/'
                                                   'Packages.xz'.
                                   format(args.suite, args.section, args.arch))

    LOGGER.info('Downloading {}...'.format(address))
    response = urllib.request.urlopen(address)
    with open(packages_xz_file, 'b+w') as f:
        f.write(response.read())

    LOGGER.info('Decompressing Packages.xz...')
    with lzma.open(packages_xz_file) as f:
        packages_content = f.read()

    with open(packages_file, 'b+w') as f:
        f.write(packages_content)

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
    with open(packages_file, encoding='utf-8') as f:
        for package in deb822.Packages.iter_paragraphs(f):
            if package['package'] not in BLACKLIST:
                packages_list.append({
                    'package': package['package'],
                    'dependencies': package.get('depends', ''),
                    'description': package['description'],
                    'version': package['version'],
                    'size': package['size'],
                    'type': ''
                })
                n += 1
            stdout.write('\rPackages processed: {}'.format(n))
            stdout.flush()

    stdout.write('\n')

    LOGGER.info('{} packages have been processed'.format(n))

    # From then on we don't need the temporary directory.
    shutil.rmtree(temp_dir)

    LOGGER.info('Inserting the packages metadata into the {} '
                'collection...'.format(collection_name))
    packages_collection.insert_many(packages_list)
    LOGGER.info('Creating indices...')
    packages_collection.ensure_index(
        [('package', 'text')], name='search_index', weights={'package': 100}
    )

if __name__ == "__main__":
    main()
