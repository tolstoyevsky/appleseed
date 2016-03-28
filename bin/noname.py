import logging
import lzma
import os
import os.path
import sys
import urllib.parse
import urllib.request
import uuid
from sys import stdout

import tornado
from debian import deb822
from pymongo import MongoClient
from tornado.options import define, options

define('arch',
       default='armhf',
       help='')
define('mirror',
       default='http://ftp.debian.org/debian/',
       help='')
define('mongodb_host',
       default='localhost',
       help='')
define('mongodb_port',
       default=27017,
       help='')
define('suite',
       default='jessie',
       help='')
define('temp_dir',
       default='/tmp',
       help='')

LOGGER = logging.getLogger('tornado.application')


def main():
    tornado.options.parse_command_line()

    temp_dir = os.path.join(options.temp_dir, uuid.uuid4())

    packages_path = 'dists/{}/main/binary-{}/Packages.xz'.format(options.suite,
                                                                 options.arch)
    address = urllib.parse.urljoin(options.mirror, packages_path)

    LOGGER.info('Downloading {}'.format(address))
    response = urllib.request.urlopen(address)
    with open('Packages.xz', 'b+w') as f:
        f.write(response.read())

    LOGGER.info('Decompressing Packages.xz')
    with lzma.open('Packages.xz') as f:
        packages_content = f.read()

    with open('Packages', 'b+w') as f:
        f.write(packages_content)

    sys.exit(0)

    collection_name = '{}-{}'.format(options.suite, options.arch)
    db_name = 'cusdeb'

    client = MongoClient(options.mongodb_host, options.mongodb_port)
    db = client[db_name]
    packages_collection = db[collection_name]

    n = 0
    packages_list = []

    with open(packages_file) as f:
        for package in deb822.Packages.iter_paragraphs('Packages'):
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

    LOGGER.info('Inserting the packages metadata into the {} '
                'collection...'.format(collection_name))
    packages_collection.insert_many(packages_list)
    LOGGER.info('Creating indices...')
    packages_collection.ensure_index(
        [('package', 'text')], name='search_index', weights={'package': 100}
    )

if __name__ == "__main__":
    main()
