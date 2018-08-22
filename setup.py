import os
from setuptools import setup

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

# load readme
with open(os.path.join(os.path.dirname(__file__), 'README.md')) as readme:
    README = readme.read()

setup(
        name='osmgeocoder',
        version='1.0.0',
        description='OpenStreetMap based geocoder',
        long_description=README,
        long_description_content_type='text/markdown',
        url='https://github.com/dunkelstern/osmgeocoder',
        author='Johannes Schriewer',
        author_email='hallo@dunkelstern.de',
        license='LICENSE.txt',
        include_package_data=True,
        classifiers=[
            'Development Status :: 5 - Production/Stable',
            'Intended Audience :: Developers',
            'Topic :: Software Development :: Testing',
            'License :: OSI Approved :: BSD License',
            'Programming Language :: Python',
            'Programming Language :: Python :: 3',
            'Programming Language :: Python :: 3.4',
            'Programming Language :: Python :: 3.5',
            'Programming Language :: Python :: 3.6',
            'Operating System :: OS Independent'
        ],
        keywords='osm openstreetmap geocoding geocoder',
        packages=['osmgeocoder'],
        scripts=[
            'bin/address2coordinate.py',
            'bin/coordinate2address.py',
            'bin/geocoder_service.py',
            'bin/postal_service.py',
        ],
        data_files = [
            ('share/osmgeocoder/sql', [
                'sql/001-create_trigram_indices.sql',
                'sql/002-wordlist_for_text_prediction',
                'sql/003-text_prediction.sql'
            ]),
            ('share/osmgeocoder/yml', [ 
                'doc/imposm_mapping.yml', 
                'data/worldwide.yml'
            ]),
            ('share/doc/osmgeocoder', [ 'doc/config-example.json'])
        ],
        install_requires=[
            'psycopg2-binary >= 2.7',
            'pyproj >= 1.9',
            'Shapely >= 1.6',
            'requests >= 2.18',
            'PyYAML >= 3.12',
            'pystache >= 0.5'
        ],
        dependency_links=[
        ]
)
