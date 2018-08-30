import os
from setuptools import setup

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
        name='osmgeocoder',
        version='1.1.0',
        description='OpenStreetMap and OpenAddresses.io based geocoder',
        long_description='''
            Python implementation for a OSM Geocoder.

            This geocoder is implemented in PostgreSQL DB functions as much as possible, there is a simple API and an example flask app included.

            You will need PostgreSQL 9.4+ with PostGIS installed as well as some disk space and data-files from OpenStreetMap and (optionally) OpenAddresses.io.

            Data import will be done via [Omniscale's imposm3](https://github.com/omniscale/imposm3) and a supplied python script to import the openaddresses.io data.

            Optionally you can use the [libpostal machine learning address classifier](https://github.com/openvenues/libpostal) to parse addresses supplied as input to the forward geocoder.

            For formatting the addresses from the reverse geocoder the `worldwide.yml` from [OpenCageData address-formatting repository](https://github.com/OpenCageData/address-formatting) is used to format the address according to customs in the country that is been encoded.

            See `README.md` in the [repository](https://github.com/dunkelstern/osmgeocoder) for more information.
        ''',
        long_description_content_type='text/markdown',
        url='https://github.com/dunkelstern/osmgeocoder',
        author='Johannes Schriewer',
        author_email='hallo@dunkelstern.de',
        license='LICENSE.txt',
        include_package_data=True,
        classifiers=[
            'Development Status :: 5 - Production/Stable',
            'Intended Audience :: Developers',
            'License :: OSI Approved :: BSD License',
            'Programming Language :: Python',
            'Programming Language :: Python :: 3',
            'Programming Language :: Python :: 3.4',
            'Programming Language :: Python :: 3.5',
            'Programming Language :: Python :: 3.6',
            'Programming Language :: Python :: 3.7',
            'Operating System :: OS Independent'
        ],
        keywords='osm openstreetmap geocoding geocoder openaddresses.io',
        packages=['osmgeocoder'],
        scripts=[
            'bin/address2coordinate.py',
            'bin/coordinate2address.py',
            'bin/geocoder_service.py',
            'bin/postal_service.py',
            'bin/import_openaddress_data.py',
            'bin/prepare_osm.py'
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
