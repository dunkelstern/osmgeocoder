import yaml
import pystache
import os

def first(address):
    def _first(content):
        tokens = [token.strip() for token in content.split('||')]
        for t in tokens:
            result = pystache.render(t, address)
            if result.strip() != '':
                return result
        return ''
    return _first

class AddressFormatter():

    def __init__(self, config=None):

        # if no opencage data file is specified in the configuration
        # we fall back to the one included with this package
        if config is None:
            my_dir = os.path.dirname(os.path.abspath(__file__))

            # assume we are in a virtualenv first
            config = os.path.abspath(os.path.join(my_dir, '../../../../share/osmgeocoder/yml/worldwide.yml'))

            # if not found, assume we have been started from a source checkout
            if not os.path.exists(config):
                config = os.path.abspath(os.path.join(my_dir, '../doc/worldwide.yml'))

        with open(config, 'r') as fp:
            self.model = yaml.load(fp)

    def format(self, address, country=None):
        search_key = country.upper() if country is not None else 'default'
        fmt = self.model.get(search_key, None)
        if fmt is None:
            fmt = self.model.get('default', None)
        if fmt is None:
            raise RuntimeError("Configuration file for address formatter has no default value!")

        address['first'] = first(address)
        return pystache.render(fmt['address_template'], address).strip()