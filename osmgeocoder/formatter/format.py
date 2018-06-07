import yaml
import pystache

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

    def __init__(self, config_file):
        with open(config_file, 'r') as fp:
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
