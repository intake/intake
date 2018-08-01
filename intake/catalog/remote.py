import msgpack
import requests
import six

from ..source import registry as plugin_registry
from .entry import CatalogEntry
from .utils import expand_defaults, coerce


class RemoteCatalogEntry(CatalogEntry):
    """An entry referring to a remote data definition"""
    def __init__(self, url, auth, *args, **kwargs):
        """

        Parameters
        ----------
        url: str
            HTTP address of the Intake server this entry comes from
        auth: Auth instance
            If there are additional headers to add to calls, this instance will
            provide them
        kwargs: additional keys describing the entry, name, description,
            container,
        """
        self.url = url
        self.auth = auth
        self.args = args
        self.kwargs = kwargs
        self._description = self.kwargs.get('description', "")
        self._metadata = self.kwargs.get('metatata', {})
        getenv = kwargs.pop('getenv', True)
        getshell = kwargs.pop('getshell', True)
        self.http_args = kwargs.pop('http_args', {}).copy()
        if 'headers' not in self.http_args:
            self.http_args['headers'] = {}
        super(RemoteCatalogEntry, self).__init__(getenv=getenv,
                                                 getshell=getshell)

    def describe(self):
        return self.kwargs

    def describe_open(self, **kwargs):
        return {
            'plugin': None,
            'description': self._description,
            'direct_access': False,
            'metadata': self._metadata,
            'args': (self.url, )
        }

    def get(self, **user_parameters):
        for par in self.kwargs['user_parameters']:
            if par['name'] not in user_parameters:
                default = par['default']
                if isinstance(default, six.string_types):
                    default = coerce(par['type'], expand_defaults(
                        par['default'], True, self.getenv, self.getshell))
                user_parameters[par['name']] = default
        entry = self.kwargs

        http_args = self.http_args.copy()
        http_args['headers'] = self.http_args['headers'].copy()
        http_args['headers'].update(self.auth.get_headers())
        return open_remote(
            self.url, entry['name'], container=entry['container'],
            user_parameters=user_parameters, description=entry['description'],
            http_args=http_args
            )


def open_remote(url, entry, container, user_parameters, description, http_args):
    """Create either local direct data source or remote streamed source"""
    from intake.container import container_map
    if url.startswith('intake://'):
        url = url[len('intake://'):]
    payload = dict(action='open',
                   name=entry,
                   parameters=user_parameters,
                   available_plugins=list(plugin_registry.keys()))
    req = requests.post(url, data=msgpack.packb(
        payload, use_bin_type=True), **http_args)
    if req.ok:
        response = msgpack.unpackb(req.content, encoding='utf-8')

        if 'plugin' in response:
            # Direct access
            source = plugin_registry[response['plugin']](**response['args'])
        else:
            # Proxied access
            source = container_map[container](
                url, http_args, parameters=user_parameters,
                name=entry, **response)
        source.description = response.get('description', description)
        return source

    else:
        raise Exception('Server error: %d, %s' % (req.status_code, req.reason))
