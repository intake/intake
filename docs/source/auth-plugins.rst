.. _authplugins:

Authorization Plugins
=====================

Authorization plugins are classes that can be used to customize access permissions to the Intake catalog server.
The Intake server and client communicate over HTTP, so when security is a concern, the *most important* step to take
is to put a TLS-enabled reverse proxy (like ``nginx``) in front of the Intake server to encrypt all communication.

Whether or not the connection is encrypted, the Intake server by default allows all clients to list the full catalog,
and open any of the entries.  For many use cases, this is sufficient, but if the visibility of catalog entries needs
to be limited based on some criteria, a server- (and/or client-) side authorization plugin can be used.

Server Side
-----------

.. highlight: yaml

An Intake server can have exactly one server side plugin enabled at startup.  The plugin is activated using the Intake
configuration file, which lists the class name and the keyword arguments it takes.  For example, the "shared secret"
plugin would be configured this way::

    auth:
      cls: intake.auth.secret.SecretAuth
      kwargs:
        secret: A_SECRET_HASH

This plugin is very simplistic, and exists as a demonstration of how an auth plugin might function for more realistic
scenarios.

.. highlight: python

For more information about configuring the Intake server, see :ref:`configuration`.

The server auth plugin has two methods.  The ``allow_connect()`` method decides whether to allow a client to make any
request to the server at all, and the ``allow_access()`` method decides whether the client is allowed to see a
particular catalog entry in the listing and whether they are allowed to open that data source.  Note that for catalog entries which allow direct access to the data (via network or shared filesystem), the Intake authorization plugins have no impact on the visibility of the underlying data, only the entries in the catalog.

The actual implementation of a plugin is very short.  Here is a simplified version of the shared secret auth plugin::

    class SecretAuth(BaseAuth):
        def __init__(self, secret, key='intake-secret'):
            self.secret = secret
            self.key = key
    
        def allow_connect(self, header):
            try:
                return self.get_case_insensitive(header, self.key, '') \
                            == self.secret
            except:
                return False
    
        def allow_access(self, header, source, catalog):
            try:
                return self.get_case_insensitive(header, self.key, '') \
                            == self.secret
            except:
                return False


The `header` argument is a dictionary of HTTP headers that were present in the client request.  In this case, the
plugin is looking for a special ``intake-secret`` header which contains the shared secret token.  Because HTTP header
names are not case sensitive, the ``BaseAuth`` class provides a helper method ``get_case_insensitive()``, which will
match dictionary keys in a case-insensitive way.

The ``allow_access`` method also takes two additional arguments.  The ``source`` argument is the instance of
``LocalCatalogEntry`` for the data source being checked.  Most commonly auth plugins will want to inspect the
``_metadata`` dictionary for information used to make the authorization decision.  Note that it is entirely up to the
plugin author to decide what sections they want to require in the metadata section.  The ``catalog`` argument is the
instance of ``Catalog`` that contains the catalog entry.  Typically, plugins will want to use information from the
``catalog.metadata`` dictionary to control global defaults, although this is also up to the plugin.


Client Side
-----------

Although server side auth plugins can function entirely independently, some authorization schemes will require the
client to add special HTTP headers for the server to look for.  To facilitate this, the Catalog constructor accepts
an optional ``auth`` parameter with an instance of a client auth plugin class.

The corresponding client plugin for the shared secret use case describe above looks like::

    class SecretClientAuth(BaseClientAuth):
        def __init__(self, secret, key='intake-secret'):
            self.secret = secret
            self.key = key
    
        def get_headers(self):
            return {self.key: self.secret}

It defines a single method, ``get_headers()``, which is called to get a dictionary of additional headers to add to the
HTTP request to the catalog server.  To use this plugin, we would do the following::

    import intake
    from intake.auth.secret import SecretClientAuth

    auth = SecretClientAuth('A_SECRET_HASH')
    cat = intake.Catalog('http://example.com:5000', auth=auth)

Now all requests made to the remote catalog will contain the ``intake-secret`` header.
