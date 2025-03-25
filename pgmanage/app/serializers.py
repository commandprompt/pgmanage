import io
import pickle
import sys

from RestrictedPython.Guards import _safe_names

safe_builtins = _safe_names + ["getattr"]

safe_modules = [
    "app.include.OmniDatabase.MariaDB",
    "app.include.OmniDatabase.PostgreSQL",
    "app.include.OmniDatabase.MySQL",
    "app.include.OmniDatabase.Oracle",
    "app.include.OmniDatabase.SQLite",
    "app.include.Spartacus.Database",
    "app.include.Session",
    "enterprise.include.OmniDatabase.PostgreSQL",
    "pgspecial.dbcommands",
    "pgspecial.iocommands",
    "pgspecial.main",
    "urllib.parse",
    "collections",
    "builtins",
    "datetime",
]


class RestrictedUnpickler(pickle.Unpickler):

    def find_class(self, module, name):
        if module not in safe_modules:
            raise pickle.UnpicklingError("global '%s.%s' is forbidden" % (module, name))

        if module == "builtins" and name not in safe_builtins:
            raise pickle.UnpicklingError("global '%s.%s' is forbidden" % (module, name))
        return getattr(sys.modules[module], name)


def restricted_loads(s):
    """Helper function analogous to pickle.loads()."""
    return RestrictedUnpickler(io.BytesIO(s)).load()


class PickleSerializer:
    """
    Simple wrapper around pickle to be used in signing.dumps()/loads() and
    cache backends.
    """

    def __init__(self, protocol=None):
        self.protocol = pickle.HIGHEST_PROTOCOL if protocol is None else protocol

    def dumps(self, obj):
        return pickle.dumps(obj, self.protocol)

    def loads(self, data):
        return restricted_loads(data)
