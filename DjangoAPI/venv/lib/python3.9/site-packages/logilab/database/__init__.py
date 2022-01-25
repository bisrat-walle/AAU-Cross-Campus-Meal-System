# copyright 2003-2014 LOGILAB S.A. (Paris, FRANCE), all rights reserved.
# contact http://www.logilab.fr/ -- mailto:contact@logilab.fr
#
# This file is part of logilab-database.
#
# logilab-database is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published by the
# Free Software Foundation, either version 2.1 of the License, or (at your
# option) any later version.
#
# logilab-database is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
# for more details.
#
# You should have received a copy of the GNU Lesser General Public License along
# with logilab-database. If not, see <http://www.gnu.org/licenses/>.
"""Wrappers to get actually replaceable DBAPI2 compliant modules and
database connection whatever the database and client lib used.

Currently support:

- postgresql (pgdb, psycopg, psycopg2, pyPgSQL)
- sqlite (pysqlite2, sqlite, sqlite3)

just use the `get_connection` function from this module to get a
wrapped connection.  If multiple drivers for a database are available,
you can control which one you want to use using the
`set_prefered_driver` function.

Additional helpers are also provided for advanced functionalities such
as listing existing users or databases, creating database... Get the
helper for your database using the `get_db_helper` function.
"""

import warnings

__docformat__ = "restructuredtext en"

import threading
import logging
from datetime import datetime, date
from warnings import warn

from logilab.common.modutils import load_module_from_name
from logilab.common.date import todate, todatetime, utcdatetime, utctime
from logilab.common.deprecation import callable_deprecated

from logilab.database.fti import FTIndexerMixIn

_LOGGER = logging.getLogger("logilab.database")

USE_MX_DATETIME = False

_LOAD_MODULES_LOCK = threading.Lock()

_PREFERED_DRIVERS = {}
_ADV_FUNC_HELPER_DIRECTORY = {}


def _ensure_module_loaded(driver):
    if driver in ("postgres", "sqlite"):
        with _LOAD_MODULES_LOCK:
            __import__("logilab.database.%s" % driver)


# main functions ###############################################################


def get_db_helper(driver):
    """returns an advanced function helper for the given driver"""
    _ensure_module_loaded(driver)
    return _ADV_FUNC_HELPER_DIRECTORY[driver]()


def get_dbapi_compliant_module(
    driver, prefered_drivers=None, quiet=False, pywrap=False
):
    """returns a fully dbapi compliant module"""
    _ensure_module_loaded(driver)
    try:
        mod = _ADAPTER_DIRECTORY.adapt(driver, prefered_drivers, pywrap=pywrap)
    except NoAdapterFound as err:
        if not quiet:
            msg = "No Adapter found for %s, returning native module"
            _LOGGER.warning(msg, err.objname)
        mod = err.adapted_obj
    return mod


def get_connection(
    driver="postgres",
    host="",
    database="",
    user="",
    password="",
    port="",
    quiet=False,
    drivers=_PREFERED_DRIVERS,
    pywrap=False,
    schema=None,
    extra_args=None,
):
    """return a db connection according to given arguments

    extra_args is an optional string that is appended to the DSN"""
    _ensure_module_loaded(driver)
    module, modname = _import_driver_module(driver, drivers)
    try:
        adapter = _ADAPTER_DIRECTORY.get_adapter(driver, modname)
    except NoAdapterFound as err:
        if not quiet:
            msg = "No Adapter found for %s, using default one"
            _LOGGER.warning(msg, err.objname)
        adapted_module = DBAPIAdapter(module, pywrap)
    else:
        adapted_module = adapter(module, pywrap)
    if host and not port:
        try:
            host, port = host.split(":", 1)
        except ValueError:
            pass
    if port:
        port = int(port)
    return adapted_module.connect(
        host, database, user, password, port=port, schema=schema, extra_args=extra_args
    )


def set_prefered_driver(driver, module, _drivers=_PREFERED_DRIVERS):
    """sets the preferred driver module for driver
    driver is the name of the db engine (postgresql,...)
    module is the name of the module providing the connect function
    syntax is (params_func, post_process_func_or_None)
    _drivers is a optional dictionary of drivers
    """
    _ensure_module_loaded(driver)
    with _LOAD_MODULES_LOCK:
        try:
            modules = _drivers[driver]
        except KeyError:
            raise UnknownDriver("Unknown driver %s" % driver)
        # Remove module from modules list, and re-insert it in first position
        try:
            modules.remove(module)
        except ValueError:
            raise UnknownDriver("Unknown module %s for %s" % (module, driver))
        modules.insert(0, module)


# types converters #############################################################


def convert_datetime(value):
    # Note: use is __class__ since issubclass(datetime, date)
    if type(value) is date:
        value = todatetime(value)
    return value


def convert_date(value):
    if isinstance(value, datetime):
        value = todate(value)
    return value


def convert_tzdatetime(value):
    # Note: use is __class__ since issubclass(datetime, date)
    if type(value) is date:
        value = todatetime(value)
    elif getattr(value, "tzinfo", None):
        value = utcdatetime(value)
    return value


def convert_tztime(value):
    if getattr(value, "tzinfo", None):
        value = utctime(value)
    return value


# unified db api ###############################################################


class UnknownDriver(Exception):
    """raised when a unknown driver is given to get connection"""


class NoAdapterFound(Exception):
    """Raised when no Adapter to DBAPI was found"""

    def __init__(self, obj, objname=None, protocol="DBAPI"):
        if objname is None:
            objname = obj.__name__
        Exception.__init__(
            self, "Could not adapt %s to protocol %s" % (objname, protocol)
        )
        self.adapted_obj = obj
        self.objname = objname
        self._protocol = protocol


# _AdapterDirectory could be more generic by adding a 'protocol' parameter
# This one would become an adapter for 'DBAPI' protocol
class _AdapterDirectory(dict):
    """A simple dict that registers all adapters"""

    def register_adapter(self, adapter, driver, modname):
        """Registers 'adapter' in directory as adapting 'mod'"""
        try:
            driver_dict = self[driver]
        except KeyError:
            self[driver] = {}
        driver_dict[modname] = adapter

    def adapt(self, driver, prefered_drivers=None, pywrap=False):
        """Returns an dbapi-compliant object based for driver"""
        prefered_drivers = prefered_drivers or _PREFERED_DRIVERS
        module, modname = _import_driver_module(driver, prefered_drivers)
        try:
            return self[driver][modname](module, pywrap=pywrap)
        except KeyError:
            raise NoAdapterFound(obj=module)

    def get_adapter(self, driver, modname):
        try:
            return self[driver][modname]
        except KeyError:
            raise NoAdapterFound(None, modname)


_ADAPTER_DIRECTORY = _AdapterDirectory()
del _AdapterDirectory


def _import_driver_module(driver, drivers, quiet=True):
    """Imports the first module found in 'drivers' for 'driver'

    :rtype: tuple
    :returns: the tuple module_object, module_name where module_object
              is the dbapi module, and modname the module's name
    """
    if driver not in drivers:
        raise UnknownDriver(driver)
    with _LOAD_MODULES_LOCK:
        for modname in drivers[driver]:
            try:
                if not quiet:
                    _LOGGER.info("Trying %s", modname)
                module = load_module_from_name(modname, use_sys=False)
                break
            except ImportError:
                if not quiet:
                    _LOGGER.warning("%s is not available", modname)
                continue
        else:
            raise ImportError("Unable to import a %s module" % driver)
    return module, modname


# base connection and cursor wrappers #####################


class _SimpleConnectionWrapper(object):
    """A simple connection wrapper in python to decorated C-level connections
    with additional attributes
    """

    def __init__(self, cnx):
        """Wraps the original connection object"""
        self._cnx = cnx

    # XXX : Would it work if only __getattr__ was defined
    def cursor(self):
        """Wraps cursor()"""
        return self._cnx.cursor()

    def commit(self):
        """Wraps commit()"""
        return self._cnx.commit()

    def rollback(self):
        """Wraps rollback()"""
        return self._cnx.rollback()

    def close(self):
        """Wraps close()"""
        return self._cnx.close()

    def __getattr__(self, attrname):
        return getattr(self._cnx, attrname)


class PyConnection(_SimpleConnectionWrapper):
    """A simple connection wrapper in python, generating wrapper for cursors as
    well (useful for profiling)
    """

    def __init__(self, cnx):
        """Wraps the original connection object"""
        self._cnx = cnx

    def cursor(self):
        """Wraps cursor()"""
        return PyCursor(self._cnx.cursor())


class PyCursor(object):
    """A simple cursor wrapper in python (useful for profiling)"""

    def __init__(self, cursor):
        self._cursor = cursor

    def close(self):
        """Wraps close()"""
        return self._cursor.close()

    def execute(self, *args, **kwargs):
        """Wraps execute()"""
        return self._cursor.execute(*args, **kwargs)

    def executemany(self, *args, **kwargs):
        """Wraps executemany()"""
        return self._cursor.executemany(*args, **kwargs)

    def fetchone(self, *args, **kwargs):
        """Wraps fetchone()"""
        return self._cursor.fetchone(*args, **kwargs)

    def fetchmany(self, *args, **kwargs):
        """Wraps execute()"""
        return self._cursor.fetchmany(*args, **kwargs)

    def fetchall(self, *args, **kwargs):
        """Wraps fetchall()"""
        return self._cursor.fetchall(*args, **kwargs)

    def __getattr__(self, attrname):
        return getattr(self._cursor, attrname)


# abstract class for dbapi adapters #######################


class DBAPIAdapter(object):
    """Base class for all DBAPI adapters"""

    UNKNOWN = None
    # True if the fetch*() methods return a mutable structure (i.e. not a tuple)
    row_is_mutable = False
    # True if the fetch*() methods return unicode and not binary strings
    returns_unicode = False
    # True is the backend support COPY FROM method
    support_copy_from = False

    def __init__(self, native_module, pywrap=False):
        """
        :type native_module: module
        :param native_module: the database's driver adapted module
        """
        self._native_module = native_module
        self._pywrap = pywrap
        self.logger = _LOGGER
        # optimization: copy type codes from the native module to this instance
        # since the .process_value method may be heavily used
        for typecode in (
            "STRING",
            "BOOLEAN",
            "BINARY",
            "DATETIME",
            "NUMBER",
            "UNKNOWN",
        ):
            try:
                setattr(self, typecode, getattr(self, typecode))
            except AttributeError:
                self.logger.warning("%s adapter has no %s type code", self, typecode)

    def connect(
        self,
        host="",
        database="",
        user="",
        password="",
        port="",
        schema=None,
        extra_args=None,
    ):
        """Wraps the native module connect method"""
        kwargs = {
            "host": host,
            "port": port,
            "database": database,
            "user": user,
            "password": password,
        }
        return self._wrap_if_needed(self._native_module.connect(**kwargs))

    def _wrap_if_needed(self, cnx):
        """Wraps the connection object if self._pywrap is True, and returns it
        If false, returns the original cnx object
        """
        if self._pywrap:
            cnx = PyConnection(cnx)
        return cnx

    def __getattr__(self, attrname):
        return getattr(self._native_module, attrname)

    # @cached ?
    def _transformation_callback(self, description, encoding="utf-8", binarywrap=None):
        typecode = description[1]
        assert typecode is not None, self
        transform = None
        if typecode == self.STRING and not self.returns_unicode:
            transform = lambda v: str(v, encoding, "replace")
        elif typecode == self.BOOLEAN:
            transform = bool
        elif typecode == self.BINARY and binarywrap is not None:
            transform = binarywrap
        elif typecode == self.UNKNOWN:
            # may occurs on constant selection for instance (e.g. SELECT 'hop')
            # with postgresql at least
            transform = (
                lambda v: str(v, encoding, "replace") if isinstance(v, bytes) else v
            )
        return transform

    def process_cursor(self, cursor, encoding, binarywrap=None):
        """return an iterator on results.

        Each record is returned a list (not a tuple) and each element
        of the record is processed :
        - database  strings are all unicode
        - database booleans are python boolean objects
        - if `binarywrap` is provided, it is used to wrap binary data
        """
        cursor.arraysize = 100
        # compute transformations (str->unicode, int->bool, etc.) required for each cell
        transformations = self._transformations(
            cursor.description, encoding, binarywrap
        )
        row_is_mutable = self.row_is_mutable
        while True:
            has_result = False
            for line in cursor:
                result = line if row_is_mutable else list(line)
                # apply required transformations on each cell
                for col, transform in transformations:
                    if result[col] is None:
                        continue
                    result[col] = transform(result[col])
                has_result = True
                yield result

            if not has_result:
                break

    def _transformations(self, description, encoding="utf-8", binarywrap=None):
        """returns the set of required transformations on the resultset

        Transformations are the functions used to convert raw results as
        returned by the dbapi module to standard python objects (e.g.
        unicode, bool, etc.)
        """
        transformations = []
        for i, coldescr in enumerate(description):
            transform = self._transformation_callback(coldescr, encoding, binarywrap)
            if transform is not None:
                transformations.append((i, transform))
        return transformations

    def process_value(self, value, description, encoding="utf-8", binarywrap=None):
        # if the dbapi module isn't supporting type codes, override to return
        # value directly
        transform = self._transformation_callback(description, encoding, binarywrap)
        if transform is not None:
            value = transform(value)
        return value

    def binary_to_str(self, value):
        """turn raw value returned by the db-api module into a python string"""
        return bytes(value)


# advanced database helper #####################################################


class BadQuery(Exception):
    pass


class UnsupportedFunction(BadQuery):
    pass


class UnknownFunction(BadQuery):
    pass


# set of hooks that should be called at connection opening time.
# mostly for sqlite'stored procedures that have to be registered...
SQL_CONNECT_HOOKS = {}
ALL_BACKENDS = object()
# marker for cases where rtype depends on arguments passed to the function
# In that case, functions should implement dynamic_rtype() method
DYNAMIC_RTYPE = object()


class FunctionDescr(object):
    supported_backends = ALL_BACKENDS
    rtype = None  # None <-> returned type should be the same as the first argument
    aggregat = False
    minargs = 1
    maxargs = 1
    name_mapping = {}

    def __init__(self, name=None):
        if name is not None:
            name = name.upper()
        else:
            name = self.__class__.__name__.upper()
        self.name = name

    def add_support(self, backend):
        if self.supported_backends is not ALL_BACKENDS:
            self.supported_backends += (backend,)

    def check_nbargs(cls, nbargs):
        if cls.minargs is not None and nbargs < cls.minargs:
            raise BadQuery("not enough argument for function %s" % cls.__name__)
        if cls.maxargs is not None and nbargs > cls.maxargs:
            raise BadQuery("too many arguments for function %s" % cls.__name__)

    check_nbargs = classmethod(check_nbargs)

    def as_sql(self, backend, args):
        try:
            return getattr(self, "as_sql_%s" % backend)(args)
        except AttributeError:
            funcname = self.name_mapping.get(backend, self.name)
            return "%s(%s)" % (funcname, ", ".join(args))

    def supports(self, backend):
        if (
            self.supported_backends is ALL_BACKENDS
            or backend in self.supported_backends
        ):
            return True
        return False


class AggrFunctionDescr(FunctionDescr):
    aggregat = True
    rtype = None


class MAX(AggrFunctionDescr):
    pass


class MIN(AggrFunctionDescr):
    pass


class SUM(AggrFunctionDescr):
    pass


class COUNT(AggrFunctionDescr):
    rtype = "Int"
    maxargs = 2

    def as_sql(self, backend, args):
        if len(args) == 2:
            # deprecated COUNT DISTINCT form, suppose 2nd argument is true
            warn(
                "[lgdb 1.10] use COUNTDISTINCT instead of COUNT(X, TRUE)",
                DeprecationWarning,
            )
            return "%s(DISTINCT %s)" % (self.name, args[0])
        return "%s(%s)" % (self.name, args[0])


class COUNTDISTINCT(AggrFunctionDescr):
    rtype = "Int"

    def as_sql(self, backend, args):
        return "COUNT(DISTINCT %s)" % args[0]


class AVG(AggrFunctionDescr):
    rtype = "Float"


class ABS(FunctionDescr):
    rtype = "Float"


class UPPER(FunctionDescr):
    rtype = "String"


class LOWER(FunctionDescr):
    rtype = "String"


class IN(FunctionDescr):
    """this is actually a 'keyword' function..."""

    maxargs = None


class LENGTH(FunctionDescr):
    rtype = "Int"


class DATE(FunctionDescr):  # XXX deprecates now we've CAST
    rtype = "Date"


class RANDOM(FunctionDescr):
    rtype = "Float"
    minargs = maxargs = 0
    name_mapping = {"postgres": "RANDOM"}


class SUBSTRING(FunctionDescr):
    rtype = "String"
    minargs = maxargs = 3
    name_mapping = {"postgres": "SUBSTR", "sqlite": "SUBSTR"}


class ExtractDateField(FunctionDescr):
    rtype = "Int"
    minargs = maxargs = 1
    field = None  # YEAR, MONTH, DAY, etc.

    def as_sql_postgres(self, args):
        return "CAST(EXTRACT(%s from %s) AS INTEGER)" % (self.field, ", ".join(args))


class MONTH(ExtractDateField):
    field = "MONTH"


class YEAR(ExtractDateField):
    field = "YEAR"


class DAY(ExtractDateField):
    field = "DAY"


class HOUR(ExtractDateField):
    field = "HOUR"


class MINUTE(ExtractDateField):
    field = "MINUTE"


class SECOND(ExtractDateField):
    field = "SECOND"


class EPOCH(ExtractDateField):
    """Return EPOCH timestamp from a datetime/date ;
    return number of seconds for an interval.
    """

    field = "EPOCH"


class WEEKDAY(FunctionDescr):
    """Return the day of the week represented by the date.

    Sunday == 1, Saturday = 7

    (pick those values since it's recommended by in the ODBC standard)
    """

    rtype = "Int"
    minargs = maxargs = 1

    def as_sql_postgres(self, args):
        # for postgres, sunday is 0
        return "(CAST(EXTRACT(DOW from %s) AS INTEGER) + 1)" % (", ".join(args))


class AT_TZ(FunctionDescr):
    """AT_TZ(TZDatetime, timezone) -> Return a datetime at a given time zone."""

    supported_backends = ("postgres",)
    minargs = maxargs = 2

    def as_sql_postgres(self, args):
        return "%s at time zone %s" % tuple(args)


class CAST(FunctionDescr):
    """usage is CAST(datatype, expression)

    sql-92 standard says (CAST <expr> as <type>)
    """

    minargs = maxargs = 2
    supported_backends = ("postgres", "sqlite")
    rtype = DYNAMIC_RTYPE

    def as_sql(self, backend, args):
        yamstype, varname = args
        db_helper = get_db_helper(backend)
        sqltype = db_helper.TYPE_MAPPING[yamstype]
        return "CAST(%s AS %s)" % (varname, sqltype)


class _FunctionRegistry(object):
    def __init__(self, registry=None):
        if registry is None:
            self.functions = {}
        else:
            self.functions = registry.functions.copy

    def register_function(self, funcdef, funcname=None):
        try:
            if issubclass(funcdef, FunctionDescr):
                funcdef = funcdef()
        except TypeError:  # issubclass is quite strict
            pass
        assert isinstance(funcdef, FunctionDescr)
        funcname = funcname or funcdef.name
        self.functions[funcname.upper()] = funcdef

    def get_function(self, funcname):
        try:
            return self.functions[funcname.upper()]
        except KeyError:
            raise UnknownFunction(funcname)

    def get_backend_function(self, funcname, backend):
        funcdef = self.get_function(funcname)
        if funcdef.supports(backend):
            return funcdef
        raise UnsupportedFunction(funcname)

    def copy(self):
        registry = _FunctionRegistry()
        for funcname, funcdef in self.functions.items():
            registry.register_function(funcdef, funcname=funcname)
        return registry


SQL_FUNCTIONS_REGISTRY = _FunctionRegistry()

for func_class in (
    # aggregate functions
    MIN,
    MAX,
    SUM,
    COUNT,
    COUNTDISTINCT,
    AVG,
    # transformation functions
    ABS,
    RANDOM,
    UPPER,
    LOWER,
    SUBSTRING,
    LENGTH,
    DATE,
    YEAR,
    MONTH,
    DAY,
    HOUR,
    MINUTE,
    SECOND,
    WEEKDAY,
    EPOCH,
    AT_TZ,
    # cast functions
    CAST,
    # keyword function
    IN,
):
    SQL_FUNCTIONS_REGISTRY.register_function(func_class())


def register_function(funcdef):
    """register the function `funcdef` on supported backends"""
    SQL_FUNCTIONS_REGISTRY.register_function(funcdef)


class _TypeMapping(dict):
    def __getitem__(self, key):
        try:
            return dict.__getitem__(self, key)
        except KeyError:
            if key == "TZDatetime":
                return self["Datetime"]
            if key == "TZTime":
                return self["Time"]
            raise

    def copy(self):
        return _TypeMapping(dict.copy(self))


class _GenericAdvFuncHelper(FTIndexerMixIn):
    """Generic helper, trying to provide generic way to implement
    specific functionalities from others DBMS

    An exception is raised when the functionality is not emulatable
    """

    # 'canonical' types are `yams` types. This dictionnary map those types to
    # backend specific types
    TYPE_MAPPING = _TypeMapping(
        {
            "String": "text",
            "SizeConstrainedString": "varchar(%s)",
            "Password": "bytea",
            "Bytes": "bytea",
            "Int": "integer",
            "BigInt": "bigint",
            "Float": "float",
            "Decimal": "decimal",
            "Boolean": "boolean",
            "Date": "date",
            "Time": "time",
            "Datetime": "timestamp",
            "Interval": "interval",
        }
    )

    TYPE_CONVERTERS = {
        "Boolean": bool,
        # XXX needed for sqlite but I don't think it is for other backends
        "Datetime": convert_datetime,
        "Date": convert_date,
        "TZDatetime": convert_tzdatetime,
        "TZTime": convert_tztime,
    }

    # DBMS resources descriptors and accessors
    backend_name = None  # overridden in subclasses ('postgres', 'sqlite', etc.)
    needs_from_clause = False
    union_parentheses_support = True
    intersect_all_support = True
    users_support = True
    groups_support = True
    ilike_support = True
    alter_column_support = True
    case_sensitive = False

    # allow call to [backup|restore]_commands without previous call to
    # record_connection_information but by specifying argument explicitly
    dbname = (
        dbhost
    ) = dbport = dbuser = dbpassword = dbextraargs = dbencoding = dbschema = None

    def __init__(self, encoding="utf-8", _cnx=None):
        self.dbencoding = encoding
        self._cnx = _cnx
        self.dbapi_module = get_dbapi_compliant_module(self.backend_name)
        self.logger = _LOGGER

    def __repr__(self):
        if self.dbname is not None:
            return "<lgdbhelper %s@%s [%s] @%#x>" % (
                self.dbname,
                self.dbhost,
                self.backend_name,
                id(self),
            )
        return super(_GenericAdvFuncHelper, self).__repr__()

    def record_connection_info(
        self,
        dbname,
        dbhost=None,
        dbport=None,
        dbuser=None,
        dbpassword=None,
        dbextraargs=None,
        dbencoding=None,
        dbschema=None,
    ):
        self.dbname = dbname
        self.dbhost = dbhost
        self.dbport = dbport
        self.dbuser = dbuser
        self.dbpasswd = dbpassword
        self.dbextraargs = dbextraargs
        if dbencoding:
            self.dbencoding = dbencoding
        self.dbschema = dbschema

    def get_connection(self, initcnx=True):
        """open and return a connection to the database

        you should first call record_connection_info to set connection
        paramaters.
        """
        if self.dbuser:
            self.logger.info(
                "connecting to %s@%s for user %s",
                self.dbname,
                self.dbhost or "localhost",
                self.dbuser,
            )
        else:
            self.logger.info(
                "connecting to %s@%s", self.dbname, self.dbhost or "localhost"
            )
        cnx = self.dbapi_module.connect(
            self.dbhost,
            self.dbname,
            self.dbuser,
            self.dbpasswd,
            port=self.dbport,
            schema=self.dbschema,
            extra_args=self.dbextraargs,
        )
        if initcnx:
            for hook in SQL_CONNECT_HOOKS.get(self.backend_name, ()):
                hook(cnx)
        return cnx

    def set_connection(self, initcnx=True):
        self._cnx = self.get_connection(initcnx)

    @classmethod
    def function_description(cls, funcname):
        """return the description (`FunctionDescription`) for a SQL function"""
        return SQL_FUNCTIONS_REGISTRY.get_backend_function(funcname, cls.backend_name)

    def func_as_sql(self, funcname, args):
        funcdef = SQL_FUNCTIONS_REGISTRY.get_backend_function(
            funcname, self.backend_name
        )
        return funcdef.as_sql(self.backend_name, args)

    def system_database(self):
        """return the system database for the given driver"""
        raise NotImplementedError("not supported by this DBMS")

    def backup_commands(
        self,
        backupfile,
        keepownership=True,
        dbname=None,
        dbhost=None,
        dbport=None,
        dbuser=None,
        dbschema=None,
    ):
        """Return a list of commands to backup the given database.

        Each command may be given as a list or as a string. In the latter case,
        expected to be used with a subshell (for instance using `os.system(cmd)`
        or `subprocess.call(cmd, shell=True)`
        """
        raise NotImplementedError("not supported by this DBMS")

    def restore_commands(
        self,
        backupfile,
        keepownership=True,
        drop=True,
        dbname=None,
        dbhost=None,
        dbport=None,
        dbuser=None,
        dbencoding=None,
    ):
        """Return a list of commands to restore a backup of the given database.


        Each command may be given as a list or as a string. In the latter case,
        expected to be used with a subshell (for instance using `os.system(cmd)`
        or `subprocess.call(cmd, shell=True)`
        """
        raise NotImplementedError("not supported by this DBMS")

    # helpers to standardize SQL according to the database #####################

    def sql_current_date(self):
        """Return sql for the current date.

        Take care default implementation return date at the beginning of the
        transaction on some backend (eg postgres)
        """
        return "CURRENT_DATE"

    def sql_current_time(self):
        """Return sql for the current time.

        Take care default implementation return time at the beginning of the
        transaction on some backend (eg postgres)
        """
        return "CURRENT_TIME"

    def sql_current_timestamp(self):
        """Return sql for the current date and time.

        Take care default implementation return date and time at the beginning
        of the transaction on some backend (eg postgres)
        """
        return "CURRENT_TIMESTAMP"

    def sql_concat_string(self, lhs, rhs):
        """Return sql for concatenating given arguments (expected to be
        evaluated as string when executing the query).
        """
        return "%s || %s" % (lhs, rhs)

    def sql_regexp_match_expression(self, pattern):
        """pattern matching using regexp"""
        raise NotImplementedError("not supported by this DBMS")

    def sql_create_index(self, table, column, unique=False):
        idx = self._index_name(table, column, unique)
        if unique:
            return "ALTER TABLE %s ADD UNIQUE(%s)" % (table, column)
        else:
            return "CREATE INDEX %s ON %s(%s);" % (idx, table, column)

    def sql_drop_index(self, table, column, unique=False):
        idx = self._index_name(table, column, unique)
        if unique:
            return "ALTER TABLE %s DROP CONSTRAINT %s" % (table, idx)
        else:
            return "DROP INDEX IF EXISTS %s" % idx

    def sqls_create_multicol_unique_index(self, table, columns, indexname=None):
        columns = sorted(columns)
        if indexname is None:
            warn(
                "You should provide an explicit index name else you risk "
                "a silent truncation of the computed index name.",
                DeprecationWarning,
            )
            indexname = "unique_%s_%s_idx" % (table, "_".join(columns))
        sql = "CREATE UNIQUE INDEX %s ON %s(%s);" % (
            indexname.lower(),
            table,
            ",".join(columns),
        )
        return [sql]

    def sqls_drop_multicol_unique_index(self, table, columns, indexname=None):
        columns = sorted(columns)
        if indexname is None:
            warn(
                "You should provide an explicit index name else you risk "
                "a silent truncation of the computed index name.",
                DeprecationWarning,
            )
            indexname = "unique_%s_%s_idx" % (table, "_".join(columns))
        sql = "DROP INDEX IF EXISTS %s;" % (indexname.lower())
        return [sql]

    # sequence protocol

    def sql_create_sequence(self, seq_name):
        return ("CREATE TABLE %s (last INTEGER);" "INSERT INTO %s VALUES (0);") % (
            seq_name,
            seq_name,
        )

    def sql_restart_sequence(self, seq_name, initial_value=1):
        return "UPDATE %s SET last=%s;" % (seq_name, initial_value)

    def sql_sequence_current_state(self, seq_name):
        return "SELECT last FROM %s;" % seq_name

    def sql_drop_sequence(self, seq_name):
        return "DROP TABLE %s;" % seq_name

    def sqls_increment_sequence(self, seq_name):
        return (
            "UPDATE %s SET last=last+1;" % seq_name,
            "SELECT last FROM %s;" % seq_name,
        )

    # /sequence
    # numrange protocol
    # this is like sequence, but allows whole range allocations
    sql_create_numrange = sql_create_sequence
    sql_restart_numrange = sql_restart_sequence
    sql_numrange_current_state = sql_sequence_current_state
    sql_drop_numrange = sql_drop_sequence

    def sqls_increment_numrange(self, seq_name, count=1):
        return (
            "UPDATE %s SET last=last+%s;" % (seq_name, count),
            "SELECT last FROM %s;" % seq_name,
        )

    # /numrange

    def sql_add_limit_offset(self, sql, limit=None, offset=0, orderby=None):
        """
        modify the sql statement to add LIMIT and OFFSET clauses
        (or to emulate them if the backend does not support these SQL extensions)

        `orderby` argument may be needed for some backend
        """
        if limit is None and not offset:
            return sql
        sql = [sql]
        if limit is not None:
            sql.append("LIMIT %d" % limit)
        if offset is not None and offset > 0:
            sql.append("OFFSET %d" % offset)
        return "\n".join(sql)

    def sql_add_order_by(
        self, sql, sortterms, selection, needwrap, has_limit_or_offset
    ):
        """
        add an ORDER BY clause to the SQL query, and wrap the query if necessary
        :sql: the original sql query
        :sortterms: a list of tuples with term, sorting order and nulls policy
        :selection: the selection that must be gathered after ORDER BY
        :needwrap: boolean, True if the query must be wrapped in a subquery
        :has_limit_or_offset: not used (sqlserver helper needs this)
        """
        order_sql_parts = []
        for sortterm in sortterms:
            try:
                # safety belt to keep the old comportment:
                # sortterms used to be a list of string, and not
                # a list of tuples
                term, sorting_order, nulls_policy = sortterm
            except ValueError:
                warnings.warn(
                    (
                        "In logilab-database [2.0], "
                        "sortterms argument must be a list of tuples "
                        "with term, sorting order and nulls policy."
                        "If you are using cubicweb < 3.31, use "
                        "logilab-database < 2.0.0 or upgrade your version "
                        "of cubicweb. This warning will be removed in the future."
                    ),
                    DeprecationWarning,
                )
                order_sql_parts.append(sortterm)
                continue
            nulls_parts = ""
            if nulls_policy == 1:  # NULLS FIRST
                nulls_parts = f"{term} is NULL DESC, "
            elif nulls_policy == 2:  # NULLS LAST
                nulls_parts = f"{term} is NULL, "
            if sorting_order == 1:  # ASC order
                order_sql_parts.append(f"{nulls_parts}{term}")
            else:  # DESC order
                order_sql_parts.append(f"{nulls_parts}{term} DESC")

        sql += "\nORDER BY %s" % ",".join(order_sql_parts)
        if sortterms[0] and needwrap:
            selection = ["T1.C%s" % i for i in range(len(selection))]
            sql = "SELECT %s FROM (%s) AS T1" % (",".join(selection), sql)
        return sql

    def sql_rename_col(self, table, column, newname, coltype, null_allowed):
        return "ALTER TABLE %s RENAME COLUMN %s TO %s" % (table, column, newname)

    def sql_rename_table(self, oldname, newname):
        return "ALTER TABLE %s RENAME TO %s" % (oldname, newname)

    def sql_change_col_type(self, table, column, coltype, null_allowed):
        return "ALTER TABLE %s ALTER COLUMN %s TYPE %s" % (table, column, coltype)

    def sql_set_null_allowed(self, table, column, coltype, null_allowed):
        cmd = null_allowed and "DROP" or "SET"
        return "ALTER TABLE %s ALTER COLUMN %s %s NOT NULL" % (table, column, cmd)

    def temporary_table_name(self, table_name):
        """
        return a temporary table name constructed from the table_name argument
        (e.g. for SQL Server, prepend a '#' to the name)
        Standard implementation returns the argument unchanged.
        """
        return table_name

    def sql_temporary_table(self, table_name, table_schema, drop_on_commit=True):
        table_name = self.temporary_table_name(table_name)
        return "CREATE TEMPORARY TABLE %s (%s);" % (table_name, table_schema)

    def binary_value(self, value):
        """convert a value to a python object known by the driver to
        be mapped to a binary column"""
        return self.dbapi_module.Binary(value)

    def increment_sequence(self, cursor, seq_name):
        for sql in self.sqls_increment_sequence(seq_name):
            cursor.execute(sql)
        return cursor.fetchone()[0]

    def create_user(self, cursor, user, password):
        """create a new database user"""
        if not self.users_support:
            raise NotImplementedError("not supported by this DBMS")
        cursor.execute(
            "CREATE USER %(user)s " "WITH PASSWORD '%(password)s'" % locals()
        )

    def _index_name(self, table, column, unique=False):
        if unique:
            # note: this naming is consistent with indices automatically
            # created by postgres when UNIQUE appears in a table schema
            return "%s_%s_key" % (table.lower(), column.lower())
        else:
            return "%s_%s_idx" % (table.lower(), column.lower())

    def create_index(self, cursor, table, column, unique=False):
        if not self.index_exists(cursor, table, column, unique):
            cursor.execute(self.sql_create_index(table, column, unique))

    def drop_index(self, cursor, table, column, unique=False):
        if self.index_exists(cursor, table, column, unique):
            cursor.execute(self.sql_drop_index(table, column, unique))

    def index_exists(self, cursor, table, column, unique=False):
        idx = self._index_name(table, column, unique)
        return idx in self.list_indices(cursor, table)

    def user_exists(self, cursor, username):
        """return True if a user with the given username exists"""
        return username in self.list_users(cursor)

    def change_col_type(self, cursor, table, column, coltype, null_allowed):
        cursor.execute(self.sql_change_col_type(table, column, coltype, null_allowed))

    def set_null_allowed(self, cursor, table, column, coltype, null_allowed):
        cursor.execute(self.sql_set_null_allowed(table, column, coltype, null_allowed))

    def create_database(self, cursor, dbname, owner=None, dbencoding=None):
        """create a new database"""
        raise NotImplementedError("not supported by this DBMS")

    def create_schema(self, cursor, schema, granted_user=None):
        """create a new database schema"""
        raise NotImplementedError("not supported by this DBMS")

    def drop_schema(self, cursor, schema):
        """drop a database schema"""
        raise NotImplementedError("not supported by this DBMS")

    def list_databases(self):
        """return the list of existing databases"""
        raise NotImplementedError("not supported by this DBMS")

    def list_users(self, cursor):
        """return the list of existing database users"""
        raise NotImplementedError("not supported by this DBMS")

    def list_tables(self, cursor):
        """return the list of tables of a database"""
        raise NotImplementedError("not supported by this DBMS")

    def list_indices(self, cursor, table=None):
        """return the list of indices of a database, only for the given table if specified"""
        raise NotImplementedError("not supported by this DBMS")

    @callable_deprecated("[lgdb 1.10] deprecated method")
    def boolean_value(self, value):
        return int(bool(value))
