# copyright 2003-2013 LOGILAB S.A. (Paris, FRANCE), all rights reserved.
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
"""Sqlite RDBMS support

Supported driver: sqlite3
"""
__docformat__ = "restructuredtext en"

from warnings import warn
from os.path import abspath
import re
import inspect

from dateutil import tz, parser

from datetime import datetime
from logilab import database as db


class _Sqlite3Adapter(db.DBAPIAdapter):
    # no type code in sqlite3
    BINARY = "XXX"
    STRING = "XXX"
    DATETIME = "XXX"
    NUMBER = "XXX"
    BOOLEAN = "XXX"
    _module_is_initialized = False

    def __init__(self, native_module, pywrap=False):
        db.DBAPIAdapter.__init__(self, native_module, pywrap)
        self._init_module()

    def _init_module(self):
        """This declares adapters for the sqlite _module_
        and should be called only once.

        We do this here in order to be as lazy as possible
        (these adapters won't be added until we instantiate at
        least one Sqlite3Adapter object).
        """
        if self._module_is_initialized:
            return
        _Sqlite3Adapter._module_is_initialized = True

        # let's register adapters
        sqlite = self._native_module

        # bytea type handling
        from io import BytesIO

        def adapt_bytea(data):
            return data.getvalue()

        sqlite.register_adapter(BytesIO, adapt_bytea)
        try:
            from StringIO import StringIO
        except ImportError:
            pass
        else:
            sqlite.register_adapter(StringIO, adapt_bytea)

        def convert_bytea(data, Binary=sqlite.Binary):
            return Binary(data)

        sqlite.register_converter("bytea", convert_bytea)

        # decimal type handling
        from decimal import Decimal

        def adapt_decimal(data):
            return str(data)

        sqlite.register_adapter(Decimal, adapt_decimal)

        def convert_decimal(data):
            return Decimal(data)

        sqlite.register_converter("decimal", convert_decimal)

        # date/time types handling
        if db.USE_MX_DATETIME:
            from mx.DateTime import DateTimeType, DateTimeDeltaType, strptime

            def adapt_mxdatetime(mxd):
                return mxd.strftime("%Y-%m-%d %H:%M:%S")

            sqlite.register_adapter(DateTimeType, adapt_mxdatetime)

            def adapt_mxdatetimedelta(mxd):
                return mxd.strftime("%H:%M:%S")

            sqlite.register_adapter(DateTimeDeltaType, adapt_mxdatetimedelta)

            def convert_mxdate(ustr):
                return strptime(ustr, "%Y-%m-%d %H:%M:%S")

            sqlite.register_converter("date", convert_mxdate)

            def convert_mxdatetime(ustr):
                return strptime(ustr, "%Y-%m-%d %H:%M:%S")

            sqlite.register_converter("timestamp", convert_mxdatetime)

            def convert_mxtime(ustr):
                try:
                    return strptime(ustr, "%H:%M:%S")
                except Exception:
                    # DateTime used as Time?
                    return strptime(ustr, "%Y-%m-%d %H:%M:%S")

            sqlite.register_converter("time", convert_mxtime)
        # else use datetime.datetime
        else:
            from datetime import time, timedelta

            # datetime.time
            def adapt_time(data):
                return data.strftime("%H:%M:%S")

            sqlite.register_adapter(time, adapt_time)

            def convert_time(data):
                return time(*[int(i) for i in data.split(":")])

            sqlite.register_converter("time", convert_time)

            # datetime.timedelta
            def adapt_timedelta(data):
                """the sign in the result only refers to the number of days.  day
                fractions always indicate a positive offset.  this may seem strange,
                but it is the same that is done by the default __str__ method.  we
                redefine it here anyways (instead of simply doing "str") because we
                do not want any "days," string within the representation.
                """
                days = data.days
                frac = data - timedelta(days)
                return "%d %s" % (data.days, frac)

            sqlite.register_adapter(timedelta, adapt_timedelta)

            def convert_timedelta(data):
                parts = data.split(b" ")
                if len(parts) == 2:
                    daypart, timepart = parts
                    days = int(daypart)
                else:
                    days = 0
                    timepart = parts[-1]
                timepart_full = timepart.split(b".")
                hours, minutes, seconds = map(int, timepart_full[0].split(b":"))
                if len(timepart_full) == 2:
                    microseconds = int(float("0." + timepart_full[1]) * 1000000)
                else:
                    microseconds = 0
                return timedelta(
                    days, hours * 3600 + minutes * 60 + seconds, microseconds
                )

            sqlite.register_converter("interval", convert_timedelta)

            def convert_tzdatetime(data):
                dt = parser.parse(data)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=tz.tzutc())
                return dt

            sqlite.register_converter("tzdatetime", convert_tzdatetime)

    def connect(
        self,
        host="",
        database="",
        user="",
        password="",
        port=None,
        schema=None,
        extra_args=None,
    ):
        """Handles sqlite connection format"""
        sqlite = self._native_module
        if schema is not None:
            warn(
                "schema support is not implemented on sqlite backends, ignoring schema %s"
                % schema
            )

        class Sqlite3Cursor(sqlite.Cursor):
            """cursor adapting usual dict format to pysqlite named format
            in SQL queries
            """

            def _replace_parameters(self, sql, kwargs):
                if isinstance(kwargs, dict):
                    return re.sub(r"%\(([^\)]+)\)s", r":\1", sql)
                return re.sub(r"%s", r"?", sql)

            def execute(self, sql, kwargs=None):
                if kwargs is None:
                    self.__class__.__bases__[0].execute(self, sql)
                else:
                    final_sql = self._replace_parameters(sql, kwargs)
                    self.__class__.__bases__[0].execute(self, final_sql, kwargs)

            def executemany(self, sql, kwargss):
                if not isinstance(kwargss, (list, tuple)):
                    kwargss = tuple(kwargss)
                self.__class__.__bases__[0].executemany(
                    self, self._replace_parameters(sql, kwargss[0]), kwargss
                )

        class Sqlite3CnxWrapper:
            def __init__(self, cnx):
                self._cnx = cnx

            def cursor(self):
                return self._cnx.cursor(Sqlite3Cursor)

            def __getattr__(self, attrname):
                return getattr(self._cnx, attrname)

        # abspath so we can change cwd without breaking further queries on the
        # database
        if database != ":memory:":
            database = abspath(database)
        cnx = sqlite.connect(
            database, detect_types=sqlite.PARSE_DECLTYPES, check_same_thread=False
        )
        return self._wrap_if_needed(Sqlite3CnxWrapper(cnx))

    def _transformation_callback(self, description, encoding="utf-8", binarywrap=None):
        def _transform(value):
            if binarywrap is not None and isinstance(value, self._native_module.Binary):
                return binarywrap(value)
            return value  # no type code support, can't do anything

        return _transform


db._PREFERED_DRIVERS["sqlite"] = ["sqlite3"]
db._ADAPTER_DIRECTORY["sqlite"] = {"sqlite3": _Sqlite3Adapter}


class _SqliteAdvFuncHelper(db._GenericAdvFuncHelper):
    """Generic helper, trying to provide generic way to implement
    specific functionalities from others DBMS

    An exception is raised when the functionality is not emulatable
    """

    backend_name = "sqlite"

    users_support = groups_support = False
    ilike_support = False
    union_parentheses_support = False
    intersect_all_support = False
    alter_column_support = False

    TYPE_CONVERTERS = db._GenericAdvFuncHelper.TYPE_CONVERTERS.copy()

    TYPE_MAPPING = db._GenericAdvFuncHelper.TYPE_MAPPING.copy()
    TYPE_MAPPING.update({"TZTime": "tztime", "TZDatetime": "tzdatetime"})

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
        dbname = dbname or self.dbname
        return [f"gzip -c {dbname} > {backupfile}"]

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
        dbschema=None,
    ):
        return ["zcat {} > {}".format(backupfile, dbname or self.dbname)]

    def sql_create_index(self, table, column, unique=False):
        idx = self._index_name(table, column, unique)
        if unique:
            return f"CREATE UNIQUE INDEX {idx} ON {table}({column});"
        else:
            return f"CREATE INDEX {idx} ON {table}({column});"

    def sql_drop_index(self, table, column, unique=False):
        return "DROP INDEX %s" % self._index_name(table, column, unique)

    def list_tables(self, cursor):
        """return the list of tables of a database"""
        # filter type='table' else we get indices as well
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return [r[0] for r in cursor.fetchall()]

    def list_indices(self, cursor, table=None):
        """return the list of indices of a database, only for the given table if specified"""
        sql = "SELECT name FROM sqlite_master WHERE type='index'"
        if table:
            sql += " AND LOWER(tbl_name)='%s'" % table.lower()
        cursor.execute(sql)
        return [r[0] for r in cursor.fetchall()]

    def sql_regexp_match_expression(self, pattern):
        """pattern matching using regexp"""
        return "REGEXP %s" % pattern


db._ADV_FUNC_HELPER_DIRECTORY["sqlite"] = _SqliteAdvFuncHelper


def init_sqlite_connexion(cnx):
    def _parse_sqlite_date(date):
        if isinstance(date, str):
            date = date.split(".")[0]  # remove microseconds
            try:
                date = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
            except Exception:
                date = datetime.strptime(date, "%Y-%m-%d")
        return date

    def year(date):
        date = _parse_sqlite_date(date)
        return date.year

    def month(date):
        date = _parse_sqlite_date(date)
        return date.month

    def day(date):
        date = _parse_sqlite_date(date)
        return date.day

    def hour(date):
        date = _parse_sqlite_date(date)
        return date.hour

    def minute(date):
        date = _parse_sqlite_date(date)
        return date.minute

    def second(date):
        date = _parse_sqlite_date(date)
        return date.second

    cnx.create_function("MONTH", 1, month)
    cnx.create_function("YEAR", 1, year)
    cnx.create_function("DAY", 1, day)
    cnx.create_function("HOUR", 1, hour)
    cnx.create_function("MINUTE", 1, minute)
    cnx.create_function("SECOND", 1, second)

    from random import random

    cnx.create_function("RANDOM", 0, random)

    def regexp_match(pattern, tested_value):
        return re.search(pattern, tested_value) is not None

    cnx.create_function("REGEXP", 2, regexp_match)


sqlite_hooks = db.SQL_CONNECT_HOOKS.setdefault("sqlite", [])
sqlite_hooks.append(init_sqlite_connexion)


def register_sqlite_pyfunc(pyfunc, nb_params=None, funcname=None):
    if nb_params is None:
        nb_params = len(inspect.getargspec(pyfunc).args)
    if funcname is None:
        funcname = pyfunc.__name__.upper()

    def init_sqlite_connection(cnx):
        cnx.create_function(funcname, nb_params, pyfunc)

    sqlite_hooks = db.SQL_CONNECT_HOOKS.setdefault("sqlite", [])
    sqlite_hooks.append(init_sqlite_connection)
    funcdescr = db.SQL_FUNCTIONS_REGISTRY.get_function(funcname)
    funcdescr.add_support("sqlite")
