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
"""Postgres RDBMS support

Supported drivers, in order of preference:
- psycopg2
- psycopg2ct

Full-text search based on the tsearch2 extension from the openfts project
(see http://openfts.sourceforge.net/)

Warning: you will need to run the tsearch2.sql script with super user privileges
on the database.
"""
import warnings

__docformat__ = "restructuredtext en"

from os.path import join, dirname, isfile
from warnings import warn

from logilab.common.deprecation import callable_deprecated

from logilab import database as db
from logilab.database.fti import normalize_words, tokenize_query


TSEARCH_SCHEMA_PATH = (
    "/usr/share/postgresql/?.?/contrib/tsearch2.sql",  # current debian
    "/usr/lib/postgresql/share/contrib/tsearch2.sql",
    "/usr/share/postgresql/contrib/tsearch2.sql",
    "/usr/lib/postgresql-?.?/share/contrib/tsearch2.sql",
    "/usr/share/postgresql-?.?/contrib/tsearch2.sql",
    join(dirname(__file__), "tsearch2.sql"),
    "tsearch2.sql",
)


class _Psycopg2Adapter(db.DBAPIAdapter):
    """Simple Psycopg2 Adapter to DBAPI (cnx_string differs from classical ones)

    It provides basic support for postgresql schemas :
    cf. http://www.postgresql.org/docs/current/static/ddl-schemas.html
    """

    # not defined in psycopg2.extensions
    # "select typname from pg_type where oid=705";
    UNKNOWN = 705
    returns_unicode = True
    # True is the backend support COPY FROM method
    support_copy_from = True

    def __init__(self, native_module, pywrap=False):
        from psycopg2 import extensions

        extensions.register_type(extensions.UNICODE)
        try:
            unicodearray = extensions.UNICODEARRAY
        except AttributeError:
            from psycopg2 import _psycopg

            unicodearray = _psycopg.UNICODEARRAY
        extensions.register_type(unicodearray)
        self.BOOLEAN = extensions.BOOLEAN
        db.DBAPIAdapter.__init__(self, native_module, pywrap)
        self._init_psycopg2()

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
        """Handles psycopg connection format"""
        args = {}
        if host:
            args.setdefault("host", host)
        if database:
            args.setdefault("dbname", database)
        if user:
            args.setdefault("user", user)
        if port:
            args.setdefault("port", port)
        if password:
            args.setdefault("password", password)
        cnx_string = " ".join("%s=%s" % item for item in args.items())
        if extra_args is not None:
            cnx_string += " " + extra_args
        cnx = self._native_module.connect(cnx_string)
        cnx.set_isolation_level(1)
        self.set_search_path(cnx, schema)
        return self._wrap_if_needed(cnx)

    def _schema_exists(self, cursor, schema):
        cursor.execute(
            "SELECT nspname FROM pg_namespace WHERE nspname=%(s)s", {"s": schema}
        )
        return cursor.fetchone() is not None

    def set_search_path(self, cnx, schema):
        if schema:
            cursor = cnx.cursor()
            if not self._schema_exists(cursor, schema):
                warn(
                    "%s schema doesn't exist, search path can't be set" % schema,
                    UserWarning,
                )
                return
            cursor.execute("SHOW search_path")
            schemas = cursor.fetchone()[0].split(",")
            if schema not in schemas:
                schemas.insert(0, schema)
            else:
                schemas.pop(schemas.index(schema))
                schemas.insert(0, schema)
            cursor.execute("SET search_path TO %s;" % ",".join(schemas))
            cursor.close()

    def _init_psycopg2(self):
        """initialize psycopg2 to use mx.DateTime for date and timestamps
        instead for datetime.datetime"""
        psycopg2 = self._native_module
        if hasattr(psycopg2, "_lc_initialized"):
            return
        psycopg2._lc_initialized = 1
        # use mxDateTime instead of datetime if available
        if db.USE_MX_DATETIME:
            from psycopg2 import extensions

            extensions.register_type(psycopg2._psycopg.MXDATETIME)
            extensions.register_type(psycopg2._psycopg.MXINTERVAL)
            extensions.register_type(psycopg2._psycopg.MXDATE)
            extensions.register_type(psycopg2._psycopg.MXTIME)
            # StringIO/cStringIO adaptation
            # XXX (syt) todo, see my december discussion on the psycopg2 list
            # for a working solution
            # def adapt_stringio(stringio):
            #    self.logger.info('ADAPTING %s', stringio)
            #    return psycopg2.Binary(stringio.getvalue())
            # import StringIO
            # extensions.register_adapter(StringIO.StringIO, adapt_stringio)
            # import cStringIO
            # extensions.register_adapter(cStringIO.StringIO, adapt_stringio)


class _Psycopg2CtypesAdapter(_Psycopg2Adapter):
    """psycopg2-ctypes adapter

    cf. https://github.com/mvantellingen/psycopg2-ctypes
    """

    def __init__(self, native_module, pywrap=False):
        # install psycopg2 compatibility
        from psycopg2ct import compat

        compat.register()
        _Psycopg2Adapter.__init__(self, native_module, pywrap)


class _Psycopg2CffiAdapter(_Psycopg2Adapter):
    """psycopg2cffi adapter

    cf. https://pypi.python.org/pypi/psycopg2cffi
    """

    def __init__(self, native_module, pywrap=False):
        # install psycopg2 compatibility
        from psycopg2cffi import compat

        compat.register()
        _Psycopg2Adapter.__init__(self, native_module, pywrap)


db._PREFERED_DRIVERS["postgres"] = [
    # 'logilab.database._pyodbcwrap',
    "psycopg2",
    "psycopg2ct",
    "psycopg2cffi",
]
db._ADAPTER_DIRECTORY["postgres"] = {
    "psycopg2": _Psycopg2Adapter,
    "psycopg2ct": _Psycopg2CtypesAdapter,
    "psycopg2cffi": _Psycopg2CffiAdapter,
}


class _PGAdvFuncHelper(db._GenericAdvFuncHelper):
    """Postgres helper, taking advantage of postgres SEQUENCE support"""

    backend_name = "postgres"
    TYPE_MAPPING = db._GenericAdvFuncHelper.TYPE_MAPPING.copy()
    TYPE_MAPPING.update(
        {"TZTime": "time with time zone", "TZDatetime": "timestamp with time zone"}
    )
    TYPE_CONVERTERS = db._GenericAdvFuncHelper.TYPE_CONVERTERS.copy()

    def pgdbcmd(self, cmd, dbhost, dbport, dbuser, dbschema, *args):
        cmd = [cmd]
        cmd += args
        if dbhost or self.dbhost:
            cmd.append("--host=%s" % (dbhost or self.dbhost))
        if dbport or self.dbport:
            cmd.append("--port=%s" % (dbport or self.dbport))
        if dbuser or self.dbuser:
            cmd.append("--username=%s" % (dbuser or self.dbuser))
        if dbschema or self.dbschema:
            cmd.append("--schema=%s" % (dbschema or self.dbschema))
        return cmd

    def system_database(self):
        """return the system database for the given driver"""
        return "template1"

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
        cmd = self.pgdbcmd("pg_dump", dbhost, dbport, dbuser, dbschema, "-Fc")
        if not keepownership:
            cmd.append("--no-owner")
        cmd.append("--file")
        cmd.append(backupfile)
        cmd.append(dbname or self.dbname)
        return [cmd]

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
        # XXX what about dbschema ?
        dbname = dbname or self.dbname
        cmds = []
        if drop:
            cmd = self.pgdbcmd("dropdb", dbhost, dbport, dbuser, None)
            cmd.append(dbname)
            cmds.append(cmd)
        cmd = self.pgdbcmd(
            "createdb",
            dbhost,
            dbport,
            dbuser,
            None,
            "-T",
            "template0",
            "-E",
            dbencoding or self.dbencoding,
        )
        cmd.append(dbname)
        cmds.append(cmd)
        cmd = self.pgdbcmd("pg_restore", dbhost, dbport, dbuser, None, "-Fc")
        cmd.append("--dbname")
        cmd.append(dbname)
        if not keepownership:
            cmd.append("--no-owner")
        cmd.append(backupfile)
        cmds.append(cmd)
        return cmds

    def sql_current_date(self):
        return "CAST(clock_timestamp() AS DATE)"

    def sql_current_time(self):
        return "CAST(clock_timestamp() AS TIME)"

    def sql_current_timestamp(self):
        return "clock_timestamp()"

    def sql_regexp_match_expression(self, pattern):
        """pattern matching using regexp"""
        return "~ %s" % (pattern)

    def sql_create_sequence(self, seq_name):
        return "CREATE SEQUENCE %s;" % seq_name

    def sql_restart_sequence(self, seq_name, initial_value=1):
        return "ALTER SEQUENCE %s RESTART WITH %s;" % (seq_name, initial_value)

    def sql_sequence_current_state(self, seq_name):
        return "SELECT last_value FROM %s;" % seq_name

    def sql_drop_sequence(self, seq_name):
        return "DROP SEQUENCE %s;" % seq_name

    def sqls_increment_sequence(self, seq_name):
        return ("SELECT nextval('%s');" % seq_name,)

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
                return super().sql_add_order_by(
                    sql, sortterms, selection, needwrap, has_limit_or_offset
                )
            nulls_parts = ""
            if nulls_policy == 1:  # NULLS FIRST
                nulls_parts = " NULLS FIRST"
            elif nulls_policy == 2:  # NULLS LAST
                nulls_parts = " NULLS LAST"
            if sorting_order == 1:  # ASC order
                order_sql_parts.append(f"{term}{nulls_parts}")
            else:  # DESC order
                order_sql_parts.append(f"{term} DESC{nulls_parts}")

        sql += "\nORDER BY %s" % ",".join(order_sql_parts)
        if sortterms[0] and needwrap:
            selection = ["T1.C%s" % i for i in range(len(selection))]
            sql = "SELECT %s FROM (%s) AS T1" % (",".join(selection), sql)
        return sql

    def sql_temporary_table(self, table_name, table_schema, drop_on_commit=True):
        if not drop_on_commit:
            return "CREATE TEMPORARY TABLE %s (%s);" % (table_name, table_schema)
        return "CREATE TEMPORARY TABLE %s (%s) ON COMMIT DROP;" % (
            table_name,
            table_schema,
        )

    def create_database(
        self, cursor, dbname, owner=None, dbencoding=None, template=None
    ):
        """create a new database"""
        sql = 'CREATE DATABASE "%(dbname)s"'
        if owner:
            sql += ' WITH OWNER="%(owner)s"'
        if template:
            sql += ' TEMPLATE "%(template)s"'
        dbencoding = dbencoding or self.dbencoding
        if dbencoding:
            sql += " ENCODING='%(dbencoding)s'"
        cursor.execute(sql % locals())

    def create_schema(self, cursor, schema, granted_user=None):
        """create a new database schema"""
        sql = "CREATE SCHEMA %s" % schema
        if granted_user is not None:
            sql += " AUTHORIZATION %s" % granted_user
        cursor.execute(sql)

    def drop_schema(self, cursor, schema):
        """drop a database schema"""
        cursor.execute("DROP SCHEMA %s CASCADE" % schema)

    def create_language(self, cursor, extlang):
        """postgres specific method to install a procedural language on a database"""
        # make sure plpythonu is not directly in template1
        cursor.execute("SELECT * FROM pg_language WHERE lanname='%s';" % extlang)
        if cursor.fetchall():
            self.logger.warning("%s language already installed", extlang)
        else:
            cursor.execute("CREATE LANGUAGE %s" % extlang)
            self.logger.info("%s language installed", extlang)

    def list_users(self, cursor):
        """return the list of existing database users"""
        cursor.execute("SELECT usename FROM pg_user")
        return [r[0] for r in cursor.fetchall()]

    def list_databases(self, cursor):
        """return the list of existing databases"""
        cursor.execute("SELECT datname FROM pg_database")
        return [r[0] for r in cursor.fetchall()]

    def list_tables(self, cursor, schema=None):
        """return the list of tables of a database"""
        schema = schema or self.dbschema
        sql = "SELECT tablename FROM pg_tables"
        if schema:
            sql += " WHERE schemaname=%(s)s"
        cursor.execute(sql, {"s": schema})
        return [r[0] for r in cursor.fetchall()]

    def list_indices(self, cursor, table=None):
        """return the list of indices of a database, only for the given table if specified"""
        sql = "SELECT indexname FROM pg_indexes"
        restrictions = []
        if table:
            table = table.lower()
            restrictions.append("LOWER(tablename)=%(table)s")
        if self.dbschema:
            restrictions.append("schemaname=%(s)s")
        if restrictions:
            sql += " WHERE %s" % " AND ".join(restrictions)
        cursor.execute(sql, {"s": self.dbschema, "table": table})
        return [r[0] for r in cursor.fetchall()]

    # full-text search customization ###########################################

    fti_table = "appears"
    fti_need_distinct = False
    config = "default"
    max_indexed = 500_000  # 500KB, avoid "string is too long for tsvector"

    def has_fti_table(self, cursor):
        if super(_PGAdvFuncHelper, self).has_fti_table(cursor):
            self.config = "simple"
        return self.fti_table in self.list_tables(cursor)

    def cursor_index_object(self, uid, obj, cursor):
        """Index an object, using the db pointed by the given cursor."""
        ctx = {"config": self.config, "uid": int(uid)}
        tsvectors, size, oversized = [], 0, False
        # sort for test predictability
        for (weight, words) in sorted(obj.get_words().items()):
            words = normalize_words(words)
            for i, word in enumerate(words):
                size += len(word) + 1
                if size > self.max_indexed:
                    words = words[:i]
                    oversized = True
                    break
            if words:
                tsvectors.append(
                    "setweight(to_tsvector(%%(config)s, "
                    "%%(wrds_%(w)s)s), '%(w)s')" % {"w": weight}
                )
                ctx["wrds_%s" % weight] = " ".join(words)
            if oversized:
                break
        if tsvectors:
            cursor.execute(
                "INSERT INTO appears(uid, words, weight) "
                "VALUES (%%(uid)s, %s, %s);"
                % ("||".join(tsvectors), obj.entity_weight),
                ctx,
            )

    def _fti_query_to_tsquery_words(self, querystr):
        if isinstance(querystr, bytes):
            querystr = querystr.decode(self.dbencoding)
        words = normalize_words(tokenize_query(querystr))
        # XXX replace '%' since it makes tsearch fail, dunno why yet, should
        # be properly fixed
        return "&".join(words).replace("*", ":*").replace("%", "")

    def fulltext_search(self, querystr, cursor=None):
        """Execute a full text query and return a list of 2-uple (rating, uid)."""
        cursor = cursor or self._cnx.cursor()
        cursor.execute(
            "SELECT 1, uid FROM appears "
            "WHERE words @@ to_tsquery(%(config)s, %(words)s)",
            {
                "config": self.config,
                "words": self._fti_query_to_tsquery_words(querystr),
            },
        )
        return cursor.fetchall()

    def fti_restriction_sql(self, tablename, querystr, jointo=None, not_=False):
        """Execute a full text query and return a list of 2-uple (rating, uid)."""
        searched = self._fti_query_to_tsquery_words(querystr)
        sql = "%s.words @@ to_tsquery('%s', '%s')" % (tablename, self.config, searched)
        if not_:
            sql = "NOT (%s)" % sql
        if jointo is None:
            return sql
        return "%s AND %s.uid=%s" % (sql, tablename, jointo)

    def fti_rank_order(self, tablename, querystr):
        """Execute a full text query and return a list of 2-uple (rating, uid)."""
        searched = self._fti_query_to_tsquery_words(querystr)
        return "ts_rank(%s.words, to_tsquery('%s', '%s'))*%s.weight" % (
            tablename,
            self.config,
            searched,
            tablename,
        )

    # XXX not needed with postgres >= 8.3 right?
    def find_tsearch2_schema(self):
        """Looks up for tsearch2.sql in a list of default paths."""
        import glob

        for path in TSEARCH_SCHEMA_PATH:
            for fullpath in glob.glob(path):
                if isfile(fullpath):
                    # tsearch2.sql found !
                    return fullpath
        raise RuntimeError("can't find tsearch2.sql")

    def init_fti_extensions(self, cursor, owner=None):
        """If necessary, install extensions at database creation time.

        For postgres, install tsearch2 if not installed by the template.
        """
        tstables = []
        for table in self.list_tables(cursor, schema="pg_catalog"):
            if table.startswith("pg_ts"):
                tstables.append(table)
        if tstables:
            self.logger.info("pg_ts_dict already present, do not execute tsearch2.sql")
            if owner:
                self.logger.info("reset pg_ts* owners")
                for table in tstables:
                    cursor.execute("ALTER TABLE %s OWNER TO %s" % (table, owner))
        else:
            fullpath = self.find_tsearch2_schema()
            cursor.execute(open(fullpath).read())
            self.logger.info("tsearch2.sql installed")

    def sql_init_fti(self):
        """Return the sql definition of table()s used by the full text index.

        Require extensions to be already in.
        """
        return """
CREATE table appears(
  uid     INTEGER PRIMARY KEY NOT NULL,
  words   tsvector,
  weight  FLOAT
);

CREATE INDEX appears_words_idx ON appears USING gin(words);
"""

    def sql_drop_fti(self):
        """Drop tables used by the full text index."""
        return "DROP TABLE appears;"

    def sql_grant_user_on_fti(self, user):
        return "GRANT ALL ON appears TO %s;" % (user)

    @callable_deprecated("[lgdb 1.10] deprecated method")
    def boolean_value(self, value):
        if value:
            return "TRUE"
        else:
            return "FALSE"


db._ADV_FUNC_HELPER_DIRECTORY["postgres"] = _PGAdvFuncHelper
