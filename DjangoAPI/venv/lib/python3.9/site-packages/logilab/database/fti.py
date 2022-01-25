# copyright 2003-2011 LOGILAB S.A. (Paris, FRANCE), all rights reserved.
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
import re

from logilab.common.textutils import unormalize

REM_PUNC = re.compile(r"[,.;:!?\n\r\t\)\(«»\<\>/\\\|\[\]{}^#@$£_=+\-*&§]")


class StopWord(Exception):
    """Raised to indicate that a stop word has been encountered."""


def normalize(word):
    """Return the normalized form for a word.

    The word given in argument should be unicode !

    currently normalized word are :
       _ in lower case
       _ without any accent

    This function may raise StopWord if the word shouldn't be indexed

    stop words are :
       _ single letter
    """
    assert isinstance(word, str), "%r should be unicode" % word
    # do not index single letters
    if len(word) == 1:
        raise StopWord()
    word = unormalize(word.lower(), substitute="")
    # we need an ascii-only unicode string, not bytes
    return word.encode("ascii", "ignore").decode("ascii")


def normalize_words(rawwords):
    words = []
    for word in rawwords:
        try:
            words.append(normalize(word))
        except StopWord:
            continue
    return words


RE_DATE = r"\d{2,4}[/-]\d{2,2}[/-]\d{2,4}"
RE_HOUR = r"\d{1,2}[:h]\d{2,2}:?\d{0,2}"
# indexation tokenizer
RE_WORD_INDEX = "[^ ,.;:!?\"\n\r\t)(«»\\<\\>/\\\\\\|\\[\\]{}^#@$£_'=+\\-&§\u2019]+"
TOKENIZE_RE_INDEX = re.compile(
    f"(?:{RE_DATE})|(?:{RE_HOUR})|(?:{RE_WORD_INDEX})", re.L & re.U
)
tokenize = TOKENIZE_RE_INDEX.findall
# query tokenizer
RE_WORD_QUERY = "[^ ,.;:!?\"\n\r\t)(«»\\<\\>/\\\\\\|\\[\\]{}^#@$£_'=+\\-&§\u2019]+"
TOKENIZE_RE_QUERY = re.compile(
    f"(?:{RE_DATE})|(?:{RE_HOUR})|(?:{RE_WORD_QUERY})", re.L & re.U
)
tokenize_query = TOKENIZE_RE_QUERY.findall


class FTIndexerMixIn:
    """The base full-text indexer mixin. To be mixed with advanced
    functionnality helper.

    Provide an inefficient but generic indexing method which can be overridden.
    """

    fti_table = "appears"
    fti_uid_attr = "uid"
    fti_need_distinct = True
    fti_sql_schema = """
%s

CREATE TABLE word (
  word_id INTEGER PRIMARY KEY NOT NULL,
  word    VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE appears(
  uid     INTEGER,
  word_id INTEGER REFERENCES word ON DELETE CASCADE,
  pos     INTEGER NOT NULL
);

CREATE INDEX appears_uid ON appears (uid);
CREATE INDEX appears_word_id ON appears (word_id);
"""

    def has_fti_table(self, cursor):
        return self.fti_table in self.list_tables(cursor)

    def init_fti(self, cursor):
        self.init_fti_extensions(cursor)
        cursor.execute(self.sql_init_fti())

    def init_fti_extensions(self, cursor, owner=None):
        """if necessary, install extensions at database creation time"""

    def index_object(self, uid, obj, cnx=None):
        """index an object with the given uid
        the object should inherit from or be compatible with Indexable object
        """
        if cnx is None:
            cnx = self._cnx
        cursor = cnx.cursor()
        try:
            self.cursor_index_object(uid, obj, cursor)
            cnx.commit()
        except Exception:
            cnx.rollback()
            raise

    def unindex_object(self, uid, cnx=None):
        """unindex an object
        the object should inherit from or be compatible with Indexable object
        """
        if cnx is None:
            cnx = self._cnx
        cursor = cnx.cursor()
        try:
            self.cursor_unindex_object(uid, cursor)
            cnx.commit()
        except Exception:
            cnx.rollback()
            raise

    def reindex_object(self, uid, obj, cnx=None):
        """index an object with the given uid
        the object should inherit from or be compatible with Indexable object
        """
        if cnx is None:
            cnx = self._cnx
        cursor = cnx.cursor()
        try:
            self.cursor_reindex_object(uid, obj, cursor)
            cnx.commit()
        except Exception:
            cnx.rollback()
            raise

    def cursor_index_object(self, uid, obj, cursor):
        position = 0
        # sort for test predictability
        for weight, words in sorted(obj.get_words().items()):
            for word in words:
                self._save_word(uid, word, position, cursor)
                position += 1

    def cursor_unindex_object(self, uid, cursor):
        cursor.execute("DELETE FROM appears WHERE uid=%s" % uid)

    def cursor_reindex_object(self, uid, obj, cursor):
        self.cursor_unindex_object(uid, cursor)
        self.cursor_index_object(uid, obj, cursor)

    def _save_word(self, uid, word, position, cursor):
        try:
            word = normalize(word)
        except StopWord:
            return
        cursor.execute("SELECT word_id FROM word WHERE word=%(word)s;", {"word": word})
        wid = cursor.fetchone()
        if wid is None:
            wid = self.increment_sequence(cursor, "word_id_seq")
            try:
                cursor.execute(
                    """INSERT INTO word(word_id, word)
                VALUES (%(uid)s,%(word)s);""",
                    {"uid": wid, "word": word},
                )
            except Exception:
                # Race condition occured.
                # someone inserted the word before we did.
                # Never mind, let's use the new entry...
                cursor.execute(
                    "SELECT word_id FROM word WHERE word=%(word)s;", {"word": word}
                )
                wid = cursor.fetchone()[0]
        else:
            wid = wid[0]
        cursor.execute(
            "INSERT INTO appears(uid, word_id, pos) "
            "VALUES (%(uid)s,%(wid)s,%(position)s);",
            {"uid": uid, "wid": wid, "position": position},
        )

    def fulltext_search(self, query_string, cursor=None):
        """execute a full text query and return a list of 2-uple (rating, uid)"""
        from logilab.database.ftiparser import IndexerQuery, IndexerQueryScanner
        from logilab.database.ftiquery import Query

        query = Query(normalize)
        parser = IndexerQuery(IndexerQueryScanner(REM_PUNC.sub(" ", query_string)))
        parser.goal(query)
        return query.execute(cursor or self._cnx.cursor())

    def fti_restriction_sql(self, tablename, querystr, jointo=None, not_=False):
        words = []
        for word in tokenize(querystr):
            try:
                words.append("'%s'" % normalize(word))
            except StopWord:
                continue
        sql = "{}.word_id IN (SELECT word_id FROM word WHERE word in ({}))".format(
            tablename,
            ", ".join(words),
        )
        if not_:
            sql = "NOT (%s)" % sql
        if jointo is None:
            return sql
        return f"{sql} AND {tablename}.uid={jointo}"

    def fti_rank_order(self, tablename, querystr):
        return None

    def sql_init_fti(self):
        """return the sql definition of table()s used by the full text index"""
        return self.fti_sql_schema % self.sql_create_sequence("word_id_seq")

    def sql_drop_fti(self):
        """drop tables used by the full text index"""
        return """DROP TABLE appears;
DROP TABLE word; %s""" % self.sql_drop_sequence(
            "word_id_seq"
        )

    def sql_grant_user_on_fti(self, user):
        return """GRANT ALL ON appears_uid TO %s;
GRANT ALL ON appears_word_id TO %s;
GRANT ALL ON appears TO %s;
GRANT ALL ON word TO %s;
""" % (
            user,
            user,
            user,
            user,
        )
