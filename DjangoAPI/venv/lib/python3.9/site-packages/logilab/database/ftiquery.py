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
"""Query objects for Generic Indexer.

"""
__docformat__ = "restructuredtext en"

from logilab.database.fti import StopWord, tokenize


class Query:
    """a query is the object manipulated by the indexer
    the query parser'll call add_word and add_phrase on this object accoring to
    the query string (see query.g for the query string's grammar)
    """

    def __init__(self, normalize):
        self.normalize = normalize
        self.words = {}
        self.phrases = []

    def add_word(self, word):
        """add a single word query"""
        try:
            word = self.normalize(word)
        except StopWord:
            return
        # all single word queries'll be in a single KeywordsQuery
        # so delay instantiation and remove duplicate words
        self.words[word] = 1

    def add_phrase(self, phrase):
        """add a single phrase query"""
        tokens = []
        for word in tokenize(phrase):
            try:
                tokens.append(self.normalize(word))
            except StopWord:
                continue
        self.phrases.append(PhraseQuery(tokens))

    def execute(self, cursor):
        """execute this query using the given cursor
        yield a list of 2-uple (rating, uid)
        """
        assert self.words or self.phrases

        # keywords query
        if not self.words:
            results = {}
        else:
            results = KeywordsQuery(sorted(self.words.keys())).dict_query(cursor)
            if not results:
                raise StopIteration()

        # phrase queries
        for q in self.phrases:
            _results = q.dict_query(cursor, results and results.keys() or None)
            if not _results:
                yield ()
                # return ()
            # adjust rating
            for uid, rating in results.items():
                try:
                    _results[uid] += rating
                except Exception:
                    continue
            results = _results

        for uid, rating in results.items():
            yield (rating, uid)


class KeywordsQuery:
    """
    a keywords query'll look for uid matching all those words in any order
    """

    def __init__(self, words):
        self.words = words

    def dict_query(self, cursor, uids=None):
        """execute this query using the given cursor
        the query maybe restricted to a given list of uids

        return a dict with uid as keys and rating as value
        """
        results = {}
        attrs = {}
        tables, select = [], []
        for i in range(len(self.words)):
            tables.append("appears as appears%d, word as word%d" % (i, i))
            select.append("word%d.word = %%(word%d)s " % (i, i))
            select.append("word%d.word_id = appears%d.word_id " % (i, i))
            attrs["word%d" % i] = self.words[i]
            if i > 0:
                select.append("appears%d.uid = appears%d.uid " % (i - 1, i))
        query = (
            "SELECT count(*) as rating, appears0.uid FROM "
            + ", ".join(tables)
            + " WHERE "
            + " AND ".join(select)
            + " GROUP BY appears0.uid ;"
        )
        cursor.execute(query, attrs)
        for rating, uid in cursor.fetchall():
            results[uid] = rating
        return results


class PhraseQuery:
    """
    a phrase query'll look for uid matching all phrase's tokens in the same order
    """

    def __init__(self, tokens):
        self.tokens = tokens

    def dict_query(self, cursor, uids=None):
        """execute this query using the given cursor
        the query maybe restricted to a given list of uids

        return a dict with uid as keys and rating as value
        """
        results = {}
        if uids is not None:
            uids = ", ".join([str(uid) for uid in uids])
            restrict = "AND uid in (%s)" % uids
        else:
            restrict = ""
        query = (
            "SELECT uid, pos FROM appears,word "
            "WHERE word.word = '%s'"
            "AND word.word_id = appears.word_id %s" % (self.tokens[0], restrict)
        )
        cursor.execute(query)
        for uid, pos in cursor.fetchall():
            w_pos = pos
            matches_all = 1
            for t in self.tokens[1:]:
                w_pos += 1
                cursor.execute(
                    "SELECT appears.uid "
                    "FROM appears,word "
                    "WHERE word.word = %(word)s "
                    "AND appears.pos = %(pos)s "
                    "AND appears.uid = %(uid)s "
                    "AND word.word_id = appears.word_id ;",
                    {"word": t, "uid": uid, "pos": w_pos},
                )
                if not cursor.fetchall():
                    matches_all = 0
                    break
            if matches_all:
                results[uid] = results.get(uid, 0) + 1
        return results
