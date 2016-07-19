import unittest
import sqlite3
import csv
import itertools
import math

from tqdm import tqdm


class _Database(object):

    def __init__(self, fp, create=False):
        self.fp = fp
        self.populated = False
        self._connect()
        if create:
            self.create_schema()

    def _connect(self):
        self._con = sqlite3.connect(self.fp)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._con.close()

    def create_schema(self):
        self._con.executescript(self.schema)
        self._con.commit()


class TaxIdDb(_Database):

    schema = (
        "DROP TABLE IF EXISTS accn_taxid;"
        "CREATE TABLE accn_taxid("
        "accn_ver CHARACTER NOT NULL PRIMARY KEY,"
        "taxid INT NOT NULL);")

    accn_query = (
        "SELECT * FROM accn_taxid WHERE accn_ver == (?)")

    def populate(self, infile, verbose=True, force=False, lines=110870098):
        """Populate the taxid database.

        :param infile: an iterable that provides lines for the csv parser
        :param verbose: display progressbar
        :param force: repopulate if already populated
        :param lines: the number of lines in the file
            (used for gzipped files to provide accurate estimates).
        """

        if self.populated and not force:
            raise ValueError(
                "Database already populated. "
                "Specify force=True to repopulate.")

        iterator = csv.reader(infile, delimiter='\t')
        if verbose:
            iterator = tqdm(iterator, unit='records', total=lines)
        for _, accn_ver, taxid, _ in iterator:
            self._con.execute(
                "INSERT OR IGNORE INTO accn_taxid VALUES (?,?)",
                (accn_ver, taxid))
        self._con.commit()
        self.populated = True

    def get_taxid_for_accns(self, accns):
        if not self.populated:
            raise ValueError(
                "Database has not been populated. Run populate() first.")
        if isinstance(accns, str):
            accns = [accns]
        for accn in accns:
            res = self._con.execute(self.accn_query, [accn]).fetchone()
            if res:
                _, taxid = res
                yield taxid
            else:
                yield None


class TaxonomyDb(_Database):
    pass


class TestTaxIDDb(unittest.TestCase):

    testData = [
        "A00002	A00002.1	9913	2",
        "A00003	A00003.1	9913	3",
        "X17276	X17276.1	9646	4"
    ]

    def testPopulateDb(self):
        with TaxIdDb(':memory:', create=True) as db:

            # Populate database successfully
            db.populate(self.testData, verbose=False)
            self.assertTrue(db.populated)

            # Don't repopulate if force not specified
            with self.assertRaises(ValueError):
                db.populate(self.testData, verbose=False)

            # Do repopulate if force specified
            db.populate(self.testData, force=True, verbose=False)

    def testQueryDb(self):
        with TaxIdDb(':memory:', create=True) as db:
            db.populate(self.testData, verbose=False)

            # Test single accn
            self.assertEqual(9646, db.get_taxid_for_accns("X17276.1").next())

            # Test multiple accns
            self.assertEqual([9913, 9913, 9646], list(db.get_taxid_for_accns(
                ["A00002.1", "A00003.1", "X17276.1"])))

            # Test missing accns
            self.assertIsNone(db.get_taxid_for_accns("MissingAccn").next())
