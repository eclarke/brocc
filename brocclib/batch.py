from itertools import izip
from urllib2 import HTTPError
import time

from Bio import Entrez


def build_taxdict(accns):
    return {accn: taxid for accn, taxid in _bulk_taxids_from_accns(accns)}


def build_taxonomy(taxids):


def _bulk_taxids_from_accns(accns, batch_size=200):
    # Keep only unique accns to save query time
    accns = list(set(accns))
    for start in range(0, len(accns), batch_size):
        end = min(len(accns), start + batch_size)
        handle = _retry_fail(
            Entrez.elink,
            dbfrom='nucleotide', linkname='nucleotide_taxonomy',
            id=accns[start:end])
        result = Entrez.read(handle)
        assert len(result) == len(accns[start:end])
        for link, accn in izip(result, accns[start:end]):
            try:
                taxid = link['LinkSetDb'][0]['Link'][0]['Id']
            except (KeyError, IndexError):
                taxid = None
            yield accn, taxid


def _bulk_taxonomy_from_ids(taxids, batch_size=5000):
    post = Entrez.read(Entrez.epost('taxonomy', id=','.join(map(str, taxids))))
    for start in range(0, len(taxids), batch_size):
        handle = _retry_fail(
            Entrez.efetch,
            db='taxonomy', retstart=start, retmax=batch_size,
            webenv=post['WebEnv'], query_key=post['QueryKey'])
        yield(Entrez.read(handle))


def _retry_fail(func, *args, **keywords):
    attempt = 1
    while attempt <= 3:
        try:
            return func.__call__(*args, **keywords)
        except HTTPError as err:
            if 500 <= err.code <= 599:
                print("NCBI server error ({}); retrying ({}/3)".format(
                    err.code, attempt))
                attempt += 1
                if attempt > 3:
                    raise
                time.sleep(15)
            else:
                raise
