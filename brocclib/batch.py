from itertools import izip, chain
from urllib2 import HTTPError
import time

from Bio import Entrez
from tqdm import tqdm
from brocclib.taxonomy import Lineage

def build_taxdict(accns):
    taxids = list(_bulk_taxids_from_accns(accns))
    return {accn: taxid for accn, taxid in taxids}

def build_taxonomy(taxids):
    if not taxids or len(taxids) == 0:
        return None
    taxonomy = {}
    raw_taxonomy = list(chain.from_iterable(_bulk_taxonomy_from_ids(taxids)))
    for raw_taxon in raw_taxonomy:
        taxon_dict = {}
        for lineage in raw_taxon.get('LineageEx'):
            rank = lineage.get('Rank')
            name = lineage.get('ScientificName')
            taxon_dict[rank] = name

        rank = raw_taxon.get('Rank')
        if rank and rank not in taxon_dict:
            taxon_dict[rank] = raw_taxon.get('ScientificName')

        taxon_dict['Lineage'] = raw_taxon.get('Lineage')

        taxonomy[raw_taxon['TaxId']] = Lineage(taxon_dict)

    return taxonomy



def _bulk_taxids_from_accns(accns, batch_size=200):
    # Keep only unique accns to save query time
    print("Retreiving taxids from accession numbers...")
    accns = list(set(accns))
    print(len(accns))
    for start in range(0, len(accns), batch_size):
        end = min(len(accns), start + batch_size)
        handle = _retry_fail(
            Entrez.elink,
            dbfrom='nucleotide', linkname='nucleotide_taxonomy',
            id=accns[start:end])
        result = Entrez.read(handle)

        if len(result) != len(accns[start:end]):
            print("Result length %s is not the same size as number of queries (%s)" % (len(result), len(accns[start:end])))
        for link, accn in izip(result, accns[start:end]):
            try:
                taxid = link['LinkSetDb'][0]['Link'][0]['Id']
            except (KeyError, IndexError):
                taxid = None
            yield accn, taxid


def _bulk_taxonomy_from_ids(taxids, batch_size=5000):
    taxids = filter(None, taxids)
    print("Retreiving taxonomy from taxids...")
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
            handle = func.__call__(*args, **keywords)
            break
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
    return handle
