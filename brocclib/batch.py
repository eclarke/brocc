from cStringIO import StringIO
from itertools import izip, chain
from urllib2 import HTTPError
import time
import re


import requests

from Bio import Entrez
from tqdm import trange, tqdm
from brocclib.taxonomy import Lineage


def build_taxdict_from_accns(accns):
    taxids = list(_bulk_taxids_from_accns(accns))
    return {accn: taxid for accn, taxid in taxids}


def build_taxdict_from_gis(gis):
    taxids = list(_bulk_taxids_from_gis(gis))
    return {gi: taxid for gi, taxid in taxids}


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


def _bulk_taxids_from_accns(accessions, batch_size=5000):
    # Keep only unique accns to save query time
    print("Retrieving taxids from accession numbers...")
    accns = list(set(filter(_validate_accn, accessions)))
    invalid_accns = filter(lambda x: not _validate_accn(x), set(accessions))
    if len(invalid_accns) > 0:
        print("The following accession numbers are invalid and were skipped:")
        for invalid in invalid_accns:
            print(invalid)
    nchunks = max(len(accns) / batch_size, 1)
    cur_chunk = 1
    for start in range(0, len(accns), batch_size):
        print("Processing block {}/{}".format(cur_chunk, nchunks))
        end = min(len(accns), start + batch_size)
        handle = _elink_post(
            dbfrom='nucleotide', linkname='nucleotide_taxonomy',
            ids=accns[start:end])
        result = Entrez.read(handle)

        assert len(result) == len(accns[start:end])

        for link, accn in izip(result, accns[start:end]):
            try:
                taxid = link['LinkSetDb'][0]['Link'][0]['Id']
            except (KeyError, IndexError):
                taxid = None
            yield accn, taxid
        cur_chunk += 1


def _elink_post(dbfrom, linkname, ids):
    # Warning: should not post more than 5000 IDs to Elink at once
    url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi'
    params = {
        'dbfrom': dbfrom,
        'linkname': linkname,
        'id': ids}
    rt = requests.post(url, params, stream=True)
    print(rt.headers)
    content = ''.join(
        tqdm(
            rt.iter_content(chunk_size=1024 * 1024),
            unit='Mb'
        ))
    return StringIO(content)


def _bulk_taxids_from_gis(gis, batch_size=5000):
    post = Entrez.read(Entrez.epost('nucleotide', id=','.join(map(str, gis))))
    for start in range(0, len(gis), batch_size):
        handle = _retry_fail(
            Entrez.elink,
            dbfrom='nucleotide', linkname='nucleotide_taxonomy',
            #            retstart=start, retmax=batch_size,
            webenv=post['WebEnv'], query_key=post['QueryKey'])
        res = Entrez.read(handle)
        print(len(res))
        for link in Entrez.read(handle):
            print(link)
            yield(link)


def _bulk_taxonomy_from_ids(taxids, batch_size=5000):
    taxids = filter(None, taxids)
    print("Retrieving taxonomy from taxids...")
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


def _validate_accn(accn):
    return re.match(r'[A-Z0-9_]+\.[0-9]+', accn) is not None
