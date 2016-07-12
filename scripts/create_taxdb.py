import subprocess


def download_nucl_gb_taxid(outfile):
    md5 = "ftp://ftp.ncbi.nih.gov/pub/taxonomy/accession2taxid/nucl_gb.accession2taxid.gz.md5"
    url = "ftp://ftp.ncbi.nih.gov/pub/taxonomy/accession2taxid/nucl_gb.accession2taxid.gz"
    wget_available = subprocess.call(['command', '-v', 'wget']) == 0
    curl_available = subprocess.call(['command', '-v', 'curl']) == 0
    if wget_available:
        subprocess.check_call(
            ['wget', url, '-O', outfile])
    elif curl_available:
        subprocess.check_call(
            ['curl', '-o', outfile, url])
    else:
        raise RuntimeError(
            "Neither wget nor curl was found: cannot download taxonomy ids.")
