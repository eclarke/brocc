from __future__ import division

import logging
import optparse
import os

from Bio import Entrez


from brocclib.assign import Assigner
from brocclib.get_xml import NcbiEutils
from brocclib.parse import iter_fasta, read_blast
from brocclib.batch import build_taxdict, build_taxonomy


'''
Created on Aug 29, 2011
@author: Serena, Kyle
'''

Entrez.email = 'ecl@mail.med.upenn.edu'

CONSENSUS_THRESHOLDS = [
    ("species", 0.6),
    ("genus", 0.6),
    ("family", 0.6),
    ("order", 0.9),
    ("clas", 0.9),
    ("phylum", 0.9),
    ("kingdom", 0.9),
    ("domain", 0.9),
    ]


def parse_args(argv=None):
    parser = optparse.OptionParser(description=(
        "BROCC uses a consensus method determine taxonomic assignments from "
        "BLAST hits."))
    parser.add_option("--min_id", type="float", default=80.0, help=(
        "minimum identity required for a db hit to be considered at any "
        "level [default: %default]"))
    parser.add_option("--min_cover", type="float", default=.7, help=(
        "minimum coverage required for a db hit to be considered "
        "[default: %default]"))
    parser.add_option("--min_species_id", type="float", help=(
        "minimum identity required for a db hit to be "
        "considered at species level [default: %default]"))
    parser.add_option("--min_genus_id", type="float", help=(
        "minimum identity required for a db hit to be "
        "considered at genus level [default: %default]"))
    parser.add_option("--max_generic", type="float", default=.7, help=(
        "maximum proportion of generic classifications allowed "
        "before query cannot be classified [default: %default]"))
    parser.add_option("--cache_fp", help=(
        "Filepath for retaining data retrieved from NCBI between runs.  "
        "Can help to reduce execution time if BROCC is run several times."))
    parser.add_option("-v", "--verbose", action="store_true",
        help="output message after every query sequence is classified")
    parser.add_option("-i", "--input_fasta_file", dest="fasta_file",
        help="input fasta file of query sequences [REQUIRED]")
    parser.add_option("-b", "--input_blast_file", dest="blast_file",
        help="input blast file [REQUIRED]")
    parser.add_option("-o", "--output_directory",
        help="output directory [REQUIRED]")
    parser.add_option("-a", "--amplicon", help=(
        "amplicon being classified, either 'ITS' or '18S'. If this option is "
        "not supplied, both --min_species_id and --min_genus_id must be "
        "specified"))
    opts, args = parser.parse_args(argv)

    if opts.amplicon == "ITS":
        opts.min_genus_id = 83.05
        opts.min_species_id = 95.2
    elif opts.amplicon == "18S":
        opts.min_genus_id = 96.0
        opts.min_species_id = 99.0
    elif opts.amplicon:
        parser.error("Provided amplicon %s not recognized." % opts.amplicon)
    else:
        if not (opts.min_species_id and opts.min_genus_id):
            parser.error("Must specify --amplicon, or provide both --min_species_id and --min_genus_id.")

    return opts


def main(argv=None):
    opts = parse_args(argv)

    # Configure
    
    if opts.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)
    
    taxa_db = NcbiEutils(opts.cache_fp)
    taxa_db.load_cache()

    consensus_thresholds = [t for _, t in CONSENSUS_THRESHOLDS]

    # Read input files
    
    with open(opts.fasta_file) as f:
        sequences = list(iter_fasta(f))

    with open(opts.blast_file) as f:
        blast_hits = read_blast(f)

    # Open output files

    if not os.path.exists(opts.output_directory):
        os.mkdir(opts.output_directory)
    output_file = open(
        os.path.join(opts.output_directory, "Full_Taxonomy.txt"), 'w')
    standard_taxa_file = open(
        os.path.join(opts.output_directory, "Standard_Taxonomy.txt"), "w")
    log_file = open(os.path.join(opts.output_directory, "brocc.log"), "w")
    log_file.write(
        "Sequence\tWinner_Votes\tVotes_Cast\tGenerics_Pruned\tLevel\t"
        "Classification\n")

    # Do the work

    # Build accn -> taxid dict
    accns = []
    for seq in sequences:
        accns += [hit.accession for hit in blast_hits[seq[0]]]
    taxdict = build_taxdict(accns)

    
    # Build the taxonomy
    taxonomy = build_taxonomy(taxdict.values())

    assigner = Assigner(
        opts.min_cover, opts.min_species_id, opts.min_genus_id, opts.min_id,
        consensus_thresholds, opts.max_generic, taxa_db, taxdict, taxonomy)

    
    for name, seq in sequences:
        seq_hits = blast_hits[name]
        # This is where the magic happens
        a = assigner.assign(name, seq, seq_hits)

        output_file.write(a.format_for_full_taxonomy())
        standard_taxa_file.write(a.format_for_standard_taxonomy())
        log_file.write(a.format_for_log())

    # Close output files, write cache

    output_file.close()
    standard_taxa_file.close()
    log_file.close()

    taxa_db.save_cache()
