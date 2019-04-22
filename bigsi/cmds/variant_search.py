import logging
import tempfile
import subprocess
from pyfasta import Fasta

logging.basicConfig()
logger = logging.getLogger(__name__)


def flatten(l):
    return [item for sublist in l for item in sublist]


class BIGSIVariantSearch(object):
    def __init__(self, bigsi, reference):
        self.bigsi = bigsi
        self.reference = reference

    def search(self, ref_base, pos, alt_base="X"):
        var_name = "".join([ref_base, str(pos), alt_base])
        fasta_string = self.create_variant_probe_set(var_name=var_name)
        with tempfile.NamedTemporaryFile() as fp:
            fp.write(fasta_string)
            fp.seek(0)
            fasta = Fasta(fp.name)
        refs = []
        alts = []
        for k, v in fasta.items():
            if "ref" in k:
                refs.append(str(v))
            else:
                alts.append(str(v))
        return {"query": var_name, "results": self.genotype_alleles(refs, alts)}

    def search_for_alleles(self, ref_seqs, alt_seqs):
        results = {"ref": [], "alt": []}
        for ref in ref_seqs:
            res = self.bigsi.search(ref, 1, score=False)
            results["ref"].extend([r["sample_name"] for r in res])
        for alt in alt_seqs:
            res = self.bigsi.search(alt, 1, score=False)
            results["alt"].extend([r["sample_name"] for r in res])
        return results

    def create_variant_probe_set(self, var_name):
        fasta_string = subprocess.check_output(
            [
                "mykrobe",
                "variants",
                "make-probes",
                "-k",
                str(self.bigsi.kmer_size),
                "-v",
                var_name,
                self.reference,
            ]
        )
        return fasta_string

    def genotype_alleles(self, refs, alts):
        ref_alt_samples = self.search_for_alleles(refs, alts)
        results = []
        for sample_name in set(flatten(ref_alt_samples.values())):
            if (
                sample_name in ref_alt_samples["ref"]
                and sample_name in ref_alt_samples["alt"]
            ):
                results.append({"sample_name": sample_name, "genotype": "0/1"})
            elif sample_name in ref_alt_samples["ref"]:
                results.append({"sample_name": sample_name, "genotype": "0/0"})
            elif sample_name in ref_alt_samples["alt"]:
                results.append({"sample_name": sample_name, "genotype": "1/1"})
        return results


class BIGSIAminoAcidMutationSearch(BIGSIVariantSearch):
    def __init__(self, bigsi, reference, genbank):
        super(BIGSIAminoAcidMutationSearch, self).__init__(bigsi, reference)
        self.genbank = genbank

    def create_variant_probe_set(self, var_name):
        ### Run mykrobe variants make-probes  -v G100T ../mykrobe-atlas-cli/src/mykrobe/data/NC_000962.3.fasta
        fasta_string = subprocess.check_output(
            [
                "mykrobe",
                "variants",
                "make-probes",
                "-k",
                str(self.bigsi.kmer_size),
                "-v",
                var_name,
                "-g",
                self.genbank,
                self.reference,
            ]
        )
        return fasta_string

    def search(self, gene, ref, pos, alt):
        mut_name = "".join([ref, str(pos), alt])
        gene_mut_name = "_".join([gene, mut_name])

        fasta_string = self.create_variant_probe_set(var_name=gene_mut_name)
        with tempfile.NamedTemporaryFile() as fp:
            fp.write(fasta_string)
            fp.seek(0)
            fasta = Fasta(fp.name)
        refs = []
        alts = []
        for k, v in fasta.items():
            if "ref" in k:
                refs.append(str(v))
            else:
                alts.append(str(v))
        return {"query": gene_mut_name, "results": self.genotype_alleles(refs, alts)}
