from atlasvar.probes.models import Mutation
from atlasvar.probes import AlleleGenerator
from atlasvar.probes import make_variant_probe
from atlasvar.annotation.genes import GeneAminoAcidChangeToDNAVariants


import logging
logging.basicConfig()
logger = logging.getLogger(__name__)


def flatten(l): return [item for sublist in l for item in sublist]


class BIGSIVariantSearch(object):

    def __init__(self, bigsi, reference):
        self.bigsi = bigsi
        self.reference = reference
        self.al = AlleleGenerator(reference_filepath=self.reference,
                                  kmer=self.bigsi.kmer_size)

    def search_for_alleles(self, ref_seqs, alt_seqs):
        results = {"ref": [], "alt": []}
        for ref in ref_seqs:
            res = self.bigsi.search(ref, score=False)
            results["ref"].extend(res.keys())
        for alt in alt_seqs:
            res = self.bigsi.search(alt, score=False)
            results["alt"].extend(res.keys())
        return results

    def make_variant_probe_set(self, var):
        return make_variant_probe(
            self.al, var, self.bigsi.kmer_size, DB=None)

    def create_variant_probe_set(self, var_name):
        var = Mutation(var_name=var_name, reference=self.reference).variant
        variant_panel = self.make_variant_probe_set(var)
        return variant_panel

    def search_for_variant(self, ref_base, pos, alt_base="X", alphabet="DNA"):
        if not alphabet in ["DNA", "PROT"]:
            raise ValueError("alphabet must be either DNA or PROT")
        var_name = "".join([ref_base, str(pos), alt_base])
        variant_probe_set = self.create_variant_probe_set(var_name=var_name)
        return {var_name: self.genotype_alleles(variant_probe_set.refs, variant_probe_set.alts)}

    def genotype_alleles(self, refs, alts):
        ref_alt_samples = self.search_for_alleles(refs, alts)
        results = {}
        for sample_id in set(flatten(ref_alt_samples.values())):
            if sample_id in ref_alt_samples["ref"] and sample_id in ref_alt_samples["alt"]:
                results[sample_id] = {"genotype": "0/1"}
            elif sample_id in ref_alt_samples["ref"]:
                results[sample_id] = {"genotype": "0/0"}
            elif sample_id in ref_alt_samples["alt"]:
                results[sample_id] = {"genotype": "1/1"}
        return results


class BIGSIAminoAcidMutationSearch(BIGSIVariantSearch):

    def __init__(self, bigsi, reference, genbank):
        super(BIGSIAminoAcidMutationSearch, self).__init__(bigsi, reference)
        self.genbank = genbank
        self.aa2dna = GeneAminoAcidChangeToDNAVariants(
            self.reference,
            self.genbank)

    def search_for_amino_acid_variant(self, gene, ref, pos, alt):
        mut_name = "".join([ref, str(pos), alt])
        gene_mut_name = "_".join([gene, mut_name])
        results = {gene_mut_name: {}}
        _results = results[gene_mut_name]

        for var_name in self.aa2dna.get_variant_names(gene, mut_name, True):
            mut = Mutation(reference=self.reference,
                           var_name=var_name,
                           gene=gene,
                           mut=mut_name)
            variant_probe_set = self.create_variant_probe_set(var_name)
            variant_calls = self.genotype_alleles(
                variant_probe_set.refs, variant_probe_set.alts)
            for sample, genotype in variant_calls.items():
                _results[sample] = {"genotype": genotype[
                    "genotype"], "aa_mut": mut.mut, "variant": mut.variant.var_name, "gene": gene}
        return results
