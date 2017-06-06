from atlasvar.probes.models import Mutation
from atlasvar.probes import AlleleGenerator
from atlasvar.probes import make_variant_probe


class CBGVariantSearch(object):

    def __init__(self, cbg):
        self.cbg = cbg

    def search_for_alleles(self, cbg):
        pass

    def create_variant_probe_set(self, var_name, reference):
        var = Mutation(var_name=var_name, reference=reference).variant
        al = AlleleGenerator(reference_filepath=reference,
                             kmer=self.cbg.kmer_size)
        variant_panel = make_variant_probe(
            al, var, self.cbg.kmer_size, DB=None)
        return variant_panel

    def search_for_variant(self, reference, ref_base, pos, alt_bases="X", alphabet="DNA"):
        if not alphabet in ["DNA", "PROT"]:
            raise ValueError("alphabet must be either DNA or PROT")
        variant_probe_set = self.create_variant_probe_set(var_name="".join(
            [ref_base, str(pos), alt_bases]), reference=reference)
        return variant_probe_set.refs
