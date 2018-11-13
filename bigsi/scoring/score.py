from bitarray import bitarray
import numpy as np
import math
import copy


def remove_short_ones(s):
    ba = bitarray(s)
    if len(ba) < 3:
        return ba.to01()
    ba2 = ba[1:]
    ba2.append(1)
    ba3 = ba2[1:]
    ba3.append(1)
    ba_out = ba & ba2 & ba3
    return ba_out.to01()


def tabulate_score(ss):
    score_counter = {"0": [], "1": []}
    cnt = 1
    for i in range(len(ss)):
        current = ss[i]
        if i < len(ss) - 1:
            _next = ss[i + 1]
            cnt += 1
            if current != _next:
                score_counter[current].append(cnt)
                cnt = 1
        else:
            score_counter[current].append(cnt)
    return score_counter


class Scorer:
    def __init__(
        self, DB_SIZE, MATCH=1, MISMATCH=2, LAMBDA_UNGAPPED=1.330, K_UNGAPPED=0.621, LAMBDA_GAPPED=1.28, K_GAPPED=0.46
    ):
        self.LAMBDA_UNGAPPED = LAMBDA_UNGAPPED
        self.K_UNGAPPED = K_UNGAPPED
        self.LAMBDA_GAPPED = LAMBDA_GAPPED
        self.K_GAPPED = K_GAPPED

        self.MATCH = MATCH
        self.DB_SIZE = DB_SIZE
        self.MISMATCH = MISMATCH
        self.kmer_adjust = 3

    def calculate_score(self, score_counter, convert):
        max_score = copy.copy(self.MATCH * sum(score_counter["1"]))
        min_score = copy.copy(max_score)
        mean_score = copy.copy(min_score)

        SNP_t = 31 + self.kmer_adjust  # correct for the 'remove_short_ones'
        max_total_N_snps = 0
        min_total_N_snps = 0
        for i in score_counter["0"]:
            min_N_snps = float(i) / SNP_t
            max_N_snps = (i - SNP_t) + 1
            if max_N_snps < min_N_snps:
                max_N_snps = min_N_snps
            max_total_N_snps += max_N_snps
            min_total_N_snps += min_N_snps
            mean_N_snps = min_N_snps + 0.05 * max_N_snps

            max_penalty = self.MISMATCH * (max_N_snps)
            min_penalty = self.MISMATCH * (min_N_snps)
            mean_penalty = self.MISMATCH * (mean_N_snps)

            points_for_max = self.MATCH * (i - max_penalty)
            points_for_min = self.MATCH * (i - min_penalty)
            points_for_mean = self.MATCH * (i - mean_penalty)

            max_score = round(max_score - min_penalty + points_for_min, 2)
            min_score = round(min_score - max_penalty + points_for_max, 2)
            mean_score = round(mean_score - mean_penalty + points_for_mean, 2)

        return {
            "score": round(mean_score * convert, 2),
            "min_score": round(min_score * convert, 2),
            "max_score": round(max_score * convert, 2),
            "max_mismatches": math.ceil(max_total_N_snps),
            "min_mismatches": math.floor(min_total_N_snps),
            "mismatches": math.ceil(math.ceil(min_total_N_snps) + (0.05 * math.floor(max_total_N_snps))),
        }

    def score(self, s):
        ss = remove_short_ones(s)
        max_possible_score = len(ss)
        seq_len = max_possible_score + 31 - 1
        convert = seq_len / max_possible_score
        score_counter = tabulate_score(ss)
        score_dict = self.calculate_score(score_counter, convert)
        score_dict["max_nident"] = seq_len - score_dict.get("min_mismatches")
        score_dict["nident"] = seq_len - score_dict.get("mismatches")
        score_dict["min_nident"] = seq_len - score_dict.get("max_mismatches")
        score_dict["pident"] = 100 * float(score_dict["nident"]) / seq_len
        score_dict["max_pident"] = 100 * float(score_dict["max_nident"]) / seq_len
        score_dict["min_pident"] = 100 * float(score_dict["min_nident"]) / seq_len
        score_dict["length"] = seq_len
        score_dict["evalue"] = self.evalue(score_dict["score"], seq_len)
        score_dict["pvalue"] = self.pvalue(score_dict["evalue"])
        score_dict["log_evalue"] = round(self.log_evalue(score_dict["score"], seq_len), 2)
        score_dict["log_pvalue"] = round(self.log_pvalue(score_dict["log_evalue"]), 2)
        return score_dict

    def bitscore(self, s):
        scored = self.score(s)
        score = scored.get("score")
        l = self.LAMBDA_UNGAPPED
        k = self.K_UNGAPPED
        return (l * score - np.log(k)) / np.log(2)

    def evalue(self, score, n):
        l = self.LAMBDA_UNGAPPED
        k = self.K_UNGAPPED
        m = self.DB_SIZE
        return k * m * n * np.exp(-l * score)

    def pvalue(self, evalue):
        return 1 - np.exp(-evalue)

    def log_evalue(self, score, n):
        m = self.DB_SIZE
        if m == 0:
            m = 1
        l = self.LAMBDA_UNGAPPED
        k = self.K_UNGAPPED
        return round(np.log10(k * m * n) - l * score, 2)

    def log_pvalue(self, log_evalue):
        evalue = 10 ** log_evalue
        if 1 - np.exp(-evalue) > 0:
            logp = np.log10(1 - np.exp(-evalue))
        else:
            logp = -np.inf
        if logp == -np.inf:
            return round(log_evalue, 2)
        else:
            return round(logp, 2)
