import requests
import begin


def search(seq, threshold=1):
    url = "http://api.cbgseq.com/search?threshold=%f&seq=%s" % (
        float(threshold), seq)
    results = requests.get(url).json()
    samples = []
    for i, j in list(results.values())[0]["results"].items():
        samples.append(i)
    return samples


@begin.start
def main(seq, threshold=1):
    samples = search(seq, threshold)

    for s in samples:
        print(s)
