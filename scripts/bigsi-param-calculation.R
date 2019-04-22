N = 10**6
K_max = 10**7
L_min = 50
k = 31
q_max = 10**-6
N=10**3



calc_p<-function(q_max, N, L_min){
  kmer_L_min<-L_min-k+1
  p<-(q_max / N) ^ (1/kmer_L_min)
  return(p)
}
p<-calc_p(q_max, N, L_min)

calc_m<-function(p,K_max){
  m = - (K_max * log(p)) / log(2)^2
  return(m)
}
calc_eta<-function(p){
  eta<- -(log(p))/(log(2))
  return(ceiling(eta))
}
calc_m(p,K_max)
calc_eta(p)

calc_m2<-function(K_max,q_max,N,L_min,k){
  kmer_L_min<-L_min-k+1
  m <- - (K_max * log(q_max / N)) / (kmer_L_min * (log(2)^2))
  return(m)
}
calc_m2(K_max,q_max,N,L_min,k)

calc_eta2<-function(q_max,N,L_min,k){
  kmer_L_min<-L_min-k+1
  eta<- -(log(q_max / N)) / (kmer_L_min*log(2))
  return(ceiling(eta))
}
calc_eta2(q_max,N,L_min,k)

# calculate bloom filter false positive rate given:
# number of bits (nbits), number of elements (nel), number of hash functions (nhash)
get_bloom_fpr <- function(nbits, nel, nhash) {
  return ((1 - exp(-nhash * nel / nbits)) ^ nhash)
}

## The theoretical false discovery rate for a query of length 2k-1 in a BIGSI with params
# m=25*10^6, K_max=10**7
L_min=2*31-1
m=25*10^6
eta=3
K_max=10**7
get_bloom_fpr(m, K_max, eta)^L_min
