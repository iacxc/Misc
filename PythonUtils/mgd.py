
import numpy as np


#Compute the probility
#Parameters:
#    data: an m * n array, the parameter must be a reference
#          m is the number of samples, n is the number of features
#Output:
#    an m * 2 array,
#        the first column is the probility,
#        the second column is the offset divided by the standard deviation,
#               if it is larger than 4, likely to be anomaly ( < 0.1% )
#        others are the original array
#
def ComputeMGD(data):
    x = np.matrix(data)
    if __debug__: print 'x =', x

    mu = x.mean(axis=0)
    if __debug__: print 'mu =', mu

    m, n = x.shape

    d = x - mu
    sigma = d.T * d / m
    dev = np.linalg.det(sigma)
    sd = np.sqrt(dev) if dev >= 0 else 0

    if __debug__:
        print 'd=', d, d.T, '\n', 'sigma =', sigma
        print 'sd =', sd

    factor = 1 if sd == 0 else 1 / ( pow(2 * np.pi, n * 0.5) * sd )
    sigma_inv = np.linalg.pinv(sigma)

    #compute p(x) for each x
    probs = []
    for i in xrange(m):
        di = x[i] - mu

        dist = np.sqrt(di * di.T)[0,0]    #offset from mu

        product = (di * sigma_inv * di.T)[0,0]

        probs.append([ factor * np.exp(-0.5 * product),
                       dist / sd if sd != 0 else 0 ])

    return probs

