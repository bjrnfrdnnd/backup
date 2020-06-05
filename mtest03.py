from timeit import repeat
import numpy as np

import mgzip
import shutil
FN = '/home/bn/archive.tar'

def ff():
    with open(FN, 'rb') as f_in:
        with mgzip.open(FN+'.mpgz', 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
NR=1
NN=1
a = repeat(ff, repeat=NR, number=NN)
a = np.array(a) / NN
a *= 1
print(
    f"upsert tmp {np.mean(a):8.3f} +- {np.std(a):8.3f}; {a} s")