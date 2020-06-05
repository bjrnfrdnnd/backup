import gzip
import shutil
FN = '/home/bn/archive.tar'
with open(FN, 'rb') as f_in:
    with gzip.open(FN+'.pgz', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)