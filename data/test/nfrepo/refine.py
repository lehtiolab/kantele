import os
import sys
from glob import glob
import shutil
from argparse import ArgumentParser

p = ArgumentParser()
p.add_argument('--filters')
p.add_argument('--outdir')
p.add_argument('--container')
p.add_argument('--instrument')
p.add_argument('--isobaric')
p.add_argument('--tdb')
p.add_argument('--input')
p.add_argument('--options')
p.add_argument('-profile')
p.add_argument('-name')
p.add_argument('-with-weblog')
p.add_argument('-with-trace', action='store_const', const=True, default=False)
p.add_argument('-resume', action='store_const', const=True, default=False)
args = p.parse_args(sys.argv[1:])

os.makedirs(args.outdir)

# raws is a 'fn;fn2;fn3;...' or '*', but we use 1 file (or *) for testing so can use glob
with open(args.input) as fp:
    srcfn = fp.read().strip()
    newfn = os.path.basename(f'{os.path.splitext(srcfn)[0]}_refined.mzML')
    with open(os.path.join(args.outdir, newfn), 'w') as fp:
        fp.write(f'{newfn} mzML file')

with open('.nextflow.log', 'w') as fp:
    pass
