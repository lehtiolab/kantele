import os
import sys
from glob import glob
import shutil
from argparse import ArgumentParser
from hashlib import md5

p = ArgumentParser()
p.add_argument('--raws')
p.add_argument('--filters')
p.add_argument('--outdir')
p.add_argument('-output-dir')
p.add_argument('--md5out', action='store_const', const=True, default=False)
p.add_argument('--mzmltool')
p.add_argument('--options')
p.add_argument('-c', required=True)
p.add_argument('--config_commit', required=True)
p.add_argument('--project', required=True)
p.add_argument('-profile')
p.add_argument('-resume', action='store_const', const=True, default=False)

args = p.parse_args(sys.argv[1:])


with open('.nextflow.log', 'w') as fp:
    pass

os.makedirs(args.outdir, exist_ok=True)

# raws is a fn, or '*'
for fn in glob(args.raws):
    newfn = os.path.basename(f'{os.path.splitext(fn)[0]}.mzML')
    newfnmd5 = f'{os.path.splitext(newfn)[0]}.md5'
    contents = f'{newfn} mzML file'
    with open(os.path.join(args.outdir, newfn), 'w') as fp:
        fp.write(contents)
    dighash = md5(contents.encode('utf-8'))
    with open(os.path.join(args.outdir, newfnmd5), 'w') as fp:
        fp.write(dighash.hexdigest())
