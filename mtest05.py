'''
 Aim:  create a program that is going to prepare files for CrashPlan to be backed up.
'''
import os
import shutil
from pathlib import Path
import sh
import pandas as pd
from dateutil import parser
from elevate import elevate
import sh

HOME = Path(os.path.expanduser("~bn"))
DATA_DIR = Path.joinpath( Path(HOME) , 'data', 'backup',)
#backup for crashplan dir
BU_FCP_DIR = Path.joinpath( Path(HOME) , 'data', 'backup_for_crashplan',)
SPLITSIZE = '10M'

pd.set_option('display.width', 500)
pd.set_option('display.max_columns', 300)
pd.set_option('display.max_colwidth', 300)

elevate()

def remove_file_or_dir(f: Path):
    if f.exists() and f.is_file():
        f.unlink()
    if f.exists() and f.is_dir():
        shutil.rmtree(f)

def make_dataframe(dir: Path):
    all_files = [file for file in dir.rglob('*') if file.is_file() and (file.name.endswith('sql.gz') or file.name.endswith('log'))]
    all_files2 = []
    for f in all_files:
        f: Path
        fn_stem = f.name.split('.')[0]
        fn_suffix = '.'.join(f.name.split('.')[1:])
        # stem to backup
        fn_stb = fn_stem[:len(fn_stem) - 17]
        fn_timestamp = fn_stem[-16:]
        backup_ts = pd.to_datetime(parser.parse(fn_timestamp.replace('_', ' ')))
        llist = [f, f.name, fn_stb, fn_suffix, fn_timestamp, f.relative_to(dir)]
        all_files2.append(llist)

    columns = ['bu_path', "name", "stem", "suffix", "bu_ts", 'bu_rel_path']
    df = pd.DataFrame.from_records(data=all_files2, columns=columns)
    return df

#  First step: find files in backup that needs to be transferred  to backup for crashplan  directory ;
#  they also need to be possibly split into many small files .
df = make_dataframe(dir=DATA_DIR)

# get a dataframe where only the latest version is contained
df2: pd.DataFrame
df2 = df.groupby(['stem','suffix']).max().reset_index()

# for each row in (df2), we are going to transfer or split the corresponding file to backup_for_crashplan.
# We want to
# * if suffix.endswith('gz'):
#   * create a new directory
#   * split the file and put the parts in the directory
# * if suffix.endswith('log'):
#   * just transfer the file
for (index, row) in df2.iterrows():
    if row['suffix'].endswith('gz'):
        # a) create the dir
        #    * if the dir exists, delete it
        path_to_copy_from = row['bu_path']
        dir_to_copy_to = Path(BU_FCP_DIR) / Path(row['bu_rel_path']).parent / Path(row['stem'] + '-' + row['bu_ts'])
        remove_file_or_dir(dir_to_copy_to)
        # create the dir and all missing parents
        dir_to_copy_to.mkdir(parents=True)

        # b) split the file and put the parts in the dir
        #  -a: suffix is 6 chars long
        #  -d: suffix is numeric
        #  -b: size of parts

        sh.split('-a', 6, '-d', '-b', SPLITSIZE, path_to_copy_from, str(dir_to_copy_to) + '/part_')

    elif row['suffix'].endswith('log'):
        # a) copy the file
        #    * replace the file if it exists
        path_to_copy_from = row['bu_path']
        path_to_copy_to = Path(BU_FCP_DIR) / Path(row['bu_rel_path'])
        remove_file_or_dir(path_to_copy_to)
        # copy preserving all metadata
        shutil.copy2(path_to_copy_from, path_to_copy_to)
        pass

# now delete all file/dirs in backup_for_crashplan except the 3 newest ones
df = make_dataframe(dir=BU_FCP_DIR)
df: pd.DataFrame

def bla(grp):
    grp: pd.DataFrame
    df2 = grp.sort_values(by='bu_ts').reset_index()
    df3 = df2.iloc[0:-3]
    return df3
df = df.groupby(['stem','suffix']).apply(bla)

df:pd.DataFrame

for (index, row) in df.iterrows():
    dir_to_copy_to = Path(BU_FCP_DIR) / Path(row['bu_rel_path']).parent / Path(row['stem'] + '-' + row['bu_ts'])
    path_to_copy_to = Path(BU_FCP_DIR) / Path(row['bu_rel_path'])
    remove_file_or_dir(dir_to_copy_to)
    remove_file_or_dir(path_to_copy_to)

