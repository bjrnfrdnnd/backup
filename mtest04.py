import os
from pathlib import Path
import sh

HOME = Path.home()
HOME = Path(os.path.expanduser("~bn"))
DATA_DIR = Path.joinpath( Path(HOME) , 'data', 'backup', 'mysql', 'data')
LOG_DIR = Path.joinpath( Path(HOME) , 'data', 'backup', 'mysql', 'logs')
print(DATA_DIR)



#cmd = "find ${DATA_DIR} -maxdepth 1 -type d -regextype sed -regex \"^\/.*${DB}\-[0-9].*\" -printf '%Ts\t%p\n' | sort -n | head -n -2 | cut -f 2- | xargs rm -rf"

# a =`echo "show databases;" | mysql `
DATA_BASES = sh.mysql(sh.echo('show databases;'))
DATA_BASES = [el.strip() for el in DATA_BASES]
DATA_BASES = DATA_BASES[1:] # first entry is 'Database' which is not a Database
DATA_BASES += ['All-Databases']
DATA_BASES = ['trading_oanda_d1']
DATESTAMP = sh.date("+%Y-%m-%d_%H:%M").strip()

for DB in DATA_BASES:
    for DD in [DATA_DIR, LOG_DIR]:
        # step a): delete all except the latest two files for each database
        print(f'database: {DB}; dir: {DD}')
        a = sh.find(DATA_DIR, '-maxdepth', '1', '-type', 'f',  '-regextype', 'sed', '-regex', f'^/.*{DB}\-[0-9].*', '-printf', '%Ts\t%p\n')
        b = sh.sort(a, '-n')
        c = sh.head(b, '-n', '-2')
        d = sh.cut(c, '-f', '2-')
        print(d.strip())
        e = sh.xargs(d, 'rm', '-rf')


    # step b): export the databases
    FILENAME = Path.joinpath(DATA_DIR,f'{DB}-{DATESTAMP}.sql.gz')
    print(f'FILENAME: {FILENAME}')
    LOGFILENAME = Path.joinpath(LOG_DIR,f'{DB}-{DATESTAMP}.log')
    print(f'LOGFILENAME: {LOGFILENAME}')

    # cmd = "mysqldump  -v --single-transaction --quick --lock-tables=false ${DB} 2>'${LOGFILENAME}' |  pigz > '${FILENAME}' "
    # sh.mysqldump('-v', '--single-transaction', '--quick', '--lock-tables=false', DB, _out=FILENAME, _err=LOGFILENAME)
    sh.ls(DATA_DIR, _out=FILENAME)
    print()
# b = sh.head(sh.sort(sh.find(DATA_DIR, '-type', 'd',  '-regextype', 'sed', '-regex', '^/.*testDB\-[0-9].*', '-printf', '%Ts\t%p\n'), '-n'), '-n', '-2')
# print(b)

