import re, os, time, glob, csv
import numpy as np
import socket

if socket.gethostname().find('metadatalab')>=0:
    DATA_ROOT = "/home/data/genbank2018"
else:
    DATA_ROOT = "/Users/jwang72/git/genbank/data"
print(f"DATA_ROOT={DATA_ROOT}")

#<script>addRow("gbcon118.seq.gz","gbcon118.seq.gz",0,11692295,"11.2 MB",1539823020,"10/17/18, 8:37:00 PM");</script>
#url = "ftp://ftp.ncbi.nlm.nih.gov/genbank/gbbct1.seq.gz
p = re.compile(r'<script>addRow\("(.*?.seq.gz)".*?,"(\d+\.?\d+) MB",')

def download_seq_gz():
    out = []
    total = 0
    for line in open('ftp-index.html').readlines():
        m = p.match(line)
        if m:
            gzfile, fsize = m.groups()
            total += float(fsize)
            url = f"ftp://ftp.ncbi.nlm.nih.gov/genbank/{gzfile}"
            out.append(url)
            outfile = f'seq_gz/{gzfile}'
            if not os.path.exists(outfile):
                cmd = f'wget {url} -O {outfile}'
                os.system(cmd)
            #break
    
    print('\n'.join(out[:5]))
    print('\n'.join(out[-5:]))
    
    print(len(out))
    
    total = int(total)
    print(f"{total:,} MB")
    
    print('2013 version has 1876 gz files')
    print(f'{len(out)/1876:.2f}')

do_it = False
do_it = True
def run(cmd): 
    if 0: print(cmd)
    if do_it:
        os.system(cmd)


FOUT = open('/tmp/accession_cnt_tmp.dat', 'w')

accession_cnt = {}
def process_one_ann_file(fname_ann):
    print(f'processing {fname_ann}')
    for line in open(fname_ann):
        #if line[:10] == "ACCESSION ":  # there could be multi or multi-line accession numbers:
                                        # for example, gbbct176.ann has: ACCESSION    CP011124 JXRE01000000 JXRE01000001 ...
        if line[:8] == "VERSION ":
            val = line[12:].strip()
            accession_cnt[val] = accession_cnt.get(val, 0) +1
            FOUT.write(f"{val} {accession_cnt[val]}\n")
    

def get_2013_gi_accession():
    cnt = 0
    out, out2, accession_freq = [], [], {}
    max_gi = 0
    with open('accession_2013_version.dat', 'w') as fout:
      with open('/home/data/genbank/csv/ANNOTATION.csv') as fin:
        for line in fin:
            cnt += 1
            #gi, _ = line.split('|', 1)
            gi, version, keywordsId, sourceId, organismId, commentId, dblinkId, locus, accession, _ = line.split('|', 9)
            version_ = f'{accession}.{version}'

            #print(gi)
            #out.append(f'{gi}')
            gi = int(gi)
            if gi>max_gi: max_gi = gi
            if 0:
                out2.append(gi)
            #accession_freq[accession] = accession_freq.get(accession, 0) +1 
            if 0:
                fout.write(f'{accession}\n')
            if 1:
                fout.write(f'{version_}\n')

            #if cnt>5: break

        #for acc, freq in sorted(accession_freq.items(), key=lambda x:x[1], reverse=True):
        #    fout.write(f'{acc} {freq}\n')

    if 0:
        with open('gi_2013.bin', 'wb') as fout:
            np.array(out2, dtype=np.uint32).tofile(fout)
    # read: gis = np.fromfile('gi_2013.bin', dtype='uint32')

    if 0:
        print(f'max_gi: {max_gi}   cnt: {len(out2)}')

def check_uniqueness_of_accession_number_2013():
    hash = {}
    with open('accession_2013.dat') as fin:
        cnt = 0
        for line in fin:
            cnt += 1
            #if cnt>10: break
            if line in hash:
                print(f'redundant: {line}')
                break
            else:
                hash[line] = 1


def get_2018_accession_only():
    ann_gz_folder = f"{DATA_ROOT}/ann_gz"
    for fpath in glob.glob(f'{ann_gz_folder}/*.gz'):
        fname_gz = fpath.split('/')[-1]
        fname = fname_gz.split('.gz')[0]
        #print(fname, fname_gz)
        cmd = f'cp "{fpath}" /tmp'
        run(cmd)
        cmd = f'gunzip "/tmp/{fname_gz}"'
        run(cmd)
        process_one_ann_file(f'/tmp/{fname}')
        cmd = f'rm "/tmp/{fname}"'
        run(cmd)
        #break

def check_uniqueness_of_accession_number_2018():
    hash = {}
    with open('/tmp/accession_cnt.dat') as fin:
        cnt = 0
        for line in fin:
            accession = line.split('.')[0]
            cnt += 1
            if cnt>10: break
            #print(line, accession)
            if accession in hash:
                print(f'redundant: {line} {accession}')
                break
            else:
                hash[accession] = 1

def compress_patent_references():
    csv_folder = f"{DATA_ROOT}/csv"
    fpath_ref = f"{csv_folder}/REFERENCE_with_year.csv"
    fpath_ref_patent_out = f"{csv_folder}/REFERENCE_PATENT.csv"
    p_patent = re.compile(r'^Patent:\s.*?\s[0-3]\d-[A-Z][A-Z][A-Z]-((?:19|20)\d\d)')
    cnt = 0
    pat_freq = {}
    author_title_freq = {}
    fpath_ref_patent_out = f"{csv_folder}/REFERENCE_PATENT.csv"
    fout = open(fpath_ref_patent_out, 'w')
    csv_writer = csv.writer(fout, delimiter='|')

    journal_index = -4 if fpath_ref.find('_with_year')>=0 else -3
    with open(fpath_ref) as fin: 
        for row in csv.reader(fin, delimiter='|'):
            cnt += 1
            #if cnt>1000000: break
            journal = row[journal_index]
            if not p_patent.match(journal):
                continue
            f0, f1, f2, _ = journal.split(' ', 3)
            pat = f"{f0} {f1} {f2}"

            if not pat in pat_freq:
                csv_writer.writerow(row)
            pat_freq[pat] = pat_freq.get(pat, 0) +1 

            if 0:
                authors = row[2] 
                title = row[4] 
                author_title = f"{authors}|{title}"
                author_title_freq[author_title] = author_title_freq.get(author_title, 0) +1
        
    if 0:
        with open('pat_authortitle.tmp', 'w') as fout:
            for pat, freq in sorted(author_title_freq.items(), key=lambda x:x[1], reverse=True):
                fout.write(f'{freq} {pat}\n')
        with open('pat.tmp', 'w') as fout:
            for pat, freq in sorted(pat_freq.items(), key=lambda x:x[1], reverse=True):
                fout.write(f'{freq} {pat}\n')


def statistics_author_name():
    reftype = 'RefPatent'
    reftype = 'RefDirectSubmission'
    reftype = 'RefPublication'

    fname = f'authors_{reftype}.dat'
    name_freq = {}
    for line in open(fname):
        p2 = 0
        has_2_ands = 0
        if line.find(' and ')>=0:
            try:
                p1, p2 = line.strip().split(' and ')
            except:
                #print(line)
                p1, p2 = line.strip().split(' and ', 1)
                has_2_ands = 1
        else:
            p1 = line.strip()
        names = p1.split(', ')
        if p2:
            if not has_2_ands:
                names = names + [p2]
            else:
                print(p2)
                names = names + p2.split(' and ')
        for name in names:
            name = name.upper()
            name_freq[name] = name_freq.get(name, 0) +1
    fname_out = f'namefreq_{reftype}.dat'
    with open(fname_out, 'w') as fout:
        for name, freq in sorted(name_freq.items(), key=lambda x:x[1], reverse=True):
            fout.write(f"{freq} {name}\n")

def main():
    tic = time.time()

    #download_seq_gz()
    #get_2013_gi_accession()
    #check_uniqueness_of_accession_number_2013()

    #get_2018_accession_only()
    #check_uniqueness_of_accession_number_2018()

    #compress_patent_references()
    statistics_author_name()

    print(f'time: {time.time()-tic:.1f}s')

if __name__ == "__main__":
    main()
