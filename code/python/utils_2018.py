import re, os, time, glob, csv, re, json
import numpy as np
import pandas as pd
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

def post_request(fpath_out, ids):
    ids_str = ','.join(ids)
    from urllib import request, parse
    url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"

    #data = {'db': 'pubmed',  'id': ids_str}
    data = {'db': 'pubmed', 'retmode':'xml', 'id': ids_str}

    data = parse.urlencode(data).encode()
    req =  request.Request(url, data=data) # this will make the method "POST"
    resp = request.urlopen(req)
    result = resp.read().decode('utf-8')
    with open(fpath_out, 'w') as fout:
        fout.write(result)

def fetch_pubmed_author_etc_info():
    #get_pubmed_ids
    p = re.compile(r'^\d\d\d\d+$')
    # note that some pubmed is: Patent: JP 2016509572-A 1 31-MAR-2016;\ PRONAI THER
    # mysql> SELECT DISTINCT pubmed FROM RefPublication WHERE pubmed IS NOT NULL INTO OUTFILE '/tmp/a.csv' LINES TERMINATED BY '\n';

    #ids_all = [line.strip() for line in open('pubmed_ids.dat').readlines()]
    ids_all = [line.strip() for line in open('pubmed_ids.dat').readlines() if p.match(line.strip())]
    total_cnt = len(ids_all)
    print(total_cnt)
    span = 10000
    for start in range(0, total_cnt, span):
        end = start+span
        if end>total_cnt:
            end = total_cnt
        ids_1w = ids_all[start:end]
        
        outfolder = '/home/data/genbank2018/pubmed_300k'
        fpath_out = f"{outfolder}/{start}-{end-1}.xml"
        if os.path.exists(fpath_out): continue
        print(fpath_out)
        #continue
        post_request(fpath_out, ids_1w)
        #break

def parse_pubmed_author_etc_info():
    import xmltodict
    folder_xml = f"{DATA_ROOT}/pubmed_300k" 
    #for fpath in glob.glob(f'{folder_xml}/a*.xml'):
    for fpath in glob.glob(f'{folder_xml}/*-*.xml'):
        print(fpath)
        fout = fpath.replace('.xml', '.csv')
        if os.path.exists(fout): continue
        xmlData = xmltodict.parse(open(fpath).read())
        articles = xmlData['PubmedArticleSet']['PubmedArticle']

        out = []
        article_errors = 0
        for article in articles:
            
          try:
            authors = article['MedlineCitation']['Article']['AuthorList']['Author']
            pmid = article['MedlineCitation']['PMID']['#text']

            aff = ""
            if not isinstance(authors, list):
                authors = [authors]
            for au in authors:
                if 'ForeName' in au:
                    name = f"{au['LastName']}, {au['ForeName']}"
                    #name = f"{au['LastName']}, {au['Initials']}"
                else:
                    name = au['LastName']
                row = {'name': name, 'pmid':pmid, 'affiliation':'', 'confidence':0}
                if 'AffiliationInfo' in au:
                    aff = au['AffiliationInfo']['Affiliation']
                    row['affiliation'] = aff
                    row['confidence'] = 1
                else:
                    row['affiliation'] = aff
                out.append(row)
          except:
            article_errors +=1
            #print(article)
        pd.DataFrame(out)['name confidence affiliation pmid'.split()].to_csv(fout, index=False)
        #break
    print(f'article_errors: {article_errors}')

def statistics_pubmed_author_name():
    folder_pubmed = f"{DATA_ROOT}/pubmed_300k" 
    name_freq = {}
    name_cnt = 0
    for fpath in glob.glob(f'{folder_pubmed}/*-*.csv'):
        df = pd.read_csv(fpath, usecols=['name'])
        for name in df['name'].tolist():
            name_freq[name] = name_freq.get(name, 0) +1
            name_cnt += 1
    out = []
    for name, freq in sorted(name_freq.items(), key=lambda x:x[1], reverse=True):
        out.append({"name":name, "freq":freq})
    pd.DataFrame(out).to_csv('/tmp/name.csv', index=False)
    print(f'name occurrences: {name_cnt}\ndistinct names: {len(out)}')

def patentsvieworg_process():
    folder = f"{DATA_ROOT}/patentsview_org" 
    # SELECT DISTINCT journal FROM RefPatent INTO OUTFILE '/tmp/genbank_patent_journal.dat' LINES TERMINATED BY '\n';
    def patent_extract_patent_number_from_genbank():
        fpath = f"{folder}/genbank_patent_journal.dat"
        p = re.compile(r'Patent: US \(?([0-9,]{5,})[^,0-9]')
        p2 = re.compile(r'Patent: US .*?/([0-9,]{5,})[^,0-9]')
        #Patent: US 5589355-A 31-DEC-1996;
        ids = []
        for line in open(fpath).readlines():
            if not line.startswith('Patent: US'):
                continue
            line = line.strip()
            m = p.match(line)
            patent_id = False
            if m:
                patent_id = m.groups()[0]
            else:
                m = p2.match(line)
                if m:
                    patent_id = m.groups()[0]
                else:
                    print(line)
            if patent_id:
                if patent_id.find(',')>0:
                    patent_id = patent_id.replace(',', '')
                ids.append(patent_id)
        print(len(ids))
        return ids
        #open('/tmp/a', 'w').write('\n'.join(ids))

    def read_patentsview_patent_tsv():
        fpath = f"{folder}/patent_simple.tsv"
        # id	type	country	date	title	kind
        df = pd.read_csv(fpath, sep='\t', usecols=['id'])
        df['id'] = df['id'].astype('str')
        return set(df['id'].tolist())
        
    def gen_involved_patent_ids_step1():
        print('I really dont need this step')
        return
        ids_7million = read_patentsview_patent_tsv() # 6.7 million
        patent_ids = patent_extract_patent_number_from_genbank()
        ids_matched = [id for id in patent_ids if id in ids_7million]
        print('matched ids:', len(ids_matched))
        open(fout_path, 'w').write(' '.join(ids_matched))

    def read_patent_inventor_tsv():
        fin_path = f'{folder}/genbank_patent_inventor.tsv'
        inventor_patents = {}
        patent_inventors = {}
        for line in open(fin_path):
            patent_id, inventor_id = line.strip().split()
            if inventor_id not in inventor_patents:
                inventor_patents[inventor_id] = []
            inventor_patents[inventor_id].append(patent_id)
            if patent_id not in patent_inventors:
                patent_inventors[patent_id] = []
            patent_inventors[patent_id].append(inventor_id)
        return inventor_patents, patent_inventors

    def gen_involved_inventors_1():
        patent_ids = patent_extract_patent_number_from_genbank()
        patent_ids = set(patent_ids)
        inventor_ids = {}
        if 0:
            fout_path = f'{folder}/genbank_patent_inventor.tsv'
            with open(fout_path, 'w') as fout:
                for line in open(f'{folder}/patent_inventor.tsv'):
                    patent_id, inventor_id = line.strip().split()
                    inventor_ids[inventor_id] = 1
                    if patent_id in patent_ids:
                        fout.write(line)

    def gen_involved_inventors_2():
        inventor_patents, patent_inventors = read_patent_inventor_tsv()
        if 0:
            fout_path = f'{folder}/genbank_inventor.tsv'
            with open(fout_path, 'w') as fout:
                for line in open(f'{folder}/inventor.tsv'):
                    try:
                        inventor_id, name_first, name_last = line.strip().split('\t')
                        if inventor_id in inventor_patents:
                            fout.write(line)
                    except:
                        print(line.strip())

    def gen_involved_assignees():
        inventor_patents, patent_inventors = read_patent_inventor_tsv()
        if 1:
            df = pd.read_csv( f'{folder}/assignee.tsv', sep='\t', usecols='id organization'.split() )
            assignee_id_org = {x:y for x, y in zip(df.id, df.organization)}

            fout_path = f'{folder}/genbank_patent_assignee.tsv'
            with open(fout_path, 'w') as fout:
                for line in open(f'{folder}/patent_assignee.tsv'):
                    try:
                        pid, aid = line.strip().split('\t')
                        if pid in patent_inventors:
                            ll = f'{line.strip()}\t{assignee_id_org[aid]}\n'
                            fout.write(ll)
                    except:
                        print(line.strip())

    def read_genbank_patent_assignee():
        df = pd.read_csv(f'{folder}/genbank_patent_assignee.tsv', sep='\t', header=None, names='pid aid org'.split())
        df.dropna(inplace=True)
        p_orgs = {}
        for p, org in zip(df.pid, df.org):
            p = str(p)
            if p not in p_orgs: p_orgs[p] = []
            p_orgs[p].append(org)
        return p_orgs


    def gen_inventor_table():
        df = pd.read_csv( f'{folder}/genbank_inventor.tsv', sep='\t', header=None, names='iid first last'.split())
        inventor_name = {iid:f'{last}, {first}' for iid, first, last in zip(df['iid'], df['first'], df['last'])}
        print(len(inventor_name))
        name_freq = {}

        patent_orgs = read_genbank_patent_assignee()
        inventor_patents, patent_inventors = read_patent_inventor_tsv()
        inventor_info = {}
        out = []
        for i1, patent_ids in sorted(inventor_patents.items(), key=lambda x:len(x[1]), reverse=True):
            orgs = []
            for pid in patent_ids:
                if pid in patent_orgs:
                    orgs += patent_orgs[pid]
            org_cnt = {}
            for org in orgs:
                org_cnt[org] = org_cnt.get(org, 0) +1
            orgs = json.dumps(org_cnt)
            name = inventor_name[i1]
            name_freq[name] = name_freq.get(name, 0) +1
            patents = ' '.join(patent_ids)
            out.append({'inventor_id':i1, 'name':name, 'orgs':orgs, 'patents':patents})

        fout_path = f'{folder}/genbank_inventor_detail.csv'
        pd.DataFrame(out).to_csv(fout_path, index=False)

        cnt = 0
        for name, freq in sorted(name_freq.items(), key=lambda x:x[1], reverse=True):
            print(name, freq)
            cnt += 1
            if cnt>10: break



    def gen_edge_table():
        fout_path = f'{folder}/genbank_inventor_inventor.csv'
        inventor_patents, patent_inventors = read_patent_inventor_tsv()

        ii_freq = {}
        for i1, patent_ids in inventor_patents.items():
            #if not i1=='3974131-3': continue
            #print(patent_ids)
            for p in patent_ids:
                for i2 in patent_inventors[p]:
                    if i1==i2: continue
                    #if i1>i2: i1, i2 = i2, i1
                    k = f'{i1} {i2}'
                    #print(p, k)
                    ii_freq[k] = ii_freq.get(k, 0)+1

        with open(fout_path, 'w') as fout:
            for ii, freq in sorted(ii_freq.items(), key=lambda x:x[1], reverse=True):
                i1, i2 = ii.split()
                if i1>i2: continue # avoid duplicates
                fout.write(f'{ii} {freq}\n')


    #gen_involved_patent_ids_step1() # not need this step

    #gen_involved_inventors_1()
    #gen_involved_inventors_2()
    #gen_involved_assignees()

    #gen_edge_table()
    gen_inventor_table()



def main():
    tic = time.time()

    #download_seq_gz()
    #get_2013_gi_accession()
    #check_uniqueness_of_accession_number_2013()

    #get_2018_accession_only()
    #check_uniqueness_of_accession_number_2018()

    #compress_patent_references()
    #statistics_author_name()
    
    #fetch_pubmed_author_etc_info()
    #parse_pubmed_author_etc_info()
    #statistics_pubmed_author_name()

    patentsvieworg_process()

    print(f'time: {time.time()-tic:.1f}s')

if __name__ == "__main__":
    main()
