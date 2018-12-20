import time, os, re, platform
from helper import *


folder_ann_gz = "%s/ann_gz" % DATA_ROOT 
folder_ann_reduced = "%s/ann_reduced" % DATA_ROOT 
folder_aujo = "%s/aujo" % DATA_ROOT 
folder_raw = "/tmp" # *.seq files downloaded from the GenBank ftp site are stored here temporarily

folder_taxonomy = "%s/taxonomy_dump" % DATA_ROOT

#download_folder = "/var/local/genbankdata"
download_folder = f"{DATA_ROOT}/seq_gz"


SEP = "//"

delete_larger_ann_file_after_reduction = True

debug_level = 1
debug_level = 2

debug = False
debug = True

do_it = False
do_it = True

def run(cmd): 
    print(cmd)
    if do_it:
        os.system(cmd)

def mkdir_if_not_exists(folder):
    if not os.path.exists(folder):
        os.makedirs(folder)

def step_1_download_seq_gz(division_max=50):
    url_filesize, foo = read_ftp_index_file()
    div_cnt = {}
    total_fsize = 0
    files = 0
    for url in url_filesize:
        fname = url.split("/")[-1]
        div = fname[2:5]
        div_cnt[div] = div_cnt.get(div, 0) +1
        if div_cnt[div] > division_max:
            continue
        total_fsize += url_filesize[url]
        fpath_seq_gz = "%s/%s" % (download_folder, fname)
        fstem = fname.split(".")[0]
        fpath_ann_gz = "%s/%s.ann.gz" % (folder_ann_gz, fstem)
        files += 1
        #print(fpath_ann_gz)
        #break
        if os.path.exists(fpath_seq_gz) or os.path.exists(fpath_ann_gz):
            continue
        cmd = 'wget "%s" -O "%s"' % (url, fpath_seq_gz)
        run(cmd)
        break

def step_2_sift_annotation_and_then_zip_the_file():
    mkdir_if_not_exists(folder_ann_gz)

    for fname in os.listdir(download_folder):
        fstem = fname.split(".")[0]
        fpath_ann_gz = "%s/%s.ann.gz" % (folder_ann_gz, fstem)
        if os.path.exists(fpath_ann_gz):
            continue
        fpath_seq_gz = "%s/%s" % (download_folder, fname)
        print('processing:', fpath_seq_gz)
        cmd = 'cp "%s" /tmp/%s' % (fpath_seq_gz, fname)
        run(cmd)
        cmd = 'gunzip "/tmp/%s"' % fname
        run(cmd)
        fpath_seq = "/tmp/%s.seq" % fstem
        fpath_ann = "%s/%s.ann" % (folder_ann_gz, fstem)
        sift_annotation_for_one_seq_file(fpath_seq, fpath_ann)
        cmd = 'gzip "%s"' % fpath_ann
        run(cmd)
        cmd = 'rm "%s"' % fpath_seq
        run(cmd)
        #break


def old_download_and_sift():
    url_filesize, foo = read_ftp_index_file()
    cnt = 0
    for url in url_filesize:
        fpath_ann = "%s/%s.ann" % (folder_ann, fname)
        fpath_ann_reduced = "%s/%s.ann.reduced" % (folder_ann_reduced, fname)
        fpath_seq = "%s/%s.seq" % (folder_raw, fname)
        fpath_seq_gz = "%s/%s.seq.gz" % (folder_raw, fname)
        if os.path.exists(fpath_ann_reduced):
            print('already downloaded and reduced "%s"' % fname)
        else:
            cnt += 1
            if cnt > 1:
                break

            if not os.path.exists(fpath_seq):
                if not os.path.exists(fpath_seq_gz):
                    cmd = 'wget "%s" -O "%s"' % (url, fpath_seq_gz)
                    print(cmd)
                    run(cmd)
                cmd = 'gunzip "%s"' % fpath_seq_gz
                print(cmd)
                run(cmd)

            if not os.path.exists(fpath_ann):
                sift_annotation_for_one_seq_file(fpath_seq, fpath_ann)
                print (fpath_seq, fpath_ann)

            reduce_one_ann_files_based_on_all_references("%s.ann"%fname)
            print("reduce ann based on references =>  .ann.reduced")

            cmd = 'rm "%s"' % fpath_seq
            print(cmd)
            run(cmd)

def summarize_references():
    cnt, cnt_pubmed, cnt_published, cnt_unpublished, direct_submitted = 0, 0, 0, 0, 0
    direct_submitted_flag = False
    journal_freq = {}
    pmid_freq = {}
    for fname in os.listdir(folder_aujo):
        if not fname.split(".")[-1] == "uni":
            continue
        fpath = "%s/%s" % (folder_aujo, fname)
        with open(fpath, 'r') as f:
            for line in f.readlines():
                if is_end_of_record(line):
                    cnt += 1
                elif is_TITLE(line) and get_value(line) == "Direct Submission":
                    direct_submitted_flag = True
                    direct_submitted += 1
                elif is_PUBMED(line):
                    cnt_pubmed += 1
                    pmid = get_value(line)
                    pmid_freq[pmid] = pmid_freq.get(pmid, 0) +1
                elif is_JOURNAL(line):
                    journal = get_value(line)
                    if direct_submitted_flag:
                        pass
                    elif journal == "Unpublished":
                        cnt_unpublished += 1
                    else:
                        cnt_published += 1
                        journal_freq[journal] = journal_freq.get(journal, 0) +1
                    direct_submitted_flag = False

    print("total: %s, cnt_pubmed: %s, unique_pmid: %s, cnt_published: %s, unique journals: %s,  cnt_unpublished: %s,  direct_submitted: %s" \
        % (cnt, cnt_pubmed, len(pmid_freq.keys()), cnt_published, len(journal_freq.keys()), cnt_unpublished, direct_submitted))

def identify_and_save_unique_author_journal():
    all_records, all_unique = 0, 0
    for fname in os.listdir(folder_ann_reduced):
        if fname.find("gbpat")>-1: continue
        if fname.split(".")[-1] == "reduced":
            records, author_journal_match_records, unique_author_journal_cnt = identify_and_save_unique_author_journal_for_one_file(fname)
            all_records += records
            all_unique += unique_author_journal_cnt
    print("all: %d   uniques: %d    unique ratio: %.3f      au-jo repeating ratio: %.3f" % (all_records, all_unique, 1.*all_unique/all_records, 1-1.*all_unique/all_records))

def identify_and_save_unique_author_journal_for_one_file(fname):
    fpath = "%s/%s" % (folder_ann_reduced, fname)
    authors, journal, title = "", "", ""
    records, exact_records, partial_records, weird_records, author_journal_match_records = [0] * 5
    exact_duplicate = []
    author_journal_record, aujo_accession = {}, {}
    lines = []
    with open(fpath, 'r') as f:
        for line in f.readlines():
            if line[0] == '#': continue
            lines.append(line)
            if is_ACCESSION(line):
                accession = get_value(line)
                authors, journal = "", ""
            elif is_DEFINITION(line):
                definition = get_value(line)
            elif is_AUTHORS(line):
                authors += get_value(line)
                #authors = get_value(line)
            elif is_TITLE(line):
                title += get_value(line)
            elif is_JOURNAL(line):
                journal += get_value(line)
                #journal = get_value(line)
                #journal = re.sub(r'\d\d-[A-Z]{3}-\d\d\d\d', "", journal)  #(17-APR-1997)
            elif is_end_of_record(line):
                records += 1

                aujo = "%s%s"%(authors, journal)
                aujo = re.sub(r'\W+', '', aujo)

                if aujo not in author_journal_record:
                    author_journal_record[aujo] = "".join(lines)
                    aujo_accession[aujo] = accession
                else:
                    aujo_accession[aujo] += " %s" % accession
                    if debug:
                        author_journal_record[aujo] += "#SKIPPED: %s. SAME AUTHORS & JOURNAL: %s\n" % (accession, aujo_accession[aujo])
                        if debug_level > 1:
                            author_journal_record[aujo] += "".join(lines)

                authors, journal = "", ""
                lines = []

    unique_author_journal_cnt = len(author_journal_record.keys())

    aujo_file = "%s/%s.uni" % (folder_aujo, fname.replace(".ann.reduced", ""))
    pre = '#total records: %s   unique author-journal records: %s\n' % (records, unique_author_journal_cnt)
    open(aujo_file, 'w').write(pre + "".join(author_journal_record.values()))

    print("==", fname)
    #print '  total records:', records, '     exact definition:', exact_records, '    author-journal match:',  author_journal_match_records
    #print '  redundancy ratio: %.3f' % ( author_journal_match_records*1./records )
    print ('  unique_author_journal_cnt: ', unique_author_journal_cnt, " unique author-journal ratio: ", "%.3f" % (1.*unique_author_journal_cnt/records) )
    return (records, author_journal_match_records, unique_author_journal_cnt)

def estimate_avg_annotation_size_per_record():
    fsizes, records = [], 0
    for file_ann in os.listdir(folder_ann):
        fpath = "%s/%s" % (folder_ann, file_ann)
        print(file_ann)
        fsize = os.path.getsize(fpath)
        fsizes.append(fsize)
        with open(fpath, "r") as f:
            for line in f:
                if is_LOCUS(line):
                    records += 1
        #break
    print(fsizes, records)
    print( "%d" % (sum(fsizes)*1./records))


def sift_annotation_for_one_seq_file(fpath_seq, fpath_ann):
    is_seeking_next_record = True
    annotation_buffer = []
    record_cnt = 0
    start = time.clock()
    #with open(fpath_seq, "r") as f:
    with open(fpath_seq, encoding="latin-1") as f:
        for line in f:
            if is_seeking_next_record:
                if not is_LOCUS(line):
                    continue
                else:
                    is_seeking_next_record = False
                    record_cnt += 1
                    annotation_buffer.append(line)
            else:
                if is_FEATURES(line):
                    is_seeking_next_record = True
                    annotation_buffer.append("%s\n"%SEP)
                else:
                    annotation_buffer.append(line)
    
    with open(fpath_ann, 'w') as f:
        f.write("".join(annotation_buffer))

    end = time.clock()
    print("%.2gs" % (end-start))
    print("records: ", record_cnt)

def sift_annotation_one_file(file_raw):
    file_ann = "%s.ann" % file_raw.split(".")[0] 
    fpath_seq = "%s/%s" % (folder_raw, file_raw)
    fpath_ann = "%s/%s" % (folder_ann, file_ann)
    
    sift_annotation_for_one_seq_file(fpath_seq, fpath_ann)

def sift_annotation_all():
    for file_raw in os.listdir(folder_raw):
        if file_raw.split(".")[-1] == "seq":
            sift_annotation_one_file(file_raw)


def merge_two_consecutive_records_if_their_references_are_the_same():
    for file_ann in os.listdir(folder_ann):
        if not file_ann.split(".")[-1] == "ann": 
            continue
        merge_two_records_if_their_references_are_the_same_one_file(file_ann)

def merge_two_records_if_their_references_are_the_same_one_file(file_ann):
    fpath_ann = "%s/%s" % (folder_ann, file_ann)
    ann_reduced_file = "%s/%s.reduced" % (folder_ann_reduced, file_ann)
    if not debug and os.path.exists(ann_reduced_file):
        print('processed: %s' % ann_reduced_file)
        return
    prev_reference_text = None
    records = 0
    lines, out, references = [], [], []
    reference_flag, comment_flag = False, False
    with open(fpath_ann, 'r') as f:
        for line in f.readlines():
            lines.append(line)
            if is_REFERENCE(line):
                reference_flag = True
            elif is_ACCESSION(line):
                accession = get_value(line)
            elif is_COMMENT(line):
                comment_flag = True
            elif reference_flag:
                if not comment_flag:
                    references.append(line)
                if is_end_of_record(line):
                    records += 1
                    reference_text = "".join(references)
                    if not prev_reference_text == reference_text:
                        out.append("".join(lines))
                    elif debug:
                        out.append("#RECORD SKIPPED: %s. ALL OF ITS REFERENCES ARE THE SAME AS THE PREVIOUS ONE.\n"%(accession))
                        if debug_level > 1:
                            out.append("".join(lines))

                    prev_reference_text = reference_text
                    lines, references = [], []
                    reference_flag = False
                    comment_flag = False

    print(file_ann, '  total records: %s     reduced: %s     reduction ratio: %.3f' % (records, len(out), len(out)*1./records))
    pre = '#total records: %s   distinct references: %s\n' % (records, len(out))
    open(ann_reduced_file, 'w').write(pre + "".join(out))

    if delete_larger_ann_file_after_reduction:
        if os.path.getsize(fpath_ann) > 20*1000*1000:
            cmd = "rm '%s'" % fpath_ann
            print(cmd)
            run(cmd)

def delete_ann_files_that_are_larger_than_10M():
    for file_ann in os.listdir(folder_ann):
        if not file_ann.split(".")[-1] == "ann": continue
        fpath_ann = "%s/%s" % (folder_ann, file_ann)
        fsize = os.path.getsize(fpath_ann)
        fsize_limit = 100*1000*1000
        if fsize > fsize_limit:
            cmd = 'rm "%s"' % fpath_ann
            print ('fsize: %.1fM    ' % (fsize*1./1000000), cmd)
            run(cmd)

def gen_taxonomy_csv_files():
    import socket
    if socket.gethostname() == "jwang.local":
        folder_csv = "/Users/jwang72/Documents/JW_big_data/genbank/data/csv"
    elif socket.gethostname() == 'metadatalab.syr.edu':
        folder_csv = "/home/data/genbank/csv"
    else:
        print( "- wrong hostname: ", socket.gethostname())
        return

    folder_taxonomy = "%s/taxonomy_dump" % DATA_ROOT
    file_dmp_nodes = "%s/nodes.dmp" % folder_taxonomy
    file_dmp_names = "%s/names.dmp" % folder_taxonomy
    file_csv_names = "%s/TAXNAME.csv" % folder_csv
    file_csv_nodes = "%s/TAXNODE.csv" % folder_csv

    cnt = 0
    out = []
    maxL = 0
    for line in open(file_dmp_names, 'r').readlines():
        tax_id, name, unique_name, name_class, foo = [x.strip() for x in line.split('|')]
        if not name_class == "scientific name":
            continue
        #print tax_id, name, unique_name, name_class
        out.append("%s|%s" % (tax_id, name))
        if len(name) > maxL:
            maxL = len(name)
            #print tax_id, name
        cnt += 1
        if cnt > 100: break
    #open(file_csv_names, 'w').write("\n".join(out))
    print('max length: ', maxL)

    cnt = 0
    out = []
    for line in open(file_dmp_nodes, 'r').readlines():
        #row = [x.strip() for x in line.split('|')]
        #out.append(row[0:-1])
        row = line.replace("\t", "")[:-2]
        out.append(row)
        cnt += 1
        if cnt > 1000: break
    open(file_csv_nodes, 'w').write("\n".join(out))



def main():
    task = "estimate_avg_annotation_size_per_record()"
    task = "sift_annotation_all()"
    task = "sift_annotation_one_file('gbbct103.seq')"
    #task = "delete_ann_files_that_are_larger_than_10M()" # not tested
    task = "to_do_download_and_sift()"
    task = "identify_and_save_unique_author_journal()"
    task = "merge_two_consecutive_records_if_their_references_are_the_same()"
    task = "summarize_references()"

    task = "step_3_separate_reference_and_other_metadata()"
    task = "step_1_download_seq_gz(division_max=1)"

    task = "gen_taxonomy_csv_files()"

    task = "step_2_sift_annotation_and_then_zip_the_file()"

# TO DO 1: replace identify_and_save_unique_author_journal() : ann => ann.reduced
# TO DO 2: / DON'T PROCESS gbpat
#gbpat / pattern / 1306431 / 1306432
#  AUTHORS   Stephen,S., Anastasia,K., Angela,R., Devin,L. and William,M.
#  TITLE     Functional and Hyperfunctional siRNA
#  JOURNAL   Patent: JP 2006507841-A 1306431 09-MAR-2006;
#
#  AUTHORS   Stephen,S., Anastasia,K., Angela,R., Devin,L. and William,M.
#  TITLE     Functional and Hyperfunctional siRNA
#  JOURNAL   Patent: JP 2006507841-A 1306432 09-MAR-2006;
    

    eval(task)

if __name__ == "__main__":
    main()
