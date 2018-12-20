import re, os

SEP = "//"
PREV_KEY = ""

#keyword: position 1 to 12;  value: positions 13 to 80
KEYWORD_COLUMNS = 12
LINE_COLUMNS = 80
STR_LOCUS = "LOCUS"
STR_DEFINITION = "DEFINITION"
STR_ACCESSION = "ACCESSION"
STR_SEGMENT = "SEGMENT"
STR_KEYWORDS = "KEYWORDS"
STR_DBLINK = "DBLINK"
STR_SOURCE = "SOURCE"
STR_ORGANISM = "  ORGANISM"
STR_VERSION = "VERSION"
STR_FEATURES = "FEATURES"
STR_REFERENCE = "REFERENCE"
STR_COMMENT = "COMMENT"
STR_AUTHORS = "  AUTHORS"
STR_TITLE   = "  TITLE"
STR_JOURNAL = "  JOURNAL"
STR_PUBMED  = "   PUBMED"
STR_REMARK  = "  REMARK"
STR_SUBMITTED = "Submitted"
LEN_LOCUS = len(STR_LOCUS)
LEN_FEATURES = len(STR_FEATURES)
LEN_DBLINK = len(STR_DBLINK)
LEN_SEGMENT = len(STR_SEGMENT)
LEN_KEYWORDS = len(STR_KEYWORDS)
LEN_SOURCE = len(STR_SOURCE)
LEN_ORGANISM = len(STR_ORGANISM)
LEN_REFERENCE = len(STR_REFERENCE)
LEN_COMMENT = len(STR_COMMENT)
LEN_AUTHORS = len(STR_AUTHORS)
LEN_DEFINITION = len(STR_DEFINITION)
LEN_ACCESSION = len(STR_ACCESSION)
LEN_VERSION = len(STR_VERSION)
LEN_JOURNAL = len(STR_JOURNAL)
LEN_TITLE = len(STR_TITLE)
LEN_PUBMED = len(STR_PUBMED)
LEN_REMARK = len(STR_REMARK)
LEN_SUBMITTED = len(STR_SUBMITTED)

def get_value(line): return line[KEYWORD_COLUMNS:] #.strip()

def has_key(line, key_str): 
    global PREV_KEY
    key_part = line[0:len(key_str)]
    result = key_part == key_str or (PREV_KEY == key_str and not key_part.strip())
    if result:
        PREV_KEY = key_str
    return result

def is_LOCUS(line): return line[0:LEN_LOCUS] == STR_LOCUS
def is_ACCESSION(line): return line[0:LEN_ACCESSION] == STR_ACCESSION
def is_VERSION(line): return line[0:LEN_VERSION] == STR_VERSION
def is_FEATURES(line): return line[0:LEN_FEATURES] == STR_FEATURES
def is_DBLINK(line): return line[0:LEN_DBLINK] == STR_DBLINK
def is_SEGMENT(line): return line[0:LEN_SEGMENT] == STR_SEGMENT

def is_KEYWORDS(line): return has_key(line, STR_KEYWORDS)
def is_SOURCE(line): return line[0:LEN_SOURCE] == STR_SOURCE
def is_ORGANISM(line): return line[0:LEN_ORGANISM] == STR_ORGANISM
def is_REFERENCE(line): return line[0:LEN_REFERENCE] == STR_REFERENCE
def is_COMMENT(line): return line[0:LEN_COMMENT] == STR_COMMENT
def is_AUTHORS(line): return line[0:LEN_AUTHORS] == STR_AUTHORS
def is_TITLE(line): return line[0:LEN_TITLE] == STR_TITLE
def is_DEFINITION(line): return line[0:LEN_DEFINITION] == STR_DEFINITION
def is_JOURNAL(line): return line[0:LEN_JOURNAL] == STR_JOURNAL
def is_PUBMED(line): return line[0:LEN_PUBMED] == STR_PUBMED
def is_REMARK(line): return line[0:LEN_REMARK] == STR_REMARK
def is_end_of_record(line): return line[0:2] == "//"



DATA_ROOT = "../data"
DATA_ROOT = ".."

def read_ftp_index_file():
    index_file = "%s/ftp-index.html" % DATA_ROOT
    url_filesize = {}
    div_filesize = {}
    for line in open(index_file, 'r').readlines():
        #15   2013 Jun 17 19:39  File        <a href="ftp://bio-mirror.net:21/biomirror/genbank/gbbct1.seq.gz">gbbct1.seq.gz</a>  (41,704,292 bytes)
        m = re.search(r'<a href="(ftp:.*?seq.gz)">gb([a-z]{3})\d+.*?\.seq\.gz.*?\((.*?) bytes', line)
        if m:
            url, div, size = m.groups()
            fsize = int(size.replace(",", ""))/1000000
            url_filesize[url] = fsize
            div_filesize[div] = div_filesize.get(div, 0) + fsize
    return url_filesize, div_filesize

def mkdir_if_not_exists(folder):
    if not os.path.exists(folder):
        os.makedirs(folder)

def main():
    url_filesize, div_filesize = read_ftp_index_file()
    print(sum(url_filesize.values()))
    print(div_filesize.items())

if __name__ == "__main__":
    main()
