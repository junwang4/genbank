debug = True
debug = False

import ConfigParser
config = ConfigParser.ConfigParser()
config.read('py.conf')

config_option = "DEPLOY"
config_option = "TEST"

def gen_taxonomy_csv_files(task):
    csv_dir = config.get(config_option, 'csv_dir')
    taxonomy_dir = config.get(config_option, 'taxonomy_dir')
    print "csv_dir: %s\ntaxonomy_dir: %s\n" % (csv_dir, taxonomy_dir)

    def read_names():
        id_name = {}
        file_dmp_names = "%s/names.dmp" % taxonomy_dir
        #file_csv_names = "%s/TAXNAME.csv" % csv_dir
        cnt = 0
        maxL = 0
        for line in open(file_dmp_names, 'r').readlines():
            tax_id, name, unique_name, name_class, foo = [x.strip() for x in line.split('|')]
            if not name_class == "scientific name":
                continue
            #out.append("%s|%s" % (tax_id, name))
            id_name[tax_id] = name
            if len(name) > maxL: maxL = len(name)
            cnt += 1
            if debug and cnt > 100: break
        print 'max length: ', maxL 
        return id_name

    def write_csv_for_nodes():
        id_name = read_names()
        file_dmp_nodes = "%s/nodes.dmp" % taxonomy_dir
        file_csv_nodes = "%s/TAXNODE.csv" % csv_dir
        cnt = 0 
        out = []
        for line in open(file_dmp_nodes, 'r').readlines():
            row = line.replace("\t", "")[:-2].split("|")
            id, pid = row[0], row[1]
            name, pname = id_name[id], id_name[pid]
            new_row = [id, name, pid, pname]
            new_row.extend(row[2:])
            out.append("|".join(new_row))
            cnt += 1
            if debug and cnt > 100: break
        open(file_csv_nodes, 'w').write("\n".join(out))

    def write_csv_for_divisions():
        file_dmp = "%s/division.dmp" % taxonomy_dir
        file_csv = "%s/TAXDIVISION.csv" % csv_dir
        out = []
        for line in open(file_dmp, 'r').readlines():
            row = line.replace("\t", "")[:-2]
            out.append(row)
        open(file_csv, 'w').write("\n".join(out))

    if task == "create_TAXNODE_csv":
        write_csv_for_nodes()
    elif task == "create_TAXDIVISION_csv":
        write_csv_for_divisions()

def analyze_taxonomy():
    file_csv_nodes = "%s/TAXNODE.csv" % csv_dir

if __name__ == "__main__":
    #gen_taxonomy_csv_files(task='create_TAXDIVISION_csv')
    gen_taxonomy_csv_files(task='create_TAXNODE_csv')
    #analyze_taxonomy()
