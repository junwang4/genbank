debug = True
debug = False

def gen_taxonomy_csv_files():
    import socket
    if socket.gethostname() == "jwang.local":
        folder_csv = "/Users/jwang72/Documents/JW_big_data/genbank/data/csv"
        folder_taxonomy = "../data/taxonomy_dump"
    elif socket.gethostname() == 'metadatalab.syr.edu':
        folder_csv = "/home/data/genbank/csv"
        folder_taxonomy = "/home/data/genbank/taxonomy"
    else:   
        print "- wrong hostname: ", socket.gethostname()
        return  

    def read_names():
        id_name = {}
        file_dmp_names = "%s/names.dmp" % folder_taxonomy
        #file_csv_names = "%s/TAXNAME.csv" % folder_csv
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
        file_dmp_nodes = "%s/nodes.dmp" % folder_taxonomy
        file_csv_nodes = "%s/TAXNODE.csv" % folder_csv
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
        file_dmp = "%s/division.dmp" % folder_taxonomy
        file_csv = "%s/TAXDIVISION.csv" % folder_csv
        out = []
        for line in open(file_dmp, 'r').readlines():
            row = line.replace("\t", "")[:-2]
            out.append(row)
        open(file_csv, 'w').write("\n".join(out))

    #write_csv_for_nodes()

    write_csv_for_divisions()

if __name__ == "__main__":
    gen_taxonomy_csv_files()
