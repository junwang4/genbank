<?php
include './helper.php';
$mysqli = connect_database();


$task_create_all_tables = 1; 
$task_create_taxonomy_tables = 11; 
$task_load_csv_files_to_database = 2; 
$task_add_indexes_to_tables = 3; 
$task_split_patent_Reference = 14;

#$task = $task_add_locus_detailed_fields_to_Annotation;
#$task = $task_add_indexes_to_tables;
$task = $task_create_taxonomy_tables;
$task = $task_create_all_tables;
$task = $task_load_csv_files_to_database;
$task = $task_split_patent_Reference;


if ($task == $task_create_all_tables) 
{
    create_all_tables($mysqli);
}
elseif ($task == $task_load_csv_files_to_database)
{
    load_csv_files($mysqli);
}
elseif ($task == $task_add_indexes_to_tables)
{
    add_indexes_to_tables($mysqli);
}
elseif ($task == $task_create_taxonomy_tables)
{
    create_taxonomy_tables($mysqli);
}
elseif ($task == $task_add_locus_detailed_fields_to_Annotation)
{
    add_locus_detailed_fields_to_Annotation($mysqli);
}
elseif ($task == $task_split_patent_Reference) 
{
    split_patent_reference($mysqli);
}
else
    print "wrong task\n";


// -----------------------------------------------
function split_patent_reference($mysqli) 
{
    $sql = "CREATE TABLE Reference_backup LIKE Reference";
    run_sql($sql, $mysqli);

    $sql = "INSERT INTO Reference_backup SELECT * FROM Reference";
    run_sql($sql, $mysqli);

    $sql = "CREATE TABLE ReferencePatent LIKE Reference";
    run_sql($sql, $mysqli);

    $sql = "INSERT INTO ReferencePatent SELECT * FROM Reference WHERE journal LIKE 'Patent%'";
    run_sql($sql, $mysqli);

    $sql = "DELETE FROM Reference WHERE journal LIKE 'Patent%'";
    run_sql($sql, $mysqli);
}

function add_locus_detailed_fields_to_Annotation($mysqli)
{

    $field_datatype = array(
        'locus_name' => "CHAR(16)", 
        'locus_sequence_length' => "INT",
        'locus_sequence_strands' => "CHAR(3)",
        'locus_nucleic_acid_type' => "CHAR(6)", 
        'locus_linear_circular' => "CHAR(8)", 
        'locus_division_code' => "CHAR(3)", 
        'locus_date' => "DATE"
    ); 
       
    $is_drop_fields_only = true;
    $is_drop_fields_only = false;
    if ($is_drop_fields_only)
    {
        foreach ($field_datatype as $field => $datatype )
        {
            $sql = "ALTER TABLE Annotation DROP $field";
            print "$sql\n";
            if (! $mysqli->query($sql) ) {
                print "  - FAILED\n\n";
            }
        }
        return;
    }

    foreach ($field_datatype as $field => $datatype )
    {
        $sql = "ALTER TABLE Annotation ADD $field $datatype, ADD INDEX ($field)";
        print "$sql\n";
        if (! $mysqli->query($sql) ) {
            print "  - FAILED\n\n";
        }
    }
}


function load_csv_files($mysqli)
{
    $conf = read_config();
    $csv_dir = $conf["csv_dir"];

    $tables = array('taxNode');
    $tables = array('taxDivision');
    $tables = array('Reference', 'Annotation', 'AnnotationReference', 'Keywords', 'Source', 'Comment', 'Organism', 'Dblink');
    $tables = array('Reference');
    foreach ($tables as $table)
    {
        $null_clause = "";
        if ($table == "Annotation")
        {
            $null_clause = "(gi, version, @keywordsId, sourceId, organismId, @commentId, @dblinkId, locus, accession, definition, @segment, locus_name, locus_sequence_length, @locus_sequence_strands, locus_nucleic_acid_type, locus_linear_circular, locus_division_code, @locus_date) SET keywordsId = nullif(@keywordsId,''), commentId = nullif(@commentId,''), dblinkId = nullif(@dblinkId,''), segment = nullif(@segment,''), locus_sequence_strands = nullif(@locus_sequence_strands,''), locus_date = STR_TO_DATE(@locus_date, '%d-%b-%Y')";
        }
        elseif ($table == "Reference")
        {
            $null_clause = "(id, reference, @authors, @consortium, title, journal, @pubmed, @remark) SET authors = nullif(@authors,''), consortium = nullif(@consortium,''), pubmed = nullif(@pubmed,''), remark = nullif(@remark,'')";
        }
        elseif ($table == "taxNode")
        {
            $null_clause = "(id, name, parent_id, parent_name, @rank, @locus_prefix, division_id, inherited_div_flag, genetic_code_id, inherited_GC_flag, mitochondrial_genetic_code_id, inherited_MGC_flag, GenBank_hidden_flag, hidden_subtree_root_flag, @comments) SET rank = nullif(@rank,''), locus_prefix = nullif(@locus_prefix,''), comments = nullif(@comments,'')";
        }

        $csv_fpath = $csv_dir . "/" . (($table == "AnnotationReference") ? "ANNOTATION_REFERENCE" : strtoupper($table)) . ".csv";
        $sql = "LOAD DATA LOCAL INFILE '$csv_fpath' INTO TABLE genbank.$table FIELDS TERMINATED BY '|' ENCLOSED BY '\"' " . $null_clause ; 

        print("$sql\n");
        if (! $mysqli->query($sql) ) 
        {
            print "     FAILED\n\n";
        }
    }
}


// -----------------------------------------------
function add_indexes_to_tables($mysqli)
{
    $table_indexes = array("table" => "Annotation", "indexes" => array("keywordsId", "sourceId", "organismId", "commentId", "dblinkId", "accession", "definition", "locus", "segment", "version"));
    $table_indexes = array("table" => "Reference", "indexes" => array("reference", "authors", "consortium", "title", "journal", "pubmed"));

    foreach ($table_indexes["indexes"] as $index)
    {
        $sql = "CREATE INDEX " . $index . " ON " . $table_indexes["table"] . " (" . $index . ")";
        print "$sql\n";
        $mysqli->query($sql);
    }
}


// -----------------------------------------------
function create_all_tables($mysqli)
{
    $tables = array('AnnotationReference');
    $tables = array('Reference', 'Annotation', 'AnnotationReference', 'Keywords', 'Source', 'Comment', 'Organism', 'Dblink');
    $tables = array('Annotation');
    $tables = array('Reference');
    foreach ($tables as $table) {
        create_table($mysqli, $table);
    }
}

function create_taxonomy_tables($mysqli)
{
    $tables = array('taxNode');
    $tables = array('taxDivision');
    foreach ($tables as $table) {
        create_table($mysqli, $table);
    }
}

function create_table($mysqli, $table) 
{
    $sql = "SET foreign_key_checks = 0";
    $mysqli->query($sql);

    $sql = "DROP TABLE IF EXISTS " . $table;
    if (! $mysqli->query($sql)) 
    {
        print " - FALIED: $sql\n\n";
    }

    if ($table == "Reference") {
        $sql = "CREATE TABLE IF NOT EXISTS " . $table . " (
            id BIGINT,
            reference VARCHAR(200),
            authors VARCHAR(10000),
            consortium VARCHAR(1000),
            title VARCHAR(1000),
            journal VARCHAR(5000),
            pubmed VARCHAR(50),
            remark VARCHAR(1000),
            INDEX (reference),
            INDEX (authors),
            INDEX (consortium),
            INDEX (title),
            INDEX (journal),
            INDEX (pubmed),
            PRIMARY KEY (id)
        )";
    } 
    else if ($table == "Annotation") {
        $sql = "CREATE TABLE IF NOT EXISTS " . $table . " (
            gi BIGINT, 
            version INT,
            keywordsId INT,
            sourceId INT,
            organismId INT,
            commentId INT,
            dblinkId INT,
            locus VARCHAR(500),
            accession VARCHAR(50),
            definition VARCHAR(500),
            segment VARCHAR(50),
            locus_name CHAR(16), 
            locus_sequence_length INT,
            locus_sequence_strands CHAR(3),
            locus_nucleic_acid_type CHAR(6), 
            locus_linear_circular CHAR(8), 
            locus_division_code CHAR(3), 
            locus_date DATE,
            INDEX (keywordsId),
            INDEX (sourceId),
            INDEX (organismId),
            INDEX (commentId),
            INDEX (dblinkId),
            INDEX (accession),
            INDEX (definition),
            INDEX (locus),
            INDEX (segment),
            INDEX (version),
            INDEX (locus_name),
            INDEX (locus_sequence_length),
            INDEX (locus_sequence_strands),
            INDEX (locus_nucleic_acid_type),
            INDEX (locus_linear_circular),
            INDEX (locus_division_code),
            INDEX (locus_date),
            PRIMARY KEY (gi)
        )";
    }
    else if ($table == "AnnotationReference") {
        //keyId BIGINT AUTO_INCREMENT,
        $sql = "CREATE TABLE IF NOT EXISTS " . $table . " (
            keyId BIGINT,
            gi BIGINT,
            referenceId BIGINT,
            PRIMARY KEY (keyId),
            INDEX (gi),
            INDEX (referenceId),
            FOREIGN KEY (gi) REFERENCES Annotation(gi),
            FOREIGN KEY (referenceId) REFERENCES Reference(id)
        )";
    }
    else if ($table == "Keywords") {
        $sql = "CREATE TABLE IF NOT EXISTS " . $table . " (
            id INT,
            content VARCHAR(5000),
            INDEX (content),
            PRIMARY KEY(id)
        )";
    }
    else if ($table == "Source") {
        $sql = "CREATE TABLE IF NOT EXISTS " . $table . " (
            id INT,
            content VARCHAR(5000),
            INDEX (content),
            PRIMARY KEY(id)
        )";
    }
    else if ($table == "Organism") {
        $sql = "CREATE TABLE IF NOT EXISTS " . $table . " (
            id INT,
            content VARCHAR(5000),
            INDEX (content),
            PRIMARY KEY(id)
        )";
    }
    else if ($table == "Comment") {
        $sql = "CREATE TABLE IF NOT EXISTS " . $table . " (
            id INT,
            content VARCHAR(5000),
            PRIMARY KEY(id)
        )";
    }
    else if ($table == "Dblink") {
        $sql = "CREATE TABLE IF NOT EXISTS " . $table . " (
            id INT,
            content VARCHAR(5000),
            PRIMARY KEY(id)
        )";
    }

    // taxonomy
    /*else if ($table == "taxName") {
        $sql = "CREATE TABLE IF NOT EXISTS " . $table . " (
            tax_id INT,
            name VARCHAR(200),
            INDEX (name),
            PRIMARY KEY(tax_id)
        )";
    }
     */
    else if ($table == "taxNode") {
        $sql = "CREATE TABLE IF NOT EXISTS " . $table . " (
            id INT,
            name VARCHAR(200),
            parent_id INT,
            parent_name VARCHAR(200),
            rank VARCHAR(100),
            locus_prefix CHAR(2),
            division_id INT,
            inherited_div_flag INT,
            genetic_code_id INT,
            inherited_GC_flag INT,
            mitochondrial_genetic_code_id INT,
            inherited_MGC_flag INT,
            GenBank_hidden_flag INT,
            hidden_subtree_root_flag INT,
            comments VARCHAR(1000),
            PRIMARY KEY(id),
            INDEX (name),
            INDEX (parent_id),
            INDEX (parent_name),
            INDEX (rank),
            INDEX (locus_prefix),
            INDEX (division_id),
            INDEX (genetic_code_id),
            INDEX (mitochondrial_genetic_code_id)
        )";
    }
    else if ($table == "taxDivision") {
        $sql = "CREATE TABLE IF NOT EXISTS " . $table . " (
            id INT,
            code CHAR(3),
            name VARCHAR(200),
            comments VARCHAR(1000),
            INDEX (code),
            INDEX (name),
            PRIMARY KEY(id)
        )";
    }
    

    if (! $mysqli->query($sql))
    {
        echo "Failed to create Table \"" . $table . "\".\n";
    } else {
        echo "Table \"" . $table . "\" successfully created.\n";
    }

    $sql = "SET foreign_key_checks = 1";
    $mysqli->query($sql);
}

function run_sql($sql, $mysqli)
{
    print "$sql\n";
    if (! $mysqli->query($sql) ) {
        print "  - FAILED\n\n";
    }
}

?>
