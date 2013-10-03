<?php
include './helper.php';
$mysqli = connect_database();


$task_create_all_tables = 1; 
$task_load_csv_files_to_database = 2; 

$task = $task_create_all_tables;
$task = $task_load_csv_files_to_database;

if ($task == $task_create_all_tables) 
{
    create_all_tables($mysqli);
}
elseif ($task == $task_load_csv_files_to_database)
{
    load_csv_files($mysqli);
}
else
    print "wrong task\n";


// -----------------------------------------------
function load_csv_files($mysqli)
{
    $conf = read_config();
    $csv_dir = $conf["csv_dir"];

    $tables = array('Reference');
    $tables = array('Reference', 'Annotation', 'AnnotationReference', 'Keywords', 'Source', 'Comment', 'Organism', 'Dblink');
    foreach ($tables as $table)
    {
        $null_clause = "";
        if ($table == "Annotation")
        {
            $null_clause = "(gi, version, @keywordsId, sourceId, organismId, @commentId, @dblinkId, locus, accession, definition, @segment) SET keywordsId = nullif(@keywordsId,''), commentId = nullif(@commentId,''), dblinkId = nullif(@dblinkId,''), segment = nullif(@segment,'')";
        }
        elseif ($table == "Reference")
        {
            $null_clause = "(id, reference, @authors, @consortium, title, journal, @pubmed, @remark) SET authors = nullif(@authors,''), consortium = nullif(@consortium,''), pubmed = nullif(@pubmed,''), remark = nullif(@remark,'')";
        }

        $csv_fpath = $csv_dir . "/" . (($table == "AnnotationReference") ? "ANNOTATION_REFERENCE" : strtoupper($table)) . ".csv";
        $sql = "LOAD DATA LOCAL INFILE '$csv_fpath' INTO TABLE genbank.$table FIELDS TERMINATED BY '|' ENCLOSED BY '\"' " . $null_clause ; 

        print("$sql\n");
        $mysqli->query($sql);
    }
}


// -----------------------------------------------
function create_all_tables($mysqli)
{
    $tables = array('Reference', 'Annotation', 'AnnotationReference', 'Keywords', 'Source', 'Comment', 'Organism', 'Dblink');
    $tables = array('AnnotationReference');
    foreach ($tables as $table) {
        create_table($mysqli, $table);
    }
}


function create_table($mysqli, $table) 
{
    $sql = "DROP TABLE IF EXISTS " . $table;
    $mysqli->query($sql);

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
            PRIMARY KEY(id)
        )";
    }
    else if ($table == "Source") {
        $sql = "CREATE TABLE IF NOT EXISTS " . $table . " (
            id INT,
            content VARCHAR(5000),
            PRIMARY KEY(id)
        )";
    }
    else if ($table == "Organism") {
        $sql = "CREATE TABLE IF NOT EXISTS " . $table . " (
            id INT,
            content VARCHAR(5000),
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

    if ($mysqli->query($sql) === TRUE) {
        echo "Table \"" . $table . "\" successfully created.\n";
    } else {
        echo "Failed to create Table \"" . $table . "\".\n";
    }
}


?>
