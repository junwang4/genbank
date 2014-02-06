<?php

define("GENBANK_RECORD_SEP", "//");
define("KEYWORD_COLUMNS", 12);

define("LOCUS", "LOCUS");
define("DEFINITION", "DEFINITION");
define("ACCESSION", "ACCESSION");
define("VERSION", "VERSION");
define("GI", "GI");
define("KEYWORDS", "KEYWORDS");
define("SEGMENT", "SEGMENT");
define("DBLINK", "DBLINK");
define("SOURCE", "SOURCE");
define("ORGANISM", "ORGANISM");
define("COMMENT", "COMMENT");
define("REFERENCE", "REFERENCE");
define("AUTHORS", "AUTHORS");
define("CONSRTM", "CONSRTM");
define("TITLE", "TITLE");
define("JOURNAL", "JOURNAL");
define("PUBMED", "PUBMED");
define("REMARK", "REMARK");

define("ANNOTATION", "ANNOTATION");
define("REFERENCE", "REFERENCE");
define("ANNOTATION_REFERENCE", "ANNOTATION_REFERENCE");
define("COMMENT", "COMMENT");
define("KEYWORDS", "KEYWORDS");
define("ORGANISM", "ORGANISM");
define("SOURCE", "SOURCE");
define("DBLINK", "DBLINK");


function connect_database() 
{
    $conf = read_config();
    print_r($conf);
    $mysqli = new mysqli($conf['host'], $conf['user'], $conf['password'], $conf['database']);
    if ($mysqli->connect_errno) {
        echo "Failed to connect to MySQL: (" . $mysqli->connect_errno . ") " . $mysqli->connect_error;
        exit();
    } else {
        echo "successfully connect to database: ". $conf['database'] . "\n";
    }
    return $mysqli;
}

function read_config()
{
    #$fileConf = (gethostname() == "metadatalab.syr.edu" || gethostname() == "vdatalab.syr.edu") ? "remote.conf" : "local.conf";
    $fileConf = "genbank.conf";
    if (!($conf = parse_ini_file($fileConf))) {
        throw new Exception("\nConfiuration file not found: " . $fileConf);
    }
    return $conf;
}
