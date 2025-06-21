<?php
include_once('config.php');
$sourcedir = temporary_csv_filtering_getsourcedir();
$destdir = temporary_csv_filtering_getdestdir();
$state = temporary_csv_filtering_getsourcefileprefix();
$output_key = temporary_csv_filtering_outputkey();
$remove_dups = temporary_csv_filtering_removedups();
/**Config file should look like the following example
 * <?php
ini_set('memory_limit','1024M');
set_time_limit(0);//0 for unlimited but you can limit this - just try stuff out

function temporary_csv_filtering_getsourcedir(){
    return 'G:\\My Drive\\Customer\\propertydata\\';//Set the directory containing the source files here
}

function temporary_csv_filtering_getdestdir(){
    return 'G:\\My Drive\\Customer\\enricheddata\\';//Set the directory where the produced files should go
}

function temporary_csv_filtering_getsourcefileprefix(){
    return 'philly';//this is the expected start of the file name - filter by state
}

function temporary_csv_filtering_outputkey(){
    return 'SITE_ZIP';//The column header to use when deciding what data goes in what output file
}

function temporary_csv_filtering_removedups(){
    return 0;//do you want duplicates to be left in the file or just keep one of each
}

 *
 */

//load all of the csv files containing the data
$sourcefiles = scandir($sourcedir);

gc_enable(); // Enable garbage collection if not already enabled


$source_data_combined_array = array();//initialize the large combined array of source data

foreach ($sourcefiles as $sourcefile){
    if($sourcefile == '.' || $sourcefile == '..'){continue;}
    if(!startswith($sourcefile,$state)){continue;}
    $source_data_single_array = array();//make an individual array of data from this one file
    // After significant memory usage
    gc_collect_cycles(); // Force garbage collection
    $csv_file = fopen($sourcedir.$sourcefile,'r');

    echo 'Pulling Source: '.$sourcefile.PHP_EOL;

    //get header row first to set key names for array
    $header_names = array();//index of header names
    $csv_header = fgetcsv($csv_file);
    foreach ($csv_header as $k=>$realkey){
        $header_names[$k] = $realkey;
    }

    while(! feof($csv_file))
    {
        $csvrow = fgetcsv($csv_file);
        $row = array();
        if(!$csvrow){continue;}//skip empty rows
        foreach ($csvrow as $k=>$csvcell){
            if(isset($header_names[$k])){//only add to rows when we know the header name
                $row[$header_names[$k]] = $csvcell;
            }

        }
        $source_data_single_array[] = $row;


    }
    fclose($csv_file);
    $source_data_combined_array = array_merge($source_data_combined_array,$source_data_single_array);
}



//We have the data, now we need to filter out duplicates and stuff we dont want from it
$owner_addr_index = array();

foreach($source_data_combined_array as $k=>$row){
    if($row['USE_CODE_MUNI_DESC'] == 'EXEMPT, GOVERNMENT AND HISTORICAL'
        ||
        $row['USE_CODE_MUNI_DESC'] == 'COMMERCIAL-MOTEL'
        ||
        $row['USE_CODE_MUNI_DESC'] == 'INDUSTRIAL'
        ||
        $row['USE_CODE_MUNI_DESC'] == 'CHURCH & CHARITABLE PROPERTY'
        ||
        $row['USE_CODE_MUNI_DESC'] == 'PUBLIC PROPERTY'
        ||
        $row['USE_CODE_MUNI_DESC'] == 'OTHER SCHOOL PROPERTY'
        ||
        $row['USE_CODE_MUNI_DESC'] == 'CONDOMINIUM'
        ||
        $row['USE_CODE_MUNI_DESC'] == 'FARM (REGULAR)'
    ){
        unset($source_data_combined_array[$k]);//get that trash out of here
    }

    $owner_addr = get_row_addr_key($row);
    //add to index of dups
    $owner_addr_index[$owner_addr][] = $k;
}

//then run through index and remove multiples
foreach($owner_addr_index as $addr=>$keygroup){
    $keeper = false;
    $owner_multi_count = count($keygroup);
    if($owner_multi_count > 1){
        foreach($keygroup as $keygroupkey){
            if(!$keeper){$keeper = $keygroupkey;}//keep one
            else{
                if($remove_dups){//remove the others
                    unset($source_data_combined_array[$keygroupkey]);
                }else{//save the multi count on the dup rows as well
                    $source_data_combined_array[$keygroupkey]['OWNED_PROPERTIES'] = $owner_multi_count;
                }
            }
        }
    }else{
        $keeper = $keygroup[0];
    }
    $source_data_combined_array[$keeper]['OWNED_PROPERTIES'] = $owner_multi_count;
}


echo count($source_data_combined_array);

//Next we need to write the files
//now loop again and write to the output arrays and delete the source as you go
$output_file_rows = array();
foreach ($source_data_combined_array as $k=>$sourcerow){
    $outputkey = $sourcerow[$output_key];
    $output_file_rows[$outputkey][] = $sourcerow;
    unset($source_data_combined_array[$k]);
}

//then write the actual files
foreach($output_file_rows as $filenamesuffix=>$rows){
    $outputfilename = $state.$filenamesuffix.'.csv';
    $outputfilename = strtolower($outputfilename);
    echo PHP_EOL.'Writing: '.$outputfilename;

    $dest_csv = fopen($destdir.$outputfilename,'w');
    //make header row first
    $header_row = array();
    foreach($rows[0] as $rk=>$rv){
        $header_row[] = $rk;
    }
    fputcsv($dest_csv,$header_row);
    //then write data to each row
    foreach($rows as $row){
        fputcsv($dest_csv,$row);
    }
    fclose($dest_csv);
}



//utility functions
function startswith($str,$substr){
    //check that these are in fact strings
    $strarr = str_split($str);
    $subarr = str_split($substr);
    foreach($subarr as $i=>$c){
        if($strarr[$i] != $c){
            return false;
        }
    }
    return true;
}


function get_row_addr_key($row){
    return strtolower(str_replace(' ','',$row['MAIL_HOUSE_NUMBER'].$row['MAIL_DIRECTION'].$row['MAIL_STREET_NAME'].$row['MAIL_MODE'].$row['MAIL_UNIT_NUMBER'].$row['MAIL_CITY'].$row['MAIL_STATE']));
}