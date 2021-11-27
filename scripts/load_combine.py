"""
Load and combine csv files in a directory
Argument to be passed - Zipfile with absolute path
"""

import pandas as pd
import datetime
import sys
import os
import shutil
import zipfile

def create_path(dir_path):
    """"Create output, processed and log paths from the file path provided"""
    parent_dir= dir_path.rsplit( "\\", 1 )[ 0 ]
    output_dir=(parent_dir+"\output")
    processed_dir=(parent_dir+"\processed")
    log_dir=(parent_dir+"\log")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    if not os.path.exists(processed_dir):
        os.makedirs(processed_dir)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    logfile = (log_dir+"\log"+"_"+str(datetime.datetime.today().strftime('%Y%m%d%H%M%S'))+".txt")
    sys.stdout = open(logfile, 'w')            #Capturing logs in the log directory
    return(output_dir,processed_dir,log_dir,parent_dir)

def extract_zip(zfile,dir_path):
    """Extract the files from the zip folder"""
    if os.path.isfile(zfile):
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)            #create the directory if not present for extraction
        with zipfile.ZipFile(zfile, 'r') as zip_ref:
            zip_ref.extractall(dir_path)
    else:
        print("Error : Zipfile passed not present in path")            #exit if zipfile path wrongly mentioned
        exit(0)
    return 'Extracted File - '+zfile+' into path - '+dir_path

def sort_dup_remove(df,dir_path,output_dir):
    """Removes the duplicates and sort the data in dataframe"""
    df1=df.drop_duplicates(subset=None, keep='first', inplace=False)            #Removing duplicate entries from the dataframe
    list1=df1.values.tolist()            #Converting data frame into list
    list1=sorted(list1, key=lambda v:[int(i) for i in v[0].split('.')])            #Sorting the data in dataframe in descending order
    df1=pd.DataFrame(list1, columns=['Source IP','Environment'])            #Converting list to dataframe after sorting
    df1.to_csv(output_dir+"\Combined"+"_"+str(datetime.datetime.today().strftime('%Y%m%d%H%M%S'))+".csv", index=False)            #Writing the results into Combined.csv file in output folder created
    df1.to_csv(dir_path+"\Combined.csv", index=False)            #Writing the results into Combined.csv file in folder created
    return 'Combined file created in path - '+output_dir+'\nCombined file also available in the files folder - '+dir_path
