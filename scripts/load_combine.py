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

def logging(text):
    print(text)>>log.txt
    
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

def extract_zip(zfile,dir):
    """Extract the files from the zip folder"""
    if os.path.isfile(zfile):
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)			#create the directory if not present for extraction
        with zipfile.ZipFile(zfile, 'r') as zip_ref:
            zip_ref.extractall(dir_path)
    else:
        print("Error : Zipfile passed not present in path")			#exit if zipfile path wrongly mentioned
        exit(0)
    return 'Extracted File - '+zfile+' into path - '+dir_path

if __name__ == "__main__":
    try:
        i=0
        zfile = sys.argv[1]            #Reading the file directory
        dir_path = zfile.rsplit( ".", 1 )[ 0 ]
        uzfpath = dir_path.rsplit( "\\", 1 )[ 0 ]
        output_dir,processed_dir,log_dir,parent_dir = create_path(dir_path)            #Assigning paths
        print("output path - ",output_dir,"\nprocessed dir - ",processed_dir,"\nlog path - ",log_dir)
        print(extract_zip(zfile,dir_path))
        if len(os.listdir(dir_path)) == 0:            #Check if the file directory is empty
            print("Directory is empty")
        else:    
            files = os.listdir(dir_path)
            for file in files:
                if os.path.isfile(os.path.join(dir_path,file)):            #Checking for files in the path
                    file_ext =  file.rsplit( ".", 1 )[ 1 ]            #Taking the file format
                    if file_ext=='csv' and file!='Combined.csv':
                        print("File being processed is : ",file)
                        fn1=file.rsplit( ".", 1 )[ 0 ]            #Removing the extension of csv files present in the path
                        fn_final=' '.join(fn1.split()[:2])            #Taking the first 2 words of the filename from fn1
                        try:
                            if i==0:            #Checking for the number of files already processed
                                df1=pd.read_csv(os.path.join(dir_path,file),usecols = ['Source IP'])            #Reading file column into dataframe df1
                                df1['Environment']=fn_final            #Adding the environment column to the dataframe created
                            else:
                                df2=pd.read_csv(os.path.join(dir_path,file),usecols = ['Source IP'])
                                df2['Environment']=fn_final
                            print("Inserted File :",file)
                            i=i+1            #Counting the number of files processed
                            shutil.copy(dir_path+"\\"+file, processed_dir+"\\"+fn1+"_"+str(datetime.datetime.today().strftime('%Y%m%d%H%M%S'))+".csv")            #Move loaded csv files into processed folder adding timestamp
                        except:
                            print('VALUE ERROR... Check file data in - ',file)            #Error message for csv file issues
                            pass
                        if i>1:
                            df1=df1.append(df2)            #If more than one file processed, append data into df1
                    else:    
                        print(file," not processed. csv files expected")
                else:
                    print(file," - Directory found. Not considered for processing ")
            if i>=1:            #Checking for files processed. If one or more processed, proceed into the condition
                df1=df1.drop_duplicates(subset=None, keep='first', inplace=False)            #Removing duplicate entries from the dataframe
                list1=df1.values.tolist()            #Converting data frame into list
                list1=sorted(list1, key=lambda v:[int(i) for i in v[0].split('.')])            #Sorting the data in dataframe in descending order
                df1=pd.DataFrame(list1, columns=['Source IP','Environment'])            #Converting list to dataframe after sorting
                df1.to_csv(output_dir+"\Combined"+"_"+str(datetime.datetime.today().strftime('%Y%m%d%H%M%S'))+".csv", index=False)            #Writing the results into Combined.csv file in output folder created
                df1.to_csv(dir_path+"\Combined.csv", index=False)            #Writing the results into Combined.csv file in folder created
                zip_file=dir_path.rsplit( "\\", 1 )[ 1 ]
                shutil.move(zfile,processed_dir+'\\'+zip_file+"_"+str(datetime.datetime.today().strftime('%Y%m%d%H%M%S'))+".zip")			#Move the zip file into proecessed directory
                shutil.make_archive(zip_file, 'zip', dir_path)
                print("Combined file created in path ",output_dir,"\nCombined file also available in the files folder")
            else:
                print("No valid files found for processing")
    except (IndexError, ValueError, RuntimeError):
        print("Valid Directory details not passed for files")
        exit(1)
