"""
mainLoad.py
Load and combine csv files in a directory
Argument to be passed - Zipfile with absolute path
"""

from load_combine import *

def read_files(files,dir_path):
    """Read csv files present in the path and load into DataFrames
       Move the processed and create the updated zip file
       sort_dup_remove() called if valid files are loaded"""
    i=0
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
                    print(file," not processed. csv files expected(other than Combined.csv)")
            else:
                print(file," - Directory found. Not considered for processing ")
    if i>=1:            #Checking for files processed. If one or more processed, proceed into the condition
        print(sort_dup_remove(df1,dir_path,output_dir))
        zip_file=dir_path.rsplit( "\\", 1 )[ 1 ]
        shutil.move(zfile,processed_dir+'\\'+zip_file+"_"+str(datetime.datetime.today().strftime('%Y%m%d%H%M%S'))+".zip")            #Move the zip file into processed directory
        shutil.make_archive(zip_file, 'zip', dir_path)
        zip_check=parent_dir+"\\"+zip_file+".zip"
        if not os.path.isfile(zip_check):
            shutil.move(zip_file+'.zip',dir_path+'\\..')			#Move the updated zip folder to parent directory
        return ('Files are processed and combined')
    else:
        return("No valid files found for processing")

if __name__ == "__main__":
    try:
        i=0
        zfile = sys.argv[1]            #Reading the file directory
        dir_path = zfile.rsplit( ".", 1 )[ 0 ]
        uzfpath = dir_path.rsplit( "\\", 1 )[ 0 ]
        output_dir,processed_dir,log_dir,parent_dir = create_path(dir_path)            #Assigning paths
        print("output path - ",output_dir,"\nprocessed dir - ",processed_dir,"\nlog path - ",log_dir,"\nparent dir - ",parent_dir)
        print(extract_zip(zfile,dir_path))
        if len(os.listdir(dir_path)) == 0:            #Check if the file directory is empty
            print("Directory is empty")
        else:    
            files = os.listdir(dir_path)
            print(read_files(files,dir_path))			#call the read_files()
    except (IndexError, ValueError, RuntimeError):
        print("Valid Directory details not passed for files")
        exit(1)