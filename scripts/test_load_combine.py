"""
test_load_combine.py
Unit tests for functions in load_combine.py
create_path(), extract_zip(), sort_dup_remove()
"""

from load_combine import *
import unittest


class MyTest(unittest.TestCase):
    def test_create_path(self):
        dir_path=r'D:\Users\ssukuma5\Documents\Python\Analytics\Files\Engineering Test Files'
        log_path=r'D:\Users\ssukuma5\Documents\Python\Analytics\Files\log'
        output_path=r'D:\Users\ssukuma5\Documents\Python\Analytics\Files\output'
        processed_path=r'D:\Users\ssukuma5\Documents\Python\Analytics\Files\processed'
        create_path(dir_path)
        self.assertTrue(os.path.exists(log_path))			#Check if the path is created
        self.assertTrue(os.path.exists(output_path))
        self.assertTrue(os.path.exists(processed_path))

    def test_extract_zip(self):
        dir_path=r'D:\Users\ssukuma5\Documents\Python\Analytics\Files\Engineering Test Files'
        zfile=r'D:\Users\ssukuma5\Documents\Python\Analytics\Files\Engineering Test Files.zip'
        extract_zip(zfile,dir_path)
        self.assertTrue(os.path.exists(dir_path))
        self.assertTrue(len(os.listdir(dir_path))!=0)			#Extracted directory count check

    def test_sort_dup_remove(self):
        dir_path=r'D:\Users\ssukuma5\Documents\Python\Analytics\Files\Engineering Test Files'
        output_dir=r'D:\Users\ssukuma5\Documents\Python\Analytics\Files\output'
        lst = [['4.4.4.4', 'Asia Prod'],['5.5.5.5', 'Asia Prod'],['6.6.6.6', 'Asia Prod'],['1.1.1.1', 'NA Prod'],['5.5.5.5', 'Asia Prod'],['2.2.2.2', 'NA Prod'],['6.6.6.6', 'Asia Prod'],['3.3.3.3', 'NA Prod']]
        df = pd.DataFrame(lst, columns=['Source IP', 'Environment'])
        sort_dup_remove(df,dir_path,output_dir)
        df1=pd.read_csv(dir_path+"\Combined.csv")
        lst1 = [['1.1.1.1', 'NA Prod'],['2.2.2.2', 'NA Prod'],['3.3.3.3', 'NA Prod'],['4.4.4.4', 'Asia Prod'],['5.5.5.5', 'Asia Prod'],['6.6.6.6', 'Asia Prod']]
        df2 = pd.DataFrame(lst1, columns=['Source IP', 'Environment'])
        pd.testing.assert_frame_equal(df1,df2)			#Compare the data created from called function with the expected data created from lst1