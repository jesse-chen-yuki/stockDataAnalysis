import os
from itertools import islice
import shutil
import multivolumefile
from py7zr import SevenZipFile


def preprocess():
    raw_path = "D:/StockProjectData/raw"
    prepro_path = "D:/StockProjectData/prepro"
    tar_path = "D:/StockProjectData/tar"

    def reset():
        # clear all prepro files and make new folder
        try:
            shutil.rmtree(prepro_path)
        except Exception as e1:
            print(e1)
        try:
            os.mkdir(prepro_path)
        except Exception as e1:
            print(e1)
        try:
            shutil.rmtree(raw_path)
        except Exception as e1:
            print(e1)
        try:
            os.mkdir(raw_path)
        except Exception as e1:
            print(e1)
    reset()

    def extract():
        # extract tar files
        for (dir_path1, dir_names, files1) in os.walk(tar_path):
            file_list = set([])
            target_path1 = dir_path1.replace('tar', 'raw')
            try:
                os.mkdir(target_path1)
            except Exception as e1:
                print(e1)
            # get list of file
            for file_name1 in files1:
                print(file_name1)
                file_list.add(file_name1[:-4])
                print(file_list)
            # extraction
            for file in file_list:
                print(dir_path1+'/'+file)
                with multivolumefile.open(dir_path1+'/'+file, mode='rb') as target_archive:
                    with SevenZipFile(target_archive, 'r') as archive:
                        archive.extractall(target_path1)

    extract()

    def process():
        # process raw files
        for (dir_path, dir_names, files) in os.walk(raw_path):
            target_path = dir_path.replace('raw', 'prepro')
            try:
                os.mkdir(target_path)
            except Exception as e:
                print(e)
            for file_name in files:
                f = open(dir_path + "/" + file_name, "r")
                w = open(target_path + "/" + file_name, "w")
                print(dir_path, dir_names, file_name)
                limit = 10
                next_n_lines = list(islice(f, limit))
                # print(next_n_lines)
                w.writelines(next_n_lines)
                f.close()
                w.close()
    process()


preprocess()
