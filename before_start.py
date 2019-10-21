import os
import consts as const
import glob

if os.path.exists(const.WAL_filename):
    os.remove(const.WAL_filename)
if os.path.exists(const.manifest_filename):
    os.remove(const.manifest_filename)
fileList = glob.glob('data*.txt')
for file in fileList:
    os.remove(file)
print("remove successfully")