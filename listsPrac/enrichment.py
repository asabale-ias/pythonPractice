import os

with os.scandir('/Users/akshaysabale/Desktop/snapchat_id_mapping/snapchat/archive/2020/12/16/00/00') as entries:
    for entry in entries:
        print(entry.name)