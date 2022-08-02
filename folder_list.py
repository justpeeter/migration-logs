import os
import sys
import argparse
import pandas as pd
import numpy as np


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # assign argument
    parser.add_argument('-f','--folder', type=str, required=True)
    flags = parser.parse_args()

    df = pd.DataFrame(columns = ['name'])
    for filename in os.listdir(flags.folder):
        data = pd.DataFrame({'name': [filename]})
        df = df.append(data, ignore_index = True)
    
    
    df['name'] = df['name'].str.replace('_export','')
    df['name'] = df['name'].str.replace('_import','')
    df = df.drop_duplicates('name')
    
    for index, row in df.iterrows():
        export_dir = f"{flags.folder}/{row[0]}_export".replace('//','/')
        import_dir = f"{flags.folder}/{row[0]}_import".replace('//','/')
        #print(f"python3.8 logs_compare.py -o ./output -n {row[0]} -e {export_dir} -i {import_dir}")
        print(f"run::: {row[0]}")
        os.system(f"""
            py logs_compare.py -o ./output -n {row[0]} -e {export_dir} -i {import_dir}
        """)
