import os
import glob
import pandas as pd
os.chdir('.')

extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]

#combine all files in the list and shuffle
combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames]).sample(frac=1)

#export to csv
combined_csv.to_csv("crime-data.csv", index=False, encoding='utf-8-sig')