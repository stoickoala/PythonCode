import numpy as np
import pandas as pd
import os
import sys

data_path = os.path.join(sys.path[0][:sys.path[0].rindex('\\')], r'Resources\Data\test_df.csv')
data_path

df = pd.read_csv(data_path)
flags = df.flag.values

def val_index_finder(arr, vals=None):
    if vals==None:
        vals = list(set(arr))
    else:
        pass
    values_index = {val:[] for val in vals}
    for i in range(len(arr)):
        for val in values_index.keys():
            if arr[i] == val:
                values_index[val].append(i)
    return values_index


def block_finder(arr, vals_to_count=None):
    values_index = val_index_finder(arr)
    if vals_to_count == None:
        vals_to_count = list(values_index.keys())
    # print(vals_to_count)
    blocks = {val:{'start_index':[], 'length':[]} for val in vals_to_count}
    for val in vals_to_count:
        # print(val)
        block_count = 1
        val_to_count = values_index[val]
        for i in range(1, len(val_to_count)):
            if block_count == 1:
                # print(f'current block_count is {block_count} so appending the previous index {val_to_count[i-1]}')
                blocks[val]['start_index'].append(val_to_count[i-1])
            
            if val_to_count[i] == val_to_count[i-1]+1:
                # print(f'current block_count for {val}: {block_count}')
                # print(f'{val_to_count[i]} is equal to {val_to_count[i-1]}+1 so adding 1 to block_count.')
                block_count += 1
            else:
                # print(f'{val_to_count[i]} is not equal to {val_to_count[i-1]}+1 so appending {block_count} to the dict and then resetting block_count.')
                blocks[val]['length'].append(block_count)
                block_count = 1
                
            if i == len(val_to_count)-1:
                blocks[val]['length'].append(block_count)
    
    return values_index, blocks