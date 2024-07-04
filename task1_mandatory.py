#!/bin/python3
import os
import pandas as pd

pickle_contacts = 'contacts.h5'
pickle_results = 'results.h5'
contacts_store = pd.HDFStore(pickle_contacts)
results_store = pd.HDFStore(pickle_results)

columns=['Country_Code', 'Invader_Species', 'Role', 'Email']
if not os.path.isfile(pickle_contacts):
    for file in os.listdir('./contacts'):
        contact = file.replace('.txt', '')
        contacts_store[contact] = pd.read_csv('./contacts/'+file, delimiter='\t')

country_df = pd.read_csv('country_hq.txt', delimiter='\t')

table_df = pd.DataFrame()


def contacts(table_df):
    for contact in contacts_store:
        contact = contact[1:]
        for _, store in contacts_store[contact].iterrows():
            for role in contacts_store[contact].columns[1:]:
                if not pd.isna(store[role]):
                    email = store[role] if '@ds' in store[role] else store[role]+'@avengers.com'
                    new_row = pd.DataFrame([[store[contact], role, email]])
                    table_df = pd.concat([table_df, new_row], ignore_index=True)
            results_store[contact] = table_df
    return results_store


results_store = contacts(table_df)

for c_idx, row in country_df.iterrows():
    country_code = row['Country Code']
    result_df = pd.DataFrame()
    for invader in country_df.columns[2:]:

        contact_head = row[invader] 
        invader = invader.lower()
        headq_file = results_store[contact_head]
        
        if invader == 'D&D Monsters':
            for row_idx, store in headq_file.iterrows():
          
                if store.iloc[row_idx].iloc[0].startswith('d&d'):
                    result_df = pd.concat([result_df, store], ignore_index=True)

        else:
            result_df = pd.concat([result_df, headq_file[headq_file[0] == invader]])

    country_len = len(result_df)
    
    if 'Country_Code' not in result_df.columns:
        result_df.insert(0, 'Country_Code', [country_code] * country_len)
    else:
        result_df['Country_Code'] = [country_code] * country_len
    
    if not c_idx:
        result_df.columns = columns
        result_df.to_csv('lookup_table.csv', index=False, mode='a')
        result_df.to_csv('lookup_table.txt', index=False, mode='a')

    else: 
        result_df = result_df.reset_index(drop=True)
        result_df.to_csv('lookup_table.csv', index=False, header=False, mode='a')
        result_df.to_csv('lookup_table.txt', index=False, header=False, mode='a')

contacts_store.close()
results_store.close()
