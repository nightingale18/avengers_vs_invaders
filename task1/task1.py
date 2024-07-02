#!/bin/python3
import os
import pandas as pd

pickle_contacts = 'contacts.h5'
contacts_store = pd.HDFStore(pickle_contacts)

if not os.path.isfile(pickle_contacts):
    for file in os.listdir('./contacts'):
        contact = file.replace('.txt', '')
        contacts_store[contact] = pd.read_csv('./contacts/'+file, delimiter='\t')

country_df = pd.read_csv('country_hq.txt', delimiter='\t')

table_df = pd.DataFrame(columns=['Country_Code', 'Invader_Species', 'Role', 'Email'])

memo = {}
def contacts(country_code, contact, table_df):
    if memo.get(contact) is None:
        for _, store in contacts_store[contact].iterrows():
            for role in contacts_store[contact].columns[1:]:
                if not pd.isna(store[role]):
                    email = store[role] if '@ds' in store[role] else store[role]+'@avengers.com'
                    new_row = pd.DataFrame([[country_code, store[contact], role, email]], columns=table_df.columns)
                    table_df = pd.concat([table_df, new_row], ignore_index=True)
            memo.update({contact: table_df})
        return table_df
    else: 
        table = memo.get(contact)
        table['Country_Code'] = country_code
        
        return table

for i, row in country_df.iterrows():
    visited_heads = []
    for i in country_df.columns[2:]:
        if i in visited_heads:
            continue
        table = contacts(row['Country Code'], row[i], table_df)
        table.to_csv('lookup_table.csv', index=False, mode='a')
        table.to_csv('lookup_table.txt', index=False, mode='a')

contacts_store.close()
