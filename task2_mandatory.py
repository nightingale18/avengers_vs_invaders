import os
import pandas as pd
import warnings
from tables import NaturalNameWarning

# Suppress NaturalNameWarning from tables module
warnings.filterwarnings('ignore', category=NaturalNameWarning)

# Define file paths
pickle_contacts = 'contacts.h5'
pickle_heroes = 'heroes.h5'
csv_folder = './csv'
txt_folder = './txt'

# Remove existing heroes.h5 file if it exists
if os.path.isfile(pickle_heroes):
    os.remove(pickle_heroes)

# Initialize HDFStore for heroes data
heroes_store = pd.HDFStore(pickle_heroes)
contacts_store = pd.HDFStore(pickle_contacts, mode='r')

# Read column names from iron.man.txt
row_col_names = pd.read_csv('task2/iron.man.txt', delimiter='\t')
invader_criteria = list(row_col_names.columns[1:])
headquat_criteria = list(row_col_names['iron.man'])

# Create initial data structures
inv_dict = {inv: [''] for inv in invader_criteria}
df_inv = pd.DataFrame(inv_dict)

roles_dict = {}
roles = ['attack_role', 'defense_role', 'healing_role']

data_wide = {role: [''] * 16 for role in invader_criteria}

# Process each headquarter criterion
for country_idx, contact in enumerate(headquat_criteria):
    # Iterate through contacts in contacts_store
    for _, store in contacts_store.get(contact).iterrows():
        # Determine roles that are not NaN
        bool_mask = pd.notna([store[role] for role in roles])
        filtered_mask = [idx for idx, value in enumerate(bool_mask) if value]
        
        # Get invader and update heroes_store
        invader = store.iloc[0]
        for j in filtered_mask:
            hero = store[roles[j]]
            if hero not in heroes_store:
                heroes_store.put(hero, pd.DataFrame(data_wide))
            
            hero_store = heroes_store[hero]
            hero_store.at[country_idx, invader] += roles[j][0].upper()
            heroes_store.put(hero, hero_store)

# Ensure csv and txt folders exist, create if not
for path in [csv_folder, txt_folder]:
    if not os.path.exists(path):
        os.makedirs(path)

# Process each hero in heroes_store
for hero in heroes_store:
    hero_store = heroes_store[hero]
    
    # Insert hero[1:] as the first column
    hero_store.insert(loc=0, column=hero[1:], value=headquat_criteria)
    
    # Clean up DataFrame
    hero_store.dropna(axis=1, how='all', inplace=True)
    empty_cols = [col for col in hero_store.columns if (hero_store[col] == '').all()]
    hero_store.drop(columns=empty_cols, inplace=True)
    hero_store.replace('', pd.NA, inplace=True)
    hero_store.dropna(axis=0, how='any', inplace=True)
    hero_store.reset_index(drop=True, inplace=True)
    
    # Save DataFrame to csv and txt files
    hero_store.to_csv(f'{csv_folder}/{hero[1:]}.csv', index=False, mode='w')
    hero_store.to_csv(f'{txt_folder}/{hero[1:]}.txt', index=False, mode='w')
    
    # Update heroes_store with the modified DataFrame
    heroes_store.put(hero, hero_store)

# Close HDFStores
contacts_store.close()
heroes_store.close()