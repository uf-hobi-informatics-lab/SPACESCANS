from itertools import product

import pandas as pd

def read_raw_exposome():
    raw_caces = pd.read_csv("/Users/looseymoose/Dropbox (UFL)/Exposome_Data_Linkage_Tool/exposome_files/raw_exposomes/uwc17066350009005fb15292c3f979ab109b4b4f789d70e1.csv")
    return raw_caces

def translate_exposome(raw_caces):
    fips_list = list(raw_caces['fips'].unique())
    year_list = list(raw_caces['year'].unique())
    pol_list = list(raw_caces['pollutant'].unique())

    new_caces = pd.DataFrame(data=list(product(fips_list, year_list)), columns=['fips', 'year'])
    new_caces = new_caces.reindex(columns=['fips', 'year']+pol_list)

    def get_pols(fips, year, pol, df):
        y = df.loc[(df['fips']==fips) & (df['year']==year) & (df['pollutant']==pol), 'pred_wght']
        return y.values.tolist()[0]

    for pol in pol_list:
        new_caces[pol] = new_caces.apply(lambda x: get_pols(x['fips'], x['year'], pol, raw_caces), axis=1)        
    return new_caces

def save_exposome(caces):
    caces.to_csv('formatted_caces.csv', index=False)

def main():
    raw_caces = read_raw_exposome()
    new_caces = translate_exposome(raw_caces)
    save_exposome(new_caces)

if __name__== '__main__':
    main()