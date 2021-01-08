# -*- coding: utf-8 -*-
"""
Created on Tue Sep  8 10:05:39 2020

@author: hcurtis
"""

# Import packages
import pyodbc
import pandas as pd
import os
from IPython.display import display, Markdown
from contextlib import contextmanager
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
import re # allows case-insensitivity for keyword filtering

import numpy as np
from ebmdatalab import charts
import matplotlib.pyplot as plt

import json



# Set up SQL connection ensuring that it is closed after each use
@contextmanager
def closing_connection(dbconn):
    cnxn = pyodbc.connect(dbconn)
    try:
        yield cnxn
    finally:
        cnxn.close()
        
def load_concept(filename, codes):
    
    '''
    Load concept map CSV
    
    Inputs:
    filename(str): file name for concept file
    codes (dataframe): lookup table for code descriptions
    
    Output:
    concepts (dataframe)
    '''
    
    with open(os.path.join("..","data",filename)) as f:
        concepts_to_descendants = json.load(f)
    records = []
    
    for ancestor, descendants in concepts_to_descendants.items():
        for descendant in descendants:
            records.append([ancestor, descendant])
    concepts = pd.DataFrame(records, columns=["ancestor", "descendant"])
    concepts["concept_digit"] = np.where(concepts["ancestor"].str[1:5]=="....", concepts["ancestor"].str[0], concepts["ancestor"])
    concepts["descendant_clean"] = concepts["descendant"].str.replace(".","")

    # join descriptions for either full-length X-codes or first digit from CTV3 dictionary
    concepts = concepts.merge(codes[["concept_digit", "concept_desc"]], on="concept_digit", how="left")

    return concepts        
        

def process_df(df, codes):
    
    '''
    Process dataframe to add calculated fields and merge with descriptions for codes
    
    Inputs:
    df (dataframe): dataframe loaded from SQL and containing frequency counts of codes
    codes (dataframe): lookup table for code descriptions
    
    Output:
    df (dataframe): processed dataframe
    '''
    
    # create/amend columns
    df["2020 events (mill)"] = round(df["events"]/1000000, 2)
    df["2020 Patient count (mill)"] = round(df["patients"]/1000000, 2)
    df["first_digit"] = df["first_digits"].str[0]
    df["first_digits"] = df["first_digits"].str.replace(".","")
    df["digits"] = df["first_digits"].str.len()

    # merge with codelist to get description
    df = df.merge(codes[["first_digits", "Description"]], on="first_digits", how="left")
    df = df.merge(codes[["first_digits", "Description"]], left_on="first_digit", right_on="first_digits", how="left", suffixes=["","_L1"])
    return df


def join_concept_descriptions(df, concepts, concepts2, concepts3):
    
    '''
    Process dataframe to add concept descriptions at various levels
    
    Inputs:
    df (dataframe): dataframe containing a "first_digits" column (may be 1-5 digits) to join to concepts 
    concepts, concepts2, concepts3 (dataframes): concept tables (level 1, 2 and 3 concepts)
    
    Output:
    df (dataframe): processed dataframe
    '''
    
    df["first_digits"] = df["first_digits"].str.replace(".","")

    # join level 1 concepts
    df = df.merge(concepts[["descendant_clean", "concept_desc"]].drop_duplicates(), 
                  left_on="first_digits", right_on="descendant_clean", how="left", suffixes=["","_L1"]).drop("descendant_clean",1)
    df = df.rename(columns={"concept_desc":"concept_desc_L1"})
    # join level 2 concepts
    df = df.merge(concepts2[["descendant_clean", "concept_desc"]].drop_duplicates(), 
                  left_on="first_digits", right_on="descendant_clean", how="left", suffixes=["","_L2"]).drop("descendant_clean",1)
    df = df.rename(columns={"concept_desc":"concept_desc_L2"})
    # join level 3 concepts
    df = df.merge(concepts3[["descendant_clean", "concept_desc"]].drop_duplicates(), 
                  left_on="first_digits", right_on="descendant_clean", how="left", suffixes=["","_L3"]).drop("descendant_clean",1)
    df = df.rename(columns={"concept_desc":"concept_desc_L3"})

    # use level 1 concepts initially:
    df["concept_desc"] = df["concept_desc_L1"]
    
    # most of the common Y codes are admin related and those beginning with "9" should also be Administration
    df["concept_desc"] = np.where((df["first_digit"].isin(["Y","9"])), "Administration", df.concept_desc.fillna("Other")) 
 
    # where codes relate to "clinical findings" according to the "first level concept", use description from first digit of code, else keep level one concept
    df["concept_desc"] = np.where(df["concept_desc_L1"]=="Clinical findings", df["Description_L1"], df["concept_desc"])
    
    # where codes relate to laboratory procedures according to the "third level concept", use concept "Laboratory procedures", else keep level one concept
    df["concept_desc"] = np.where((df["concept_desc_L3"].isin(["Laboratory test","Laboratory test observations"])) | (df["first_digit"]=="4"),
                                  "Laboratory procedures", df["concept_desc"])
    
    # drop excess columns
    df = df.drop(["first_digits_L1", "Description_L1", "concept_desc_L1", "concept_desc_L2", "concept_desc_L3"], axis=1)
    
    return df



def get_subcodes(codelist, code_dict, digits, end_date, threshold, dbconn):
    
    '''
    Find top full length codes within the parent codes to help with interpretation
    
    Inputs:
    codelist (tuple): list of "first_digits" 
    code_dict (dataframe): lookup table for code descriptions
    digits (int): number of digits of the codes listed in the codelist
    end_date (str): end date of study period
    threshold (int): lower limit for activity numbers 
    dbconn (str): SQL credentials
    
    Outputs:
    df_out (dataframe): dataframe containing list of top 50 full-length codes for each code in codelist
    '''
    
    # make a temp table of all codes in codelist with counts above the threshold, then query each code in turn to find top 5
    sql1 = f'''select 
    CTV3Code AS first_digits, 
    COUNT(Patient_ID) as events
    FROM CodedEvent 
    WHERE 
    LEFT(CTV3Code,{digits}) in {codelist} 
    AND ConsultationDate >= '20200101'
    AND ConsultationDate < '{end_date}'

    GROUP BY CTV3Code
    HAVING COUNT(Patient_ID) > {threshold}'''

    df_out = pd.DataFrame()
    with closing_connection(dbconn) as connection:
        out = pd.read_sql(sql1, connection)

        for code in codelist:
            out1 = out.loc[out["first_digits"].str[:len(code)]==code].sort_values(by="events", ascending=False).head(50)
            out1["parent_code"] = code
            df_out = df_out.append(out1)
    
    df_out["first_digits"] = df_out["first_digits"].str.replace(".","")
    df_out["2020 events (thou)"] = (df_out["events"].astype("float")/1000).round(1)
    df_out = df_out.drop("events", 1)

    # merge with codelist to get description
    df_out = df_out.merge(code_dict[["first_digits", "Description"]], on="first_digits", how="left")
    return df_out



def plot_charts(out, outer_percentiles):
    charts.deciles_chart(
        out,
        period_column='month',
        column='value',
        #title=Title,
        ylabel="rate per 1000",
        show_outer_percentiles=outer_percentiles,
        show_legend=True
    )
    
    # Now add a single line against the deciles

    #df_subject = pd.DataFrame(np.random.normal(0.6, 0.05, 37), columns=['val'])
    #df_subject['month'] = out["month"].drop_duplicates()
    #df_subject.set_index('month')

    #plt.plot(df_subject['month'], df_subject['val'], 'r--')
    plt.show()
    
    
def all_pracs(df0, df, code):
    
    '''
    Expand filtered dataframe (df) to include all relevant practices every month. Relevant practices are those ever using the current code during the covered period. This ensures that deciles represent true trends rather than appearing to change when there is simply a change in the number of practices using the code over time. 
    
    Inputs:
    df0 (dataframe): full time series data for all codes in codelist 
    df (dataframe): df0 filtered to a single code
    code (str): full or truncated CTV3 code
        
    Output:
    out (dataframe): time series data at practice level to plot decile charts
    practice_count_thou (float): number of practices included
    practices_percent (float): percent of all practices included
    '''

    # cross join all practices and months to make sure they all appear under the current code
    # all months 
    cross = df0[["month"]].drop_duplicates()
    cross["first_digits"] = code
    cross["key"] = 1

    # all practices which have used the current code, and their list size (denominator) 
    # note this has been calculated based on current registrations so does not vary by month
    cross2 = df.copy()
    cross2 = cross2[["Practice_ID","denominator"]].drop_duplicates()
    cross2["key"] = 1
    practice_count = cross2["Practice_ID"].nunique()
    practice_count_thou = round(practice_count/1000, 1)
    practices_percent = round(100*practice_count/df0["Practice_ID"].nunique(), 1)
    
    out = df.copy()
    cross2 = cross.merge(cross2, on="key").drop("key",1)
    # merge with data
    out = cross2.merge(out, how="left", on=["first_digits", "month","Practice_ID","denominator"]).fillna(0)

    return out, practice_count_thou, practices_percent



def plotting_all(codelist, code_dict, h, threshold, end_date, dbconn, second_chart=False):
    
    '''
    Extract data and plot a series of decile charts
    
    Inputs:
    codelist (dataframe): list of codes
    code_dict (dataframe): lookup table for code descriptions
    h (int): No of charts to plot
    threshold (int): lower limit for activity number (global variable)
    end_date (str): end date of study period
    dbconn (str): SQL credentials
    second_chart (bool): opt in to display the trend for the top code within each parent code (e.g. useful for path)
        
    Outputs:
    Header text, charts and tables
    '''
    
    #######
    # sql queries necessary for plotting:
    sql1 = f'''-- patient registrations
    SELECT
    Patient_ID,
    Organisation_ID AS Practice_ID,
    ROW_NUMBER() OVER (partition by Patient_ID ORDER BY StartDate DESC, EndDate DESC) AS registration_date_rank -- row_num gives unique results
    INTO #reg
    FROM RegistrationHistory
    WHERE 
    StartDate <= '{end_date}' AND
    EndDate >= '{end_date}'; -- registrations which were live at the end of the study period
    '''
    
    sql2 = f'''-- practice list size
    SELECT
    Organisation_ID AS Practice_ID,
    COUNT(DISTINCT Patient_ID) AS list_size
    INTO #listsize
    FROM RegistrationHistory
    WHERE StartDate <= '{end_date}' AND
    EndDate >= '{end_date}' -- registrations which were live at the end of the study period
    GROUP BY Organisation_ID
    '''
    ##### create subset of codelist up to maximum number provided
    subset = codelist.head(h)
    
    ###### set up sql string to query database for codes of varying lengths
    listcodes = {}
    for n in np.arange(1, 6, 1):
        inter = subset.loc[subset["digits"]==n]
        listcodes[n] = str(tuple(inter["first_digits"])).replace(",)",")")     

    out_string = ""

    for i in listcodes:
        if len(listcodes[i])>4:
            out_string = " OR ".join([out_string,f"CAST(LEFT(CTV3Code,{i}) AS VARCHAR) IN {listcodes[i]}"]).strip(" OR ") # strip OR from start
    ######
    
    #### sql query for extracting data for all codes in codelist into temp table
    sql3 = f'''select 
        CASE WHEN CHARINDEX('.',CTV3Code) > 0
        THEN LEFT(CTV3Code,CHARINDEX('.',CTV3Code)-1) 
        ELSE CTV3Code END AS first_digits, 
        DATEFROMPARTS(YEAR(ConsultationDate),MONTH(ConsultationDate),1) AS month, 
        r.Practice_ID,
        COUNT(e.Patient_ID) as numerator,
        l.list_size as denominator
        FROM CodedEvent e
        INNER JOIN #reg r ON e.Patient_ID = r.Patient_ID AND r.registration_date_rank = 1
        INNER JOIN #listsize l ON r.Practice_ID = l.Practice_ID
        WHERE 
        ConsultationDate IS NOT NULL 
        AND ({out_string})
        AND ConsultationDate >= '20190101' 
        AND ConsultationDate <= '{end_date}'
        GROUP BY 
        CASE WHEN CHARINDEX('.',CTV3Code) > 0
            THEN LEFT(CTV3Code,CHARINDEX('.',CTV3Code)-1) 
            ELSE CTV3Code END, 
            DATEFROMPARTS(YEAR(ConsultationDate), MONTH(ConsultationDate), 1), 
            r.Practice_ID, l.list_size
        ORDER BY month'''

    ######
    
    ##### find top full-length codes appearing within parent codes at level 2 and level 3 respectively
    d = np.where(subset["digits"].max()==5, 3, 2)
    c = subset.loc[subset["digits"]==d] # select only codes with the min number of digits 
                                        # (either all are 2, or there is a mix of 3 & 5, where only the 3-digit ones need a list of sub codes producing)
    c = tuple(subset["first_digits"])
    subcodes = get_subcodes(c, code_dict, d, end_date, threshold, dbconn) 
    now = datetime.now()
    subcodes.to_csv(os.path.join("..","output",f"subcodes_l{d}_{end_date}_{now}.csv"), index=False)
    #####
    
    
    ##### list of activity categories to group by
    cats = subset.groupby("concept_desc")[["2020 events (mill)"]].sum().sort_values(by="2020 events (mill)", ascending=False).reset_index()
    cats = cats.loc[~cats["concept_desc"].isin(["Additional values","Unit"])]
    cats = cats.concept_desc
    #####

    ##################
    # run sql queries:
    with closing_connection(dbconn) as connection:
        connection.execute(sql1) # patient registrations
        connection.execute(sql2) # practice list size
        df0 = pd.read_sql(sql3, connection) # events  
            
                    
        for cat in cats:
            subset_cat = subset.copy().loc[subset["concept_desc"]==cat]
            total_events = round(subset_cat["2020 events (mill)"].sum(),2)
            # fill any missing descriptions
            subset_cat["Description"] = subset_cat["Description"].fillna("Unknown")
            display(Markdown(f"# --- \n # Category: {cat}"))
            display(Markdown(f"Total events: {total_events} m"))
            display(Markdown(f"## Contents:"))
            display(subset_cat[["first_digits", "Description", "2020 events (mill)", "2020 Patient count (mill)"]].drop_duplicates())
            
            for code, digits, desc, e_mill, pts in zip(subset_cat.first_digits, subset_cat.digits, subset_cat.Description, subset_cat["2020 events (mill)"], subset_cat["patients"]):
                # extract time series data for each code
                df = df0.copy().loc[df0["first_digits"].str[:len(code)]==code]
                # group data to all full codes that begin with current code
                df["first_digits"] = df["first_digits"].str[:len(code)]
                df = df.groupby(["month","first_digits", "Practice_ID","denominator"]).sum().reset_index()

                desc = desc.replace("'","") # replace apostrophes 
                
                if len(df)>0:
                    out, practice_count, practices_percent = all_pracs(df0, df, code)
                    out["value"] = 1000*out["numerator"]/out["denominator"]
                    
                    endmonth = out["month"].max()
                    endmonth_2019 = endmonth + relativedelta(years=-1)
                    endmonthname = endmonth.strftime("%B")
                    
                    # classify changes occurring between timepoints
                    clfy = pd.Series(dtype="float64")
                    clfy2 = pd.Series(dtype="float64")
                    for d in [date(2019,4,1), endmonth_2019, date(2020,2,1), date(2020,4,1), endmonth]:
                        clfy[d] = out.loc[out["month"]==d]["value"].median()
                    for d in [date(2020,2,1), date(2020,4,1), endmonth]:
                        clfy[str(d)+"_IDR"] = out.loc[out["month"]==d]["value"].quantile(0.9) - out.loc[out["month"]==d]["value"].quantile(0.1)
                    def pct_change(clfy, a, b): # calculate percentage change between two columns
                        if clfy[b]>0: # if denominator is non-zero calculate as normal
                            out = 100*(clfy[a]-clfy[b])/clfy[b]
                        else: # if denominator is zero:
                            if clfy[a]>0: # if numerator is non-zero set increase to 100%
                                out = 100
                            else:
                                out = 0
                        return (out)

                    clfy2["peak"] = pct_change(clfy, date(2020,4,1), date(2019,4,1))
                    clfy2["recovery"] = pct_change(clfy, endmonth, endmonth_2019)

                    ##### categories for drop / rise / recovery 
                    ###################################################################### 
                    condlist = [(clfy2["peak"]>15), ## increase
                                ((clfy2["peak"]<15)&(clfy2["peak"]>-15)), ## no change
                                (clfy2["peak"]>-60), # small drop
                                (clfy2["peak"]<-60)] # large drop
                    choicelist = ["Increase", "No change", "Small drop", "Large drop"]
                    clfy2["april_position"] = np.select(condlist, choicelist, default="Other") 

                    condlist = [(clfy2["recovery"]>15), ## increase
                                ((clfy2["recovery"]<15)&(clfy2["recovery"]>-15)), ## no change
                                (clfy2["recovery"]>-60), # small drop
                                (clfy2["recovery"]<-60)] # large drop
                    choicelist = ["Increase", "No change", "Small drop", "Large drop"]
                    clfy2[f"{endmonthname}_position"] = np.select(condlist, choicelist, default="Other") 

                    condlist = [((clfy2["april_position"]=="Increase")|(clfy2[f"{endmonthname}_position"]=="Increase")), ## increase
                                 ((clfy2["april_position"]=="No change")), ## no change
                                 ((clfy2[f"{endmonthname}_position"]=="Small drop")|(clfy2[f"{endmonthname}_position"]=="Large drop")), # sustained drop
                                 ((clfy2["peak"]<-15) & (clfy2["recovery"]>-15))] # drop with recovery
                    choicelist = ["Increase", "No change", "Sustained drop", "Recovered"]
                    clfy2["overall_position"] = np.select(condlist, choicelist, default="Other") 

                    feb_median = round(clfy[date(2020,2,1)],1)
                    apr_median = round(clfy[date(2020,4,1)],1)
                    endmonth_median = round(clfy[endmonth],1)
                    feb_idr = round(clfy["2020-02-01_IDR"],1)
                    apr_idr = round(clfy["2020-04-01_IDR"],1)
                    endmonth_idr = round(clfy[f"{endmonth}_IDR"],1)
                    peak = round(clfy2["peak"],1)
                    recovery = round(clfy2["recovery"],1)
                    april_position = clfy2["april_position"]
                    endmonth_position = clfy2[f"{endmonthname}_position"]
                    pos = clfy2["overall_position"]
                    ######################################################################
                    
                    e_mill = round(e_mill, 2)
                    if pts>1000000:
                        pt_count = str(round(pts/1000000, 2))+ "m"
                    else:
                        pt_count = str(round(pts/1000, 1))+ "k"
                        
                    Title = f'''"{code}" - {desc} \n ### (Practices included: {practice_count}k ({practices_percent}%); 2020 patients: {pt_count}; 2020 events: {e_mill}m)'''
                    display(Markdown(f"## {Title}"))
                    
                    total_events = round(out["numerator"].sum(), 1)

                    if total_events>10:
                        display(Markdown(f"Feb median: {feb_median} (IDR {feb_idr}), April median: {apr_median} (IDR {apr_idr}), {endmonthname} median: {endmonth_median} (IDR {endmonth_idr})"))
                        display(Markdown(f"Change in median from 2019: April {peak}% ({april_position}); {endmonthname} {recovery}%, ({endmonth_position}); Overall classification: **{pos}**"))
                        
                        plot_charts(out, outer_percentiles=False)
                        
                        if (digits==2) | (digits==3):   ## display top 5 codes within each parent code
                            display(Markdown(f"Top 'child' codes represented within parent code above:"))
                            subs = subcodes.copy().loc[subcodes["parent_code"]==code].drop("parent_code",1).head(5)
                            display(subs)
                            if second_chart==True:
                                top_test = subs.head(1)
                                if (len(top_test)==0):
                                    pass
                                else:
                                    code2 = top_test["first_digits"].values[0]
                                    desc2 = top_test["Description"].values[0]

                                    df2 = df0.loc[df0["first_digits"].str[:len(code2)]==code2]
                                    df2, _, _ = all_pracs(df0, df2, code2)

                                    if (code2!=code):
                                        out = df2.copy()
                                        out["value"] = 1000*df2["numerator"]/df2["denominator"]

                                        # title
                                        display(Markdown(f"### Trend in top child code: {code2} - {desc2}"))

                                        total_events = out["numerator"].sum()
                                        plot_charts(out, outer_percentiles=False)
                                    else:
                                    	pass
                            else:
                                pass
                    else:
                        display (Markdown(f"### {desc}: _Too few events to plot_"))
                        pass

                else:
                    pass
                

            
    
def filter_codelists(df, keywords=None, concepts=None, eventcount=False, in_or_out="out", codelist_type=None):
    
    '''
    Filter codelists to items of interest based on keywords and/or concepts
    
    Inputs:
    df (dataframe): codelist
    keywords (list): keywords to search code descriptions (can be partial words such as "imag")
    concept (list): ctv3 concepts to be filtered
    in_or_out (str): "in or "out" filter method
    codelist_type (str): "High level" or "Detailed"
    
    Returns:
    out (df): filtered codelist
    '''    
    
    full_list = df.copy()
    
    # setup empty dataframe
    filtered_list = pd.DataFrame()

    # filter keywords
    if keywords is None or keywords==[]:
        keywords=[]
    else:
        for k in keywords:
            if in_or_out == "in":
                filtered_list = pd.concat([filtered_list, full_list.loc[full_list["Description"].fillna("").str.contains(k, flags=re.IGNORECASE)]]).drop_duplicates()
            else:
                filtered_list = full_list.loc[~full_list["Description"].fillna("").str.contains(k, flags=re.IGNORECASE)]
                
    # filter concepts       
    if concepts is None or concepts==[]:
        concepts = []
        if len(filtered_list)==0:
            filtered_list = full_list
    else:
        for c in concepts:
            if in_or_out == "in":
                filtered_list = pd.concat([filtered_list, full_list.loc[full_list["concept_desc"]==c] ])
            else:
                if len(filtered_list)>0: # if the list was populated in previous step, filter it further
                    filtered_list = filtered_list.loc[filtered_list["concept_desc"]!=c]
                else: # if no existing list, filter the full list
                    filtered_list = full_list.loc[full_list["concept_desc"]!=c]
                

    filtered_list = filtered_list.drop_duplicates().sort_values(by="2020 events (mill)", ascending=False)
    
    if codelist_type is None:
        codelist_type = ""

    display(Markdown(f"{codelist_type} codes: {len(filtered_list)}"))
    
    if eventcount == True:
        total_events = filtered_list["2020 events (mill)"].sum().round(2)
        display(Markdown(f"Event count: {total_events} million"))
    
    return filtered_list


def load_filter_codelists(end_date, keywords=None, concepts=None, in_or_out="in"):
    '''
    Import codelists and filter to items of interest based on keywords and/or concepts
    
    Inputs:
    end_date (str): end date of data ("YYYYMMDD")
    keywords (list): keywords to search code descriptions (can be partial words such as "imag")
    concept (list): ctv3 concepts to be filtered
    in_or_out (str): whether to filter the selected keywords/concepts in or out of the results
    
    Returns:
    out1 (df): high level codelist
    out2 (df): more detailed codelist
    '''
    
    # load codelist csvs
    highlevel_full = pd.read_csv(os.path.join("..","output",f"level_two_codes_{end_date}.csv"))
    detailed_full = pd.read_csv(os.path.join("..","output",f"combined_codelist_{end_date}.csv"))

    out1 = filter_codelists(highlevel_full, keywords, concepts, in_or_out="in", codelist_type="High level")
    out2 = filter_codelists(detailed_full, keywords, concepts, in_or_out="in", codelist_type="Detailed")
    return out1, out2

