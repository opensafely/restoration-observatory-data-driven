# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: all
#     notebook_metadata_filter: all,-language_info
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.3.3
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# # Changes occurring in primary care DIABETES activity during the pandemic
#
# ## Producing decile charts to show activity across all practices in TPP
#
# - Jump to [charts](#detailed)

# +
import pyodbc
import pandas as pd
import os
from IPython.display import display, Markdown


dbconn = os.environ.get('DBCONN', None)
if dbconn is None:
    display("No SQL credentials. Check that the file 'environ.txt' is present. Refer to readme for further information")
else:
    dbconn = dbconn.strip('"')
    
# import custom functions from 'analysis' folder
import sys
sys.path.append('../lib/')
    
from functions import closing_connection, load_filter_codelists, filter_codelists, plotting_all


# global variables 
if 'OPENCoronaExport' in dbconn:
    threshold = 1 # min activity threshold in dummy data
else:
    threshold = 1000

# descriptions for ctv3 codes    
codes = pd.read_csv(os.path.join('..','data','code_dictionary.csv'))

# +
######################### customise ################################
# Create lists of keywords and/or CTV3 concepts to filter codelist
keywords = ["diabe", " DM", "DM ", "insulin", "hypoglycaem", "desmond", "a1c"]
concepts = []

display(Markdown(f"## Load list of common codes from csv and filter to Diabetes only"))

end_date = "20201231"
####################################################################


highlevel, detailed = load_filter_codelists(end_date, keywords=keywords, concepts=concepts)
# -

# ### Additional manual filtering
# Customised for each topic after inspecting the results of the previous step

# +
######################### customise ################################
# remove irrelevant items by filtering on CTV3 concepts/descriptions
concepts1 = ["Drug"]
keywords1 = []
concepts2 = [] 
keywords2 = []
####################################################################


highlevel = filter_codelists(highlevel, concepts=concepts1, keywords=keywords1, codelist_type="High level",  eventcount=True)
detailed = filter_codelists(detailed, concepts=concepts2, keywords=keywords2, codelist_type="Detailed",  eventcount=True) 


######################### customise ################################
# replace all detailed concepts with single topic
highlevel["concept_desc"] = "Diabetes"
detailed["concept_desc"] = "Diabetes"
####################################################################
# -

# # Plotting decile charts
# ### Charts highlight the median practice for the rate of each code occurrence each month, along with the rates at each decile (10-90%)
# - NB Practice denominators (list size) and patient registrations only include current patients

# # Detailed codes <a id="detailed"></a>

N = min(len(detailed), 75) # number of charts to plot
plotting_all(detailed, codes, N, threshold, end_date, dbconn, False)
