# OpenSAFELY NHS Service Restoration Observatory - Data Driven analysis

This is the code and configuration for two papers describing trends and variation in NHS Primary care activity for 23.3 million patients in England, using data-driven analysis to identify common clinical codes, grouped into eight clinical topics. 


# OpenSAFELY NHS Service Restoration Observatory 1: describing trends and variation in primary care clinical activity for 23.3 million patients in England during the first wave of COVID-19

This is our first paper, published in BJGP [here](https://bjgp.org/content/72/714/e63.long). Here we focus on pathology and respiratory clinical activity.

- Jupyter notebooks containing all graphs are in [/notebooks](https://github.com/opensafely/restoration-observatory-intro-notebook/tree/master/notebooks). If notebooks do not load you can use https://nbviewer.jupyter.org/
- Code dictionaries are available in [/data](https://github.com/opensafely/restoration-observatory-intro-notebook/tree/master/data)


# OpenSAFELY NHS Service Restoration Observatory 2: Changes in primary care activity across six clinical areas

This is our second paper, available as a preprint shortly, which cover six further clinical topics. 

- Jupyter notebooks containing all graphs are in [/notebooks](https://github.com/opensafely/restoration-observatory-intro-notebook/tree/master/notebooks). If notebooks do not load you can use https://nbviewer.jupyter.org/
- Code dictionaries are available in [/data](https://github.com/opensafely/restoration-observatory-intro-notebook/tree/master/data)


# About the OpenSAFELY framework
The OpenSAFELY framework is a new secure analytics platform for electronic health records research in the NHS.

Instead of requesting access for slices of patient data and transporting them elsewhere for analysis, the framework supports developing analytics against dummy data, and then running against the real data within the same infrastructure that the data is stored. Read more at [OpenSAFELY.org](https://opensafely.org/). In order to conduct emergency analytics and support development of a framework within OpenSAFELY for rapid operational research, this specific analysis utilised the [EBM DataLab template notebook project](https://github.com/ebmdatalab/datalab-notebook-template). These notebooks were deployed within the TPP datastore and finalised results were aproved for publication in line with OpenSAFELY methodolgy. A "measures" framework is now available in OpenSAFELY to support rapid operational research. Read more in the [OpenSAFELY documentation](https://docs.opensafely.org/en/latest/). 

