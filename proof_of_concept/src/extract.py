"""Functions to extract data from csv, remove sensititve info 
and convert into a pandas dataframe"""

import pandas as pd

 
def extract_clean_data(file_to_clean):
    """take csv file and remove sensitive data

    Args:
        file_to_clean (string): path to file that would be cleaned

    Returns:
        Dataframe: file with senstitve information removed
    """
    dataframe = pd.read_csv(file_to_clean, header=None)
    dataframe.drop(dataframe.columns[[2,6]], axis=1, inplace=True)
    return dataframe