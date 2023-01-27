from unittest.mock import patch, Mock
from extract import extract_clean_data
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

@patch("extract.pd.read_csv")

def test_extract_clean_data(mock_read_csv):
    
    mock_read_csv.return_value = pd.DataFrame(
            {
                "Date-": "2023-01-01 09:00:00",
                "location": "Chesterfield",
                "customer_name": "David Tennant",
                "items": "Large,Hot chocolate,5.4,Large,Milk latte,2.6" ,
                "total_amount": "8.00",
                "payment_type": "CARD",
                "card_number": "9489084752450204",
            }, index=[0])

    expected = pd.DataFrame(
            {
                "Date-": "2023-01-01 09:00:00",
                "location": "Chesterfield",
                "items": "Large,Hot chocolate,5.4,Large,Milk latte,2.6",
                "total_amount": "8.00",
                "payment_type": "CARD", 
            }, index=[0])

    
    actual = extract_clean_data(mock_read_csv)
    pd.testing.assert_frame_equal(actual, expected)
        
