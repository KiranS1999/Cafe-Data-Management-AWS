from unittest.mock import patch, Mock
from normalise import create_transaction_df, create_product_df, create_order_df
import pandas as pd


def test_create_transaction_df(): 
    df = pd.DataFrame(
            {
                "purchase_date": "01/01/2023 09:00",
                "branch_location": "Chesterfield",
                "items": "Regular Flavoured iced latte - Hazelnut - 2.75, Large Latte - 2.45",
                "total_spend": "8.00",
                "payment_method": "CARD", 
            }, index=[0])

    expected = pd.DataFrame(
            {
                "date": "2023-01-01",
                "time": "09:00",
                "branch_location": "Chesterfield",
                "total_spend": "8.00",
                "payment_method": "CARD", 
            }, index=[0])
    expected['date'] = pd.to_datetime(expected['date'])        

    actual = create_transaction_df(df)
    pd.testing.assert_frame_equal(actual, expected)
                 

def test_create_product_df():
    df = pd.DataFrame(
        {
            "purchase_date": "01/01/2023 09:00",
            "branch_location": "Chesterfield",
            "items": "Regular Flavoured iced latte - Hazelnut - 2.75, Large Latte - 2.45",
            "total_spend": "8.00",
            "payment_method": "CARD", 
        }, index=[0]
        )

  
    expected = pd.DataFrame(
        {
            "product_name": ["Regular Flavoured iced latte", "Large Latte" ],
            "product_flavour": ["Hazelnut", ""],
            "price": ["2.75", "2.45"],
            
        
        }, index=[0,1]
        )

    actual = create_product_df(df)
    pd.testing.assert_frame_equal(actual, expected)
                 

def test_create_order_df():
    
    product_df = pd.DataFrame(
        {
            "product_name": ["Regular Flavoured iced latte", "Large Latte" ],
            "product_flavour": ["Hazelnut", ""],
            "price": ["2.75", "2.45"],
            
        
        }, index=[0,1]
        )
    
    df = pd.DataFrame(
    {
        "purchase_date": "01/01/2023 09:00",
        "branch_location": "Chesterfield",
        "items": "Regular Flavoured iced latte - Hazelnut - 2.75, Large Latte - 2.45",
        "total_spend": "8.00",
        "payment_method": "CARD", 
    }, index=[0]
    )

    expected = pd.DataFrame(
    {
        "transaction_id": ['1','1'],
        "product_id": ['0','1'],  
    }, index=[0,1]
    )

    actual = create_order_df(df, product_df)
    pd.testing.assert_frame_equal(actual, expected)

test_create_order_df()
