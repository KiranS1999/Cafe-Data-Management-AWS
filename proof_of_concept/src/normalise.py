"""Functions to transform the csv data into transaction/product/order information
and normalise to 3NF""" 

import pandas as pd

def create_transaction_df(df):
    """creates transaction dataframe for 3NF

    Args:
        df (dataframe): cleaned dataframe

    Returns:
        transaction_df: dataframe with transaction data
    """
    transaction_df = df.copy()
    transaction_df.reset_index(drop=True, inplace=True)
    transaction_df.drop((df.columns[2]), axis=1, inplace=True)
    transaction_df.rename(columns = { 0:'purchase_date', 1:'branch_location', 4:'total_spend', 5:'payment_method'}, inplace=True)
    
    date_time = transaction_df.purchase_date.str.split(" +", expand=True)
    date_time.rename(columns = {0:'date', 1:'time'})
    date= date_time[0]
    time=date_time[1]
    transaction_df.insert(1, "time", time)
    transaction_df.insert(1, "date", date)
    transaction_df.drop(['purchase_date'], axis=1, inplace=True)
    transaction_df['date'] = pd.to_datetime(transaction_df['date'])
    return transaction_df

def create_product_df(df):
    """creates product info dataframe for 3NF

    Args:
        df (dataframe): cleaned dataframe

    Returns:
        product_df: product dataframe 
    """
    product_items = df.copy()
    product_items.drop(product_items.columns[[0, 1, 3, 4]], axis=1, inplace=True)
    products_list = product_items.values.tolist()
    product_df = pd.DataFrame(columns=['product_name', 'product_flavour', 'price'])
   
    for items in products_list:
        
        for sep_order in items:
            
            sep_order = sep_order.split(',')

            for item in sep_order: 
                product_info = item.split('-') 
    
                if len(product_info) == 3:
                    product, flavour, price = product_info
                    product = product.strip()
                    flavour = flavour.strip()
                    price = price.strip()
                    row = pd.Series({'product_name':product, 'product_flavour':flavour, 'price':price}) 
                    product_df = pd.concat([product_df, pd.DataFrame([row], columns=row.index)]).reset_index(drop=True)
                
                elif len(product_info) == 2:
                    product, price = product_info
                    product = product.strip()
                    price = price.strip()
                    row = pd.Series({'product_name':product, 'product_flavour':'', 'price':price}) 
                    product_df = pd.concat([product_df, pd.DataFrame([row], columns=row.index)]).reset_index(drop=True)
            
    
    product_df.drop_duplicates(subset=None, keep="first", inplace=True)
    return product_df

def create_order_df(df, product_df):
    """creates order info dataframe for 3NF

    Args:
        df (dataframe): cleaned dataframe

    Returns:
        order_df: order dataframe
    """
    dataframe_items = df.copy()
    dataframe_items.drop(dataframe_items.columns[[0, 1, 3, 4]], axis=1, inplace=True)
    products_list = dataframe_items.values.tolist()

    order_df = pd.DataFrame(columns=['transaction_id', 'product_name', 'product_flavour', 'price'])
    
    
    count =1
   
    for items in products_list:
        
        for sep_order in items:
            
            sep_order = sep_order.split(',')

            for item in sep_order: 
                product_info = item.split('-') 

                if len(product_info) == 3:
                    product, flavour, price = product_info
                    product = product.strip()
                    flavour = flavour.strip()
                    price = price.strip()
                    row = pd.Series({'transaction_id':count,'product_name':product, 'product_flavour':flavour, 'price':price}) 
                    order_df = pd.concat([order_df, pd.DataFrame([row], columns=row.index)]).reset_index(drop=True)
                
                elif len(product_info) == 2:
                    product, price = product_info
                    product=product.strip()
                    price = price.strip()
                    row = pd.Series({'transaction_id':count,'product_name':product, 'product_flavour':'', 'price':price}) 
                    order_df = pd.concat([order_df, pd.DataFrame([row], columns=row.index)]).reset_index(drop=True)
            count+=1

    product_df['id']=product_df.index
    order_df = pd.merge(order_df, product_df, on=['product_name','product_flavour', 'price'], how='left')
    order_df.drop(order_df.columns[[1, 2, 3]], axis=1, inplace=True)
    product_id = order_df.pop('id')
    order_df.insert(1, 'product_id', product_id)
    return order_df

def third_nf(df):
    """normalises a dataframe to 3NF

    Args:
        df (dataframe): cleaned dataframe
    Returns:
        normalised (list): a list of dataframes that are in 3NF
    """
    
    transaction_df = create_transaction_df(df)
    product_df = create_product_df(df)
    order_df = create_order_df(df, product_df)
    print('count transcation df' , len(transaction_df.axes[1]))
    print('count product df' , len(product_df.axes[1]))
    print('count order df' , len(order_df.axes[1]))
    return transaction_df, product_df, order_df

