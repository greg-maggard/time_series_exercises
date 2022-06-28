import os
import pandas as pd
import requests

def get_api_items_data():
    url = 'https://python.zgulde.net/api/v1/items'
    response = requests.get(url)
    data = response.json()
    items = pd.DataFrame(data['payload']['items'])
    base_url = 'https://python.zgulde.net'
    response = requests.get(base_url + data['payload']['next_page'])
    data = response.json()
    items = pd.concat([items, pd.DataFrame(data['payload']['items'])]).reset_index()
    response = requests.get(base_url + data['payload']['next_page'])
    data = response.json()
    items = pd.concat([items, pd.DataFrame(data['payload']['items'])]).reset_index()
    return items

def get_api_store_data():
    url = 'https://python.zgulde.net/api/v1/stores'
    response = requests.get(url)
    data = response.json()
    stores = pd.DataFrame(data['payload']['stores'])
    return stores

def get_api_sales_data():
    #Retrieving Sales Information:
    endpoint = '/api/v1/sales'
    sales = []
    # For each page -- until next page is None
    #loop the process of adding pages to database
    while endpoint != None:
        base_url = 'https://python.zgulde.net'
        url = base_url + endpoint
        response = requests.get(url)
        data = response.json()
        #grab new end point
        sales.extend(data['payload']['sales'])
        endpoint = data['payload']['next_page']
    sales = pd.DataFrame(sales)
    return sales

def join_store_data(sales, items, stores):
    ##Cleaning up a bit and joining the DataFrames:
    #joining sales and stores data:
    store_sales = sales.join(stores.set_index('store_id'), on = 'store_id')
    #Joining item data to sales and store data:
    store_item_sales = items.join(store_sales.set_index('item'), on = 'item_id')
    #Dropping residual columns from join:
    store_item_sales = store_item_sales.drop(columns = ['level_0', 'index'])
    return store_item_sales

def get_items_data():
    '''Makes an API call that checks for a CSV of item data. Otherwise, it pulls store information from a website.'''
    # Retrieving item information:
    if os.path.exists('items.csv'):
        return pd.read_csv('items.csv')
    items = get_api_items_data()
    items.to_csv('items.csv', index = False)
    return items

def get_store_data():
    '''Makes an API call that checks for a CSV of store data. Otherwise, it pulls item information from a website.'''
    # Retrieving store information:
    if os.path.exists('stores.csv'):
        return pd.read_csv('stores.csv')
    stores = get_api_store_data()
    stores.to_csv('stores.csv', index = False)
    return stores

def get_sales_data():
    '''Makes an API call that checks for a CSV of sales data. Otherwise, it pulls sales information from a website.'''
    # Retrieving sales information:
    if os.path.exists('sales.csv'):
        return pd.read_csv('sales.csv')
    sales = get_api_sales_data()
    #renaming 'store' column to 'store_id' to match up with column in stores table for join 
    sales = sales.rename(columns = {'store' : 'store_id'})
    sales.to_csv('sales.csv', index = False)
    return sales

def get_store_item_sales_data():
    items = get_items_data()
    sales = get_sales_data()
    stores = get_store_data()
    df = join_store_data(sales, items, stores)
    return df

def get_german_energy_data(url = 'https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv'):
    df = pd.read_csv(url)
    return df



