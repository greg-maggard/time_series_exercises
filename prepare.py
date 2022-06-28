import acquire

import pandas as pd
import matplotlib.pyplot as plt

def prepare_store_data():
    df = acquire.get_store_item_sales_data()
    #Adding line below from Adam's review to eliminate the extra timestamp:
    df.sale_date = df.sale_date.apply(lambda date: date[:-13])
    #Converting the column to datetime:
    df.sale_date = pd.to_datetime(df.sale_date, format='%a, %d %b %Y')
    #Setting the index to the date and sorting by the date:
    df = df.set_index('sale_date').sort_index()
    #Adding a 'month' column based on sale_date:
    df['month'] = df.index.strftime('%B')
    #Adding a 'day_of_week' column based on sale_date:
    df['day_of_week'] = df.index.strftime('%A')
    #Adding a 'sales_total' column by multiplying 'sale_amount' and 'item_price':
    df['sales_total'] = df.sale_amount * df.item_price
    return df

def prepare_energy_data():
    #Importing energy data via acquire function:
    df = acquire.get_german_energy_data()
    #Making all columns lower case for simplicity:
    df.columns = [x.lower() for x in df.columns]
    #Converting the date column to datetime with the appropriate format:
    df.date = pd.to_datetime(df.date, format='%Y-%m-%d')
    #Setting the index to the date and sorting by the data by date:
    df = df.set_index('date').sort_index()
    #Adding a month column based on 'date':
    df['month'] = df.index.strftime('%B')
    #Adding a 'year' column based on 'date':
    df['year'] = df.index.strftime('%Y')
    #Replacing all null values with 0:
    df = df.fillna(0)
    return df