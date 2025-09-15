import sqlite3
import pandas as pd
import logging
from ingestion_db import ingest_db

logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.DEBUG , 
    format="%(asctime)s - %(levelname)s - %(message)s" , 
    filemode="a"
)


def create_vendor_summary(conn):
    """this function will merge the different tables to get the overall vendor summary and adding new columns int the resultant data """
    vendor_sales_summary = pd.read_sql_query("""
    WITH
    FreightSummary AS(
    SELECT
        VendorNumber ,
        sum(Freight) AS FreightCost
        from vendor_invoice
        group by VendorNumber  
    ), 
    
    PurchaseSummary AS(
    SELECT 
        p.VendorNumber, 
        p.VendorName, 
        p.Brand , 
        p.Description,
        p.PurchasePrice ,
        pp.Price as ActualPrice ,
        pp.Volume,
        sum(p.Quantity) as TotalPurchasesQuantity,
        sum(p.Dollars) as TotalPurchaseDollars
    from Purchases p
    join Purchase_prices pp on p.Brand = pp.Brand
    where p.PurchasePrice > 0 
    group by p.VendorNumber, p.VendorName, p.Brand , p.Description,p.PurchasePrice ,pp.Price ,pp.Volume) , 
    
    SalesSummary As( 
    SELECT 
        VendorNo ,
        Brand , 
        sum(SalesQuantity) as TotalSalesQuantity , 
        sum(SalesDollars) as TotalSalesDollars,
        sum(SalesPrice) as TotalSalesPrice, 
        sum(ExciseTax) as TotalExciseTax 
    from sales
    group by VendorNo , Brand
    )
    
    select 
        ps.VendorNumber, 
        ps.VendorName, 
        ps.Brand , 
        ps.Description,
        ps.PurchasePrice , 
        ps.ActualPrice ,
        ps.Volume,
        ps.TotalPurchasesQuantity,
        ps.TotalPurchaseDollars ,
        ss.TotalSalesQuantity , 
        ss.TotalSalesDollars,
        ss.TotalSalesPrice, 
        ss.TotalExciseTax ,
        fs.FreightCost 
    from PurchaseSummary ps 
    left join SalesSummary ss
    on ps.VendorNumber = ss.VendorNo
    and ps.Brand = ss.Brand
    left join FreightSummary fs
    on ps.VendorNumber = fs.VendorNumber
    order by ps.TotalPurchaseDollars desc
    """,conn)

    return vendor_sales_summary


# def clean_data(df):
#     '''This function will clean the data'''
#     # Changing datatype to float
#     df['Volumn'] = df['Volumn'].astype('float')

#     # Filling spaces from categorical columns
#     df['VendorName'] = df['VendorName'].str.strip()
#     df['Decription'] = df['Decription'].str.strip()

#     # creating new columns for better analysis
#     vendor_sales_summary['GrossProfit'] = vendor_sales_summary['TotalSalesDollars'] - vendor_sales_summary['TotalPurchaseDollars']
#     vendor_sales_summary['ProfitMargin'] = (vendor_sales_summary['GrossProfit'] / vendor_sales_summary['TotalSalesDollars']  ) * 100 
#     [vendor_sales_summary['StockTurnover']] = vendor_sales_summary['TotalSalesQuantity'] / vendor_sales_summary['TotalPurchasesQuantity']
#     vendor_sales_summary['SalesPurchaseRatio'] = vendor_sales_summary['TotalSalesDollars'] / vendor_sales_summary['TotalPurchaseDollars'] 

#     return df

def clean_data(df):
    '''This function will clean the data'''
    # Changing datatype to float
    df['Volume'] = df['Volume'].astype('float64')
    
    # filling missing value with 0
    df.fillna(0,inplace= True)
    
    # Filling spaces from categorical columns
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()
    

    # creating new columns for better analysis
    # vendor_sales_summary['GrossProfit'] = vendor_sales_summary['TotalSalesDollars'] - vendor_sales_summary['TotalPurchaseDollars']
    # vendor_sales_summary['ProfitMargin'] = (vendor_sales_summary['GrossProfit'] / vendor_sales_summary['TotalSalesDollars']) * 100 
    # vendor_sales_summary['StockTurnover'] = vendor_sales_summary['TotalSalesQuantity'] / vendor_sales_summary['TotalPurchasesQuantity']
    # vendor_sales_summary['SalesPurchaseRatio'] = vendor_sales_summary['TotalSalesDollars'] / vendor_sales_summary['TotalPurchaseDollars']

     df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    df['ProfitMargin'] = (df['GrossProfit'] / df['TotalSalesDollars']) * 100 
    df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchasesQuantity']
    df['SalesPurchaseRatio'] = df['TotalSalesDollars'] / df['TotalPurchaseDollars']

    return df



if __name__ == '__main__':
    # creating database connection 
    conn = sqlite3.connect('inventory.db')

    logging.info('Creating Vendor Summary Table...')
    summary_df = create_vendor_summary(conn)
    logging.info(summary_df.head())

    logging.info('Cleaning Data...')
    clean_df = clean_data(summary_df)
    logging.info(clean_df.head()) 

    logging.info('Ingesting Data...')
    ingest_db(clean_df , 'vendor_sales_summary' ,conn )
    logging.info('Completed')