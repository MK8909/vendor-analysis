import sqlite3
import pandas as pd
import logging

from ingestion_db import ingest_db

logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)


def create_vendor_summary(conn):
    '''this function will merege the different tables to get overalll vendor summary and adding new columns in resultant data'''
    vendor_sales_summary = pd.read_sql_query("""
                                              
    WITH FreightSummary AS (
    SELECT VendorNumber, SUM(Freight) AS FreightCost
    FROM vendor_invoice
    GROUP BY VendorNumber
    ),

    PurchasesSummary AS (
    SELECT 
        p.VendorNumber,
        p.VendorName,
        p.Brand,
        p.Description,
        p.PurchasePrice,
        pp.Price AS ActualPrice,
        pp.Volume,
        SUM(p.Quantity) AS TotalPurchaseQuantity,
        SUM(p.Dollars) AS TotalPurchaseDollars
    FROM purchases p
    JOIN purchase_prices pp
        ON p.Brand = pp.Brand
    WHERE p.PurchasePrice > 0
    GROUP BY 
        p.VendorNumber,
        p.VendorName,
        p.Brand,
        p.Description,
        p.PurchasePrice,
        pp.Price,
        pp.Volume
    ),

    SalesSummary AS (
    SELECT
        VendorNo,
        Brand,
        SUM(SalesQuantity) AS TotalSalesQuantity,
        SUM(SalesDollars) AS TotalSalesDollars,
        SUM(SalesPrice) AS TotalSalesPrice,
        SUM(ExciseTax) AS TotalExciseTax
    FROM sales
    GROUP BY VendorNo, Brand
   )

    SELECT
    ps.VendorNumber,
    ps.VendorName,
    ps.Brand,
    ps.Description,
    ps.PurchasePrice,
    ps.ActualPrice,
    ps.Volume,
    ps.TotalPurchaseQuantity,
    ps.TotalPurchaseDollars,
    ss.TotalSalesQuantity,
    ss.TotalSalesDollars,
    ss.TotalSalesPrice,
    ss.TotalExciseTax,
    fs.FreightCost
    FROM PurchasesSummary ps
    LEFT JOIN SalesSummary ss
    ON ps.VendorNumber = ss.VendorNo
    AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary fs
    ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC
    """, conn)
    return vendor_sales_summary

def clean_data(df):
    '''this function will clean the data'''
    df['Volume']=df['Volume'].astype('float')

    df.fillna(0,inplace=True)

    ##removing spaces from categorical columns
    df['VendorName']=df['VendorName'].str.strip()
    df['Description']=df['Description'].str.strip()

    ##creating new columns
    vendor_sales_summary['GrossProfit']=vendor_sales_summary['TotalSalesDollars'] - vendor_sales_summary['TotalPurchaseDollars'] 
    vendor_sales_summary['ProfitMargin']=(vendor_sales_summary['GrossProfit']/vendor_sales_summary['TotalSalesDollars'])*100 
    vendor_sales_summary['StockTurnover']=vendor_sales_summary['TotalSalesQuantity']/vendor_sales_summary['TotalPurchaseQuantity']
    vendor_sales_summary['SalesToPurchaseRatio']=vendor_sales_summary['TotalSalesDollars']/vendor_sales_summary['TotalPurchaseDollars']
    
    return df


if __name__ == '__main__':
    #creating dtabase connection
    conn = sqlite3.connect('vendors.db')
    
    logging.info('creating vendor summary table..')
    summary_df=create_vendor_summary(conn)
    logging.info(summary_df.head())

    
    logging.info('cleaning data...')
    clean_df=clean_data(summary_df)
    logging.info(summary_df.head())

    
    logging.info('Ingesting data...')
    ingest_db(clean_df, 'vendor_sales_summary', conn)
    logging.info('Data Ingested Successfully into vendor_sales_summary table')