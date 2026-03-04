import sqlite3
import pandas as pd
import logging
from ingestion_db import ingest_db

# Logging configuration
logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.DEBUG,
    format= "%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

def create_vendor_summary(conn):
    '''Joins tables to create a comprehensive vendor performance summary'''
    query = """
    WITH FreightSummary AS (
        SELECT VendorNumber, SUM(Freight) AS FreightCost
        FROM vendor_invoice
        GROUP BY VendorNumber
    ),
    PurchaseSummary AS (
        SELECT
            p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice,
            pp.Price AS ActualPrice, pp.Volume,
            SUM(p.Quantity) AS TotalPurchaseQuantity,
            SUM(p.Dollars) AS TotalPurchaseDollars
        FROM purchases p
        JOIN purchase_prices pp ON p.Brand = pp.Brand
        WHERE p.PurchasePrice > 0
        GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice, pp.Price, pp.Volume
    ),
    SalesSummary AS (
        SELECT VendorNo, Brand,
            SUM(SalesQuantity) AS TotalSalesQuantity,
            SUM(SalesDollars) AS TotalSalesDollars,
            SUM(SalesPrice) AS TotalSalesPrice,
            SUM(ExciseTax) AS TotalExciseTax
        FROM sales
        GROUP BY VendorNo, Brand
    )
    SELECT
        ps.*, ss.TotalSalesQuantity, ss.TotalSalesDollars,
        ss.TotalSalesPrice, ss.TotalExciseTax, fs.FreightCost
    FROM PurchaseSummary ps
    LEFT JOIN SalesSummary ss ON ps.VendorNumber = ss.VendorNo AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary fs ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC
    """
    return pd.read_sql_query(query, conn)

def clean_data(df):
    '''Cleans the data and calculates business KPIs'''
    df = df.copy()
    
    # Standardizing Volume to float
    df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0)
    
    # Handling missing values
    df.fillna(0, inplace=True)
    
    # Trimming whitespace
    df['VendorName'] = df['VendorName'].astype(str).str.strip()
    df['Description'] = df['Description'].astype(str).str.strip()
    
    # KPI Calculations
    df['Grossprofit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    
    # Using replace to handle potential Infinity (division by zero)
    df['ProfitMargin'] = (df['Grossprofit'] / df['TotalSalesDollars'] * 100).replace([float('inf'), -float('inf')], 0).fillna(0)
    df['stockTurnover'] = (df['TotalSalesQuantity'] / df['TotalPurchaseQuantity']).replace([float('inf'), -float('inf')], 0).fillna(0)
    df['SalesToPurchaseRatio'] = (df['TotalSalesDollars'] / df['TotalPurchaseDollars']).replace([float('inf'), -float('inf')], 0).fillna(0)
    
    return df

if __name__ == '__main__':
    conn = None
    try:
        # 1. Establish connection
        conn = sqlite3.connect('inventory.db')
        logging.info('--- ETL Process Started ---')

        # 2. Extract
        logging.info('Creating vendor summary Table....')
        summary_df = create_vendor_summary(conn)
        
        # 3. Transform
        logging.info('Cleaning Data and calculating metrics....')
        clean_df = clean_data(summary_df)
        
        # 4. Load
        logging.info(f'Ingesting {len(clean_df)} records into Vendor_sales_summary....')
        ingest_db(clean_df, 'Vendor_sales_summary', conn)
        
        logging.info('--- ETL Process Completed Successfully ---')
        print("Success: Vendor summary created and ingested.")

    except Exception as e:
        logging.error(f"Critical error: {e}")
        print(f"Error occurred: {e}")
    finally:
        if conn:
            conn.close()
            logging.info('Database connection closed.')