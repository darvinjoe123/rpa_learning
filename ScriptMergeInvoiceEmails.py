import pandas as pd
import numpy as np
import argparse 
def process_invoices_and_emails(invoice_file_path: str, customer_mail_file_path: str, output_file_path: str):
    """
    Reads invoice and customer email data from Excel files, merges them to find
    recipient emails, reports the status of the lookup, and writes the result
    to a new Excel file.
 
    Args:
        invoice_file_path (str): The file path for the invoice data Excel sheet.
        customer_mail_file_path (str): The file path for the customer emails Excel sheet.
        output_file_path (str): The file path where the output Excel file will be saved.
    """
    try:
        # --- 1. DATA LOADING ---
        print(f"Reading invoice data from: {invoice_file_path}")
        df_invoices = pd.read_excel(invoice_file_path,dtype={'Billing Document Number': str})
        print(f"Reading customer email data from: {customer_mail_file_path}")
        df_mails = pd.read_excel(customer_mail_file_path)
        print("Successfully loaded data. Starting processing...")
 
        # --- 2. DATA CLEANING AND PREPARATION ---
 
        # Step 2a: Standardize the customer identifier in the customer mails DataFrame.
        print("Cleaning and standardizing 'Customer' column in mail master...")
        df_mails.dropna(subset=['Customer'], inplace=True)
        df_mails['Cleaned Customer ID'] = pd.to_numeric(df_mails['Customer'].astype(str), errors='coerce')
        df_mails.dropna(subset=['Cleaned Customer ID'], inplace=True)
        df_mails['Cleaned Customer ID'] = df_mails['Cleaned Customer ID'].astype('Int64').astype(str)
 
        # Step 2b: Process the customer emails DataFrame.
        print("Aggregating and de-duplicating customer emails...")
        email_aggregator = lambda emails: '; '.join(emails.dropna().unique())
        df_email_map = df_mails.groupby('Cleaned Customer ID')['E-Mail Address'].apply(email_aggregator).reset_index()
        df_email_map.rename(columns={'E-Mail Address': 'To_Mail_IDs'}, inplace=True)
        # Step 2c: Standardize the customer identifier in the invoices DataFrame.
        print("Cleaning and standardizing 'Customer Code' column in invoice data...")
        df_invoices.dropna(subset=['Customer Code'], inplace=True)
        df_invoices['Cleaned Customer ID'] = pd.to_numeric(df_invoices['Customer Code'].astype(str), errors='coerce')
        df_invoices.dropna(subset=['Cleaned Customer ID'], inplace=True)
        df_invoices['Cleaned Customer ID'] = df_invoices['Cleaned Customer ID'].astype('Int64').astype(str)
        # --- 3. MERGING DATA ---
        print("Merging invoice data with aggregated emails...")
        df_output = pd.merge(
            df_invoices,
            df_email_map,
            on='Cleaned Customer ID',
            how='left'
        )
 
        # --- 4. FINALIZING THE OUTPUT DATAFRAME ---
 
        # Step 4a: UPDATED LOGIC - Populate Status, Remarks, and Processed Date based on mail lookup
        print("Populating status columns based on email lookup results...")
 
        # Initialize the new columns with empty values.
        # Using np.nan is the standard pandas way to represent missing data.
        df_output['Status'] = np.nan
        df_output['Remarks'] = np.nan
        df_output['Processed Date'] = pd.NaT # NaT for Not a Time
 
        # Create a boolean mask for rows where the email was not found.
        email_not_found_mask = df_output['To_Mail_IDs'].isnull()
 
        # For records where mail was NOT found (Failed), populate the fields.
        df_output.loc[email_not_found_mask, 'Status'] = 'Failed'
        df_output.loc[email_not_found_mask, 'Remarks'] = 'Mail not Found'
        df_output.loc[email_not_found_mask, 'Processed Date'] = pd.Timestamp.now()
 
        # For successful records, 'Status' and 'Remarks' remain empty (np.nan), 
        # and 'Processed Date' remains empty (NaT), as requested for downstream processing.
        # Step 4b: Define the final column order, including the new columns.
        final_columns = [
            'New Invoice Number',
            'Date',
            'Bill Amount',
            'Eway Bill No',
            'Purchase Order No',
            'Billing Document Number',
            'Plant',
            'Customer Code',
            'Customer Name',
            'To_Mail_IDs',
            'Status',
            'Remarks',
            'Processed Date'
        ]
        # Ensure all required columns exist to prevent KeyErrors
        for col in final_columns:
            if col not in df_output.columns:
                df_output[col] = np.nan
        df_final_output = df_output[final_columns]
 
        print("Processing complete. Final data prepared.")
 
        # --- 5. SAVING THE OUTPUT ---
        print(f"Saving output to: {output_file_path}")
        # Format the date column for output, turning NaT into empty cells in Excel
        df_final_output['Processed Date'] = pd.to_datetime(df_final_output['Processed Date']).dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')
        df_final_output.to_excel(output_file_path, index=False)
        print("\n--- SCRIPT FINISHED SUCCESSFULLY ---")
        print(f"Output file '{output_file_path}' has been created.")
    except FileNotFoundError as e:
        print(f"Error: Input file not found. Please check the path. Details: {e}")
    except KeyError as e:
        print(f"Error: A required column was not found. Please check your Excel files. Missing column: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
 
# --- EXECUTION ---

def main():
    parser = argparse.ArgumentParser(description="Merge invoice and emails")
    parser.add_argument("--invoice_excel",required=False,help="Excel Path for Invoice Details")
    parser.add_argument("--email_excel",required=False,help="Excel Path for Email Details")
    parser.add_argument("--output_excel",required=False,help="Excel Path for Output files")


    args = parser.parse_args()
 
    process_invoices_and_emails(
        invoice_file_path=args.invoice_excel,
        customer_mail_file_path=args.email_excel,
        output_file_path=args.output_excel
    )


if __name__ == "__main__":
    main()
