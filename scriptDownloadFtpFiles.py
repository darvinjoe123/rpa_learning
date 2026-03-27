import argparse
import logging
import os
import sys

import pandas as pd
import paramiko
import mysql.connector

# --- Configure Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)


def get_sftp_connection(hostname, port, username, password):
    """
    Establishes and returns an SFTP connection object.

    Args:
        hostname (str): The SFTP server hostname or IP address.
        port (int): The SFTP server port.
        username (str): The username for SFTP authentication.
        password (str): The password for SFTP authentication.

    Returns:
        paramiko.SFTPClient: An active SFTP client object or None if connection fails.
    """
    try:
        transport = paramiko.Transport((hostname, port))
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        logging.info(f"Successfully connected to SFTP server: {hostname}")
        return sftp
    except Exception as e:
        logging.error(
            f"Failed to establish SFTP connection to {hostname}: {e}")
        return None


def get_remote_file_list(sftp, remote_base_path):
    """
    Retrieves a list of all files from the specified remote SFTP directory.

    Args:
        sftp (paramiko.SFTPClient): An active SFTP client object.
        remote_base_path (str): The base directory on the SFTP server to list files from.

    Returns:
        list: A list of full file paths on the SFTP server.
    """
    try:
        all_files = []
        # The walktree method recursively lists all files and directories.
        for filename in sftp.listdir(remote_base_path):
            # for filename in filenames:
            all_files.append(os.path.join(
                remote_base_path, filename).replace("\\", "/"))
        logging.info(
            f"Successfully retrieved {len(all_files)} file paths from '{remote_base_path}'.")
        return all_files
    except Exception as e:
        logging.error(
            f"Failed to list files from SFTP path '{remote_base_path}': {e}")
        return []


def download_invoices_and_update_status(sftp, sftp_file_list, invoice_excel_path,
                                        local_download_path, db_params=None):
    """
    Reads an Excel file for a list of invoices, downloads them from SFTP,
    and updates their status in a database.

    Args:
        sftp (paramiko.SFTPClient): An active SFTP client object.
        sftp_file_list (list): A list of all file paths on the SFTP server.
        invoice_excel_path (str): The local file path to the Excel file containing invoice numbers.
        local_download_path (str): The local directory path to save downloaded PDFs.
        db_params (dict): A dictionary containing database connection parameters.
    """
    try:
        # Assuming the invoice numbers are in a column named 'Invoice'
        df = pd.read_excel(invoice_excel_path, dtype={'Billing Document Number': str})
        invoice_list = df['Billing Document Number'].str.strip().tolist()
    except Exception as e:
        logging.error(
            f"Failed to read or process the Excel file at '{invoice_excel_path}': {e}")
        return

    sftp_file_map = {os.path.basename(f): f for f in sftp_file_list}

    for invoice_number in invoice_list:
        str_invoice = str(invoice_number)
        found_file_path = None

        # Efficiently find the matching file
        for filename, full_path in sftp_file_map.items():
            if str_invoice in filename:
                found_file_path = full_path
                break

        if found_file_path:
            local_file_path = os.path.join(
                local_download_path, f"{str_invoice}.pdf")
            try:
                sftp.get(found_file_path, local_file_path)
                logging.info(
                    f"Successfully downloaded '{found_file_path}' to '{local_file_path}'.")
                update_database_status(
                    str_invoice, "Success", db_params)
            except Exception as e:
                logging.error(
                    f"Failed to download invoice '{str_invoice}' from '{found_file_path}': {e}")
                update_database_status(
                    str_invoice, "Download Failed", db_params)
        else:
            logging.warning(
                f"Invoice '{str_invoice}' not found on the SFTP server.")
            update_database_status(str_invoice, "File Not Found", db_params)


def update_database_status(invoice_number, status, db_params):
    """
    Updates the download status of an invoice in a MySQL database.

    Args:
        invoice_number (str): The invoice number to update.
        status (str): The new status (e.g., 'Downloaded', 'Download Failed', 'Not Found').
        db_params (dict): Database connection parameters.
    Example db_params:
        {
            "host": "localhost",
            "user": "your_username",
            "password": "your_password",
            "database": "your_db"
        }
    """
    cursor=False
    conn = False
    try:
        conn = mysql.connector.connect(**db_params)
        cursor = conn.cursor()
        update_query = """
        UPDATE agi.invoice_automation 
        SET sftp_file_status = %s
        WHERE billing_document_number = %s;
        """
        cursor.execute(update_query, (status,invoice_number))
        conn.commit()
        logging.info(
            f"Updated status for invoice '{invoice_number}' to '{status}'.")
    except mysql.connector.Error as e:
        logging.error(
            f"Failed to update database for invoice '{invoice_number}': {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def main():
    """
    Main function to parse arguments and orchestrate the invoice download process.
    """
    parser = argparse.ArgumentParser(description="Download invoices from an SFTP server.")
    parser.add_argument("--sftp_host", required=True, help="SFTP server hostname.")
    parser.add_argument("--sftp_port", type=int, default=22, help="SFTP server port.")
    parser.add_argument("--sftp_user", required=True, help="SFTP username.")
    parser.add_argument("--sftp_pass", required=True, help="SFTP password.")
    parser.add_argument("--sftp_path", required=True, help="Remote directory of invoices.")
    parser.add_argument("--excel_path", required=True, help="Path to the invoice list Excel file.")
    parser.add_argument("--download_path", required=True, help="Local directory to save PDFs.")
    parser.add_argument("--db_host", required=True, help="Database host.")
    parser.add_argument("--db_port", required=True, help="Database port.")
    parser.add_argument("--db_name", required=True, help="Database name.")
    parser.add_argument("--db_user", required=True, help="Database user.")
    parser.add_argument("--db_pass", required=True, help="Database password.")

    args = parser.parse_args()

    db_params = {
        "host": args.db_host,
        "port":  args.db_port,
        "database":  args.db_name,
        "user":  args.db_user,
        "password":  args.db_pass
    }
    # db_params = {
    #     "host": '127.0.0.1',
    #     "port":  '3306',
    #     "database":  'agi',
    #     "user":  'root',
    #     "password":  'root'
    # }

    sftp_client = get_sftp_connection(args.sftp_host,args.sftp_port, args.sftp_user, args.sftp_pass)
    # sftp_client = get_sftp_connection(
    #     '10.90.128.100', 22, 'procodebot', 'Procode@123')

    if sftp_client:
        all_sftp_files = get_remote_file_list(sftp_client,args.sftp_path)
        # all_sftp_files = get_remote_file_list(
        #     sftp_client, '/usr/share/DSCSIGNER/Outbound/')

        if all_sftp_files:
            download_invoices_and_update_status(sftp_client, all_sftp_files,args.excel_path, args.download_path,db_params)
            # download_invoices_and_update_status(sftp_client, all_sftp_files, 'D:\RPA_Process\AGI SEND EMAIL\TestingPythonScriptFTP\SAPExport.xlsx',
            #                                     'D:\RPA_Process\AGI SEND EMAIL\TestingPythonScriptFTP', db_params)
        sftp_client.close()
        logging.info("SFTP connection closed.")


if __name__ == "__main__":
    main()
