from pyspark.sql.functions import *
import time
import logging
from init_spark import create_spark_object
from validate import get_current_date
from load import load_files, display_df, df_count, drop_duplicate_cols
from mongo_connect import insert_document
from web_scraping import scraped_df
from pdf_extraction import pdf
logging.config.fileConfig('properties/configuration/logging.config')


def main():
    try:
        logging.info('I am the main method')
        logging.info('calling spark object')
        spark = create_spark_object()

        #logging.info('object create...', str(spark))
        logging.info('validating spark object')
        get_current_date(spark)

        logging.info('reading file....')
        df_csv = load_files(spark, file_dir='/home/utkarsh/Downloads/archive/vehicle-data.csv')
        df_tsv = load_files(spark, file_dir='/home/utkarsh/Downloads/archive/tollplaza-data.tsv')
        df_txt = load_files(spark, file_dir='/home/utkarsh/Downloads/archive/payment-data.txt')
        logging.info('validating the dataframes... ')
        df_count(df_csv, 'df_csv')
        df_count(df_tsv, 'df_tsv')
        df_count(df_txt, 'df_txt')

        df_csv = df_csv.drop('Rowid')
        df_tsv = df_tsv.drop('Rowid')

        df_csv = df_csv.join(df_tsv, df_csv.Timestamp == df_tsv.Timestamp, 'inner')
        df_clean1 = drop_duplicate_cols(df_csv, 'df_csv')
        display_df(df_clean1, 'df_clean1')

        df_txt = df_txt.join(df_clean1, df_txt.Tollplaza_id == df_tsv.Tollplaza_id, 'inner')
        df_clean2 = drop_duplicate_cols(df_txt, 'df_txt')
        display_df(df_clean2, 'df_clean2')

        insert_document(df_clean2, 'df_clean2')
        insert_document(scraped_df(),'scraped_df')
        #insert_document(extracted_df,'df_extract')
    except Exception as e:
        logging.error('An error occurred ===', str(e))
        sys.exit(1)


if __name__ == '__main__':
    main()
    logging.info('Application done')
