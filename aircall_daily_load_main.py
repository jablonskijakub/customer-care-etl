from AircallAPIImportSettings import AircallAPIImportSettings 
import logging
import sys
import argparse
import os

def main(execution_date):

    aircall_calls_import = AircallAPIImportSettings(name='aircall_calls', import_data_template = './config/aircall_template.json', s3_bucket = 'customer-care-import', target_schema = 'public', execution_date = execution_date)

    all_ok = True
    
    aircall_calls_import.run(execution_date)

    if all_ok:
        logging.info('great')
        sys.exit(os.EX_OK)
    
    sys.exit(os.EX_SOFTWARE)

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s',
                        datefmt='[%Y-%m-%d %H:%M:%S]', level=logging.INFO)
    parser = argparse.ArgumentParser(description='Aircall ETL')
    parser.add_argument('--execution-date', default=None)

    args = parser.parse_args()
    main(args.execution_date)
