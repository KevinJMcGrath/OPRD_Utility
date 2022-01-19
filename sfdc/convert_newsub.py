import csv
import logging

import sfdc

from dateutil import parser
from dateutil.relativedelta import relativedelta
from pathlib import Path

def fix_zero_asv():
    filename = 'Q122-ForConversion-NewSubscriptions.csv'
    csv_path = Path(r"C:\Users\Kevin\Downloads\AlphaSense") / filename
    for_update = []

    with open(csv_path, 'r') as csv_file:
        reader = csv.DictReader(csv_file)

        for row in reader:
            o_id = row['Opportunity CaseSafe ID']
            asv = float(row['ASV'])
            wsi_type = row['WSI License Type']

            p = {
                'Id': o_id,
                'ASV__c': asv,
                'Amount': asv
            }

            if wsi_type == '0':
                for_update.append(p)

    i = 0
    while True:
        start_i = i
        end_i = i + 49

        if start_i > len(for_update):
            break

        logging.info(f'Sending rows {start_i} to {end_i}')
        sfdc.sfdc_client.inner_client.bulk.Opportunity.update(for_update[start_i:end_i])

        i += 50


def convert_opps():
    filename = 'NewSub-Upsell-Batch2-ForConversion.csv'
    csv_path = Path(r"C:\Users\Kevin\Downloads\AlphaSense") / filename

    for_update = []
    products_for_insert = []
    with open(csv_path, 'r') as csv_file:
        reader = csv.DictReader(csv_file)

        for row in reader:
            if 'Exclude' in row and row['Exclude'] == 'TRUE':
                continue

            otype = row['Type']

            if otype != 'Up Sell':
                continue

            new_type = 'Upsell'

            o_id = row['Opportunity CaseSafe ID']
            close_date_str = row['Close Date']
            close_date = parser.parse(close_date_str)
            eff_date_str = row['Effective Date'] if row['Effective Date'] else close_date_str
            eff_date = parser.parse(eff_date_str)

            mons = int(float(row['Contract Length (Months)']))

            c_start_date = eff_date
            c_end_date = eff_date + relativedelta(months=mons, days=-1)

            user_count = int(row['# Unique Users'])

            if user_count == 0:
                user_count = 1

            asv_per_user = float(row['ASV']) / user_count

            price_book_id = '01s3g000000IeHr'

            wsi_type = row['WSI License Type']
            wsi_product_id = 'skip'

            if wsi_type == 'Full WSI':
                wsi_product_id = '01t3g000000d3C0'  # Full WSI
            elif wsi_type == 'Base + Content Pool':
                wsi_product_id = '01t3g000000d3C5'  # Base + Content Pool
            elif wsi_type == 'Token':
                wsi_product_id = '01t3g000000d3CA' # Token

            p = {
                'Id': o_id,
                'RecordTypeId': '0123g000000PP5Q',
                'Type': new_type,
                'Contract_Start_Date__c': c_start_date.date().isoformat(),
                'Contract_End_Date__c': c_end_date.date().isoformat(),
                'PriceBook2Id': price_book_id
            }

            for_update.append(p)

            if wsi_product_id != 'skip':
                oli = {
                    'OpportunityId': o_id,
                    'ASV__c': asv_per_user,
                    'Quantity': user_count,
                    'Product2Id': wsi_product_id
                }

                products_for_insert.append(oli)

    logging.info('Updating Opps...')
    i = 0
    while True:
        start_i = i
        end_i = i + 49

        if start_i > len(for_update):
            break

        logging.info(f"Sending rows {start_i} to {end_i}...")

        sfdc.sfdc_client.inner_client.bulk.Opportunity.update(for_update[start_i:end_i])

        i += 50

    # logging.info('Inserting Line Items...')
    # i = 0
    # while True:
    #     start_i = i
    #     end_i = i + 49
    #
    #     if start_i > len(for_update):
    #         break
    #
    #     logging.info(f"Sending rows {start_i} to {end_i}...")
    #
    #     sfdc.sfdc_client.inner_client.bulk.OpportunityLineItem.insert(products_for_insert[start_i:end_i])
    #
    #     i += 50

    logging.info('Done!')