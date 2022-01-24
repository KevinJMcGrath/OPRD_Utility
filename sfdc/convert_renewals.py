import csv
import logging

import sfdc

from datetime import datetime, date
from dateutil import parser
from dateutil.relativedelta import relativedelta
from pathlib import Path

def delete_upsell_subs():

    soql = "SELECT Id, Subscription__c FROM Opportunity WHERE Type = 'Upsell' AND Migration_Id__c = 'b1-upsell'"

    #sub_ids = [f"'{res['Subscription__c']}'" for res in sfdc.sfdc_client.query(soql)]

    #sub_id_str = ','.join(sub_ids)
    results = sfdc.sfdc_client.query(soql)
    sub_ids = [res['Subscription__c'] for res in results]

    sfdc.sfdc_client.inner_client.bulk.Subscription__c.delete(sub_ids)


def set_migration_id():
    filename = 'Q122-ForConversion-Upsell-forreal.csv'
    csv_path = Path(r"C:\Users\Kevin\Downloads\AlphaSense") / filename

    for_update = []

    with open(csv_path, 'r') as csv_file:
        reader = csv.DictReader(csv_file)

        for row in reader:
            o_id = row['Opportunity CaseSafe ID']
            p = {
                'Id': o_id,
                'Migration_Id__c': 'b1-upsell'
            }

            for_update.append(p)

    idx = 0
    while True:

        start_i = idx
        end_i = idx + 199

        if start_i > len(for_update):
            break

        logging.info(f'Updating rows {start_i} to {end_i}')

        sfdc.sfdc_client.inner_client.bulk.Opportunity.update(for_update[start_i:end_i])

        idx += 200

    logging.info('Done!')

def fix_products():
    soql = "SELECT Id, Renewed_Product__c FROM OpportunityLineItem WHERE"
    soql += " CreatedDate = TODAY AND Opportunity.Type = 'Renewal' AND Opportunity.RecordTypeId = '0123g000000PP5G' "
    soql += " AND LastModifiedBy.Name = 'Kevin McGrath' AND Renewed_Product__c = FALSE"

    prods = sfdc.sfdc_client.query(soql)

    for_update = []
    for prod in prods:
        p = {
            'Id': prod['Id'],
            'Renewed_Product__c': True
        }

        for_update.append(p)

    i = 0
    while True:
        start_i = i
        end_i = i + 49

        if start_i > len(for_update):
            break

        logging.info(f"Sending rows {start_i} to {end_i}...")

        sfdc.sfdc_client.inner_client.bulk.OpportunityLineItem.update(for_update[start_i:end_i])

        i += 50

    logging.info('Done!')

def create_sub_existing_renewals():
    soql = "SELECT Id, AccountId, Subscription__c FROM Opportunity WHERE"
    soql += " Type = 'Renewal' AND LastModifiedDate = TODAY AND IsClosed = FALSE "
    soql += " AND Subscription__c = NULL AND RecordTypeId = '0123g000000PP5G'"

    opps = sfdc.sfdc_client.query(soql)

    for_insert = []
    for o in opps:
        p = {
            'Account__c': o['AccountId'],
            'Status__c': 'Active Client',
            'Description__c': f"oid: {o['Id']}"
        }

        for_insert.append(p)

    logging.info("Inserting Subscriptions...")
    results = sfdc.sfdc_client.inner_client.bulk.Subscription__c.insert(for_insert)

def set_sub_renewals():
    soql = "SELECT Id, AccountId, Subscription__c FROM Opportunity WHERE"
    soql += " Type = 'Renewal' AND LastModifiedDate = TODAY AND IsClosed = FALSE "
    soql += " AND Subscription__c = NULL AND RecordTypeId = '0123g000000PP5G'"

    opps = sfdc.sfdc_client.query(soql)

    soql = "SELECT Id, Description__c FROM Subscription__c WHERE Description__c LIKE 'oid:%'"

    results = {s['Description__c'].split(':')[1].strip():  s['Id'] for s in sfdc.sfdc_client.query(soql)}

    for_update = []
    for o in opps:
        o_id = o['Id']

        if o_id not in results:
            logging.warning(f'Could not find {o_id} in Subscription Id list...')
            continue

        s_id = results[o_id]

        p = {
            'Id': o_id,
            'Subscription__c': s_id
        }

        for_update.append(p)

    logging.info('Updating Renewals...')
    sfdc.sfdc_client.inner_client.bulk.Opportunity.update(for_update)

    logging.info('Done!')




def get_existing_renewals():

    soql = 'SELECT Id, Id_18__c, IsClosed, StageName, Type, Contract_Length_Months__c FROM Opportunity WHERE '
    soql += " Type IN ('Renewal', 'Renewal (full term)', 'Renewal (contract/email required)', 'Pending Partial Cancel', 'Evergreen') "
    soql += " AND CloseDate = THIS_YEAR "
    soql += " AND IsClosed = TRUE"

    opps = sfdc.sfdc_client.inner_client.query_all(soql)['records']

    return opps

def get_pending_renewals():
    soql = 'SELECT Id, Id_18__c, StageName, Type, Contract_Length_Months__c FROM Opportunity WHERE '
    soql += " Type IN ('Renewal (full term)', 'Renewal (contract/email required)', 'Pending Partial Cancel', 'Evergreen') "
    soql += " AND IsClosed = false AND RecordTypeId != '0123g000000PP5G'"

    opps = sfdc.sfdc_client.inner_client.query_all(soql)['records']

    return { o['Id']: o for o in opps }

def convert_renewals_from_SFDC():
    migration_id = 'sfdc-01-20'
    price_book_id = '01s3g000000IeHr'
    record_type_id = '0123g000000PP5G'  # Renewal

    soql = "SELECT Id, Id_18__c, StageName, Type, Contract_Length_Months__c, Contract_Start_Date__c, "
    soql += " Contract_End_Date__c, ASV__c, Effective_Date__c, AccountId, Subscription__c, CloseDate, "
    soql += " Primary_Contact__c "
    soql += " FROM Opportunity "
    soql += " WHERE IsClosed = false AND "
    soql += " Type IN ('Renewal (full term)', 'Renewal (contract/email required)', 'Pending Partial Cancel')"

    opps = sfdc.sfdc_client.query(soql)

    logging.info(f"Opportunities pending migration: {len(opps)}")

    contract_rqrd_opps = [o['Id'] for o in opps if o['Type'] == 'Renewal (contract/email required)']

    opps_for_update = []
    subs_for_insert = []
    for o in opps:
        o_id = o['Id']
        a_id = o['AccountId']
        stage = o['StageName']
        o_type = o['Type']
        close_date = o['CloseDate']
        term = int(float(o['Contract_Length_Months__c'])) if o['Contract_Length_Months__c'] else 12
        eff_date_str = o['Effective_Date__c'] if o['Effective_Date__c'] else o['CloseDate']

        eff_date = parser.parse(eff_date_str)
        contract_start_date = eff_date
        contract_end_date = contract_start_date + relativedelta(months=term, days=-1)

        prim_contact_id = o['Primary_Contact__c']

        p = {
            'Id': o_id,
            'RecordTypeId': record_type_id,
            'Type': 'Renewal',
            'StageName': 'Open',
            'Contract_Start_Date__c': contract_start_date.date().isoformat(),
            'Contract_End_Date__c': contract_end_date.date().isoformat(),
            'PriceBook2Id': price_book_id,
            'Migration_Id__c': migration_id,
            'Decision_Maker__c': prim_contact_id
        }

        opps_for_update.append(p)


        sub = {
            'Account__c': a_id,
            'Status__c': 'Active Client',
            'Description__c': o_id,
            'Migration_Id__c': migration_id,
            'Renewal_Conditions__c': 'Auto-Renewal',
            'Decision_Maker__c': prim_contact_id
        }

        if o['Id'] in contract_rqrd_opps:
            sub['Renewal_Conditions__c'] = 'Explicit Confirmation Required'

        subs_for_insert.append(sub)

    results = sfdc.sfdc_client.inner_client.bulk.Subscription__c.insert(subs_for_insert)
    success_ids, err_list = process_sfdc_errors(results)

    soql = f"SELECT Id, Description__c FROM Subscription__c WHERE Migration_Id__c = '{migration_id}'"
    subs = sfdc.sfdc_client.query(soql)

    for sub in subs:
        s_id = sub['Id']
        s_o_id = sub['Description__c']

        for o in opps_for_update:
            if o['Id'] == s_o_id:
                o['Subscription__c'] = s_id

    results = sfdc.sfdc_client.inner_client.bulk.Opportunity.update(opps_for_update)
    success_ids, err_list = process_sfdc_errors(results)

    link_contacts_to_sub(migration_id, is_renewal=True)

def convert_renewals(status_set: list, migration_id: str, skip_products: bool):
    logging.info('Starting Migration for statuses: ' + ', '.join(status_set))
    logging.info('Loading Opps pending migration...')
    pending_renewals = get_pending_renewals()

    contract_rqrd_opps = [o['Id'] for o in pending_renewals.values() if o['Type'] == 'Renewal (contract/email required)']

    prepare_renewals(status_set, migration_id, skip_products, pending_renewals)
    insert_subscriptions(migration_id, contract_rqrd_opps)
    link_opps_to_sub(migration_id)
    link_contacts_to_sub(migration_id, True)

    logging.info('Done!')

def prepare_renewals(status_set: list, migration_id: str, skip_products: bool, pending_renewals: dict):
    price_book_id = '01s3g000000IeHr'
    record_type_id = '0123g000000PP5G' # Renewal

    filename = 'In Flight Renewal Opp_1.18.csv'
    csv_path = Path(r"C:\Users\Kevin\Downloads\AlphaSense") / filename
    opps_for_update = []
    products_for_insert = []

    with open(csv_path, 'r') as csv_file:
        reader = csv.DictReader(csv_file)

        for row in reader:
            skip = row['Skip']
            if skip:
                continue

            status = row['Migration Status']
            if status not in status_set:
                continue

            o_id = row['Opportunity CaseSafe ID']
            if not o_id:
                continue

            p_renewal = pending_renewals[o_id]
            if not p_renewal:
                logging.error(f'Renewal record for Id {o_id} was not loaded from Salesforce. Skipping.')

            effective_date_str = row['Effective Date 2']
            term_str = p_renewal['Contract_Length_Months__c']

            eff_date = parser.parse(effective_date_str)
            term = int(float(term_str))

            contract_start_date = eff_date
            contract_end_date = contract_start_date + relativedelta(months=term, days=-1)

            p = {
                'Id': o_id,
                'RecordTypeId': record_type_id,
                'Type': 'Renewal',
                'StageName': 'Open',
                'Contract_Start_Date__c': contract_start_date.date().isoformat(),
                'Contract_End_Date__c': contract_end_date.date().isoformat(),
                'PriceBook2Id': price_book_id,
                'Migration_Id__c': migration_id
            }

            opps_for_update.append(p)

            products = get_products(row)

            for prod in products:
                products_for_insert.append(prod)

    logging.info(f'Updating Opps ({len(opps_for_update)})...')
    i = 0
    while True:
        start_i = i
        end_i = i + 49

        if start_i > len(opps_for_update):
            break

        if len(opps_for_update) < end_i:
            end_i = len(opps_for_update) - 1

        logging.info(f"Sending rows {start_i} to {end_i}...")

        sfdc.sfdc_client.inner_client.bulk.Opportunity.update(opps_for_update[start_i:end_i])

        i += 50


    if not skip_products:
        i = 0
        logging.info('Inserting Line Items...')
        while True:
            start_i = i
            end_i = i + 49

            if start_i > len(products_for_insert):
                break

            if len(products_for_insert) < end_i:
                end_i = len(products_for_insert) - 1


            logging.info(f'Sending Line Items {start_i} to {end_i}...')

            sfdc.sfdc_client.inner_client.bulk.OpportunityLineItem.insert(products_for_insert[start_i:end_i])

            i += 50


def insert_subscriptions(migration_id: str, contract_required_opps: list):
    subs_for_insert = []

    soql = 'SELECT Id, AccountId, Subscription__c, Primary_Contact__c FROM Opportunity WHERE'
    soql += f" Migration_Id__c = '{migration_id}' AND Subscription__c = NULL"

    opps = sfdc.sfdc_client.query(soql)

    for o in opps:
        p = {
            'Account__c': o['AccountId'],
            'Status__c': 'Active Client',
            'Description__c': f"{o['Id']}",
            'Migration_Id__c': migration_id
        }

        if o['Id'] in contract_required_opps:
            p['Renewal_Conditions__c'] = 'Explicit Confirmation Required'

        if o['Primary_Contact__c']:
            p['Decision_Maker__c'] = o['Primary_Contact__c']

        subs_for_insert.append(p)

    logging.info(f"Inserting Subscriptions ({len(subs_for_insert)})...")
    sfdc.sfdc_client.inner_client.bulk.Subscription__c.insert(subs_for_insert)


def link_opps_to_sub(migration_id: str):
    logging.info('Linking Opps to new Subscriptions...')

    logging.info('Loading Subscriptions...')
    soql = f"SELECT Id, Description__c FROM Subscription__c WHERE Migration_Id__c = '{migration_id}'"
    subs = {s['Description__c']:  s['Id'] for s in sfdc.sfdc_client.query(soql)}

    logging.info('Loading Opportunities...')
    soql = f"SELECT Id FROM Opportunity WHERE Migration_Id__c = '{migration_id}' and Subscription__c = NULL"
    opps = sfdc.sfdc_client.query(soql)

    opps_for_update = []
    for o in opps:
        o_id = o['Id']

        if o_id not in subs:
            logging.warning(f"Opp Id {o_id} not found in Sub list...")
            continue

        sub_id = subs[o_id]
        p = {
            'Id': o['Id'],
            'Subscription__c': sub_id
        }

        opps_for_update.append(p)

    logging.info(f'Updating Opps (again) ({len(opps_for_update)})...')
    i = 0
    while True:
        start_i = i
        end_i = i + 49

        if start_i > len(opps_for_update):
            break

        if len(opps_for_update) < end_i:
            end_i = len(opps_for_update) - 1

        logging.info(f"Sending rows {start_i} to {end_i}...")

        sfdc.sfdc_client.inner_client.bulk.Opportunity.update(opps_for_update[start_i:end_i])

        i += 50


def link_contacts_to_sub(migration_id: str, is_renewal: bool=False):
    logging.info('Loading Contacts for Sub Update...')
    field_name = 'Renewal_Opportunity__r' if is_renewal else 'Opportunity__r'
    soql = f"SELECT Id, Renewal_Opportunity__r.Subscription__c, Opportunity__r.Subscription__c " \
           f"FROM Contact WHERE Subscription__c = NULL AND {field_name}.Migration_Id__c = '{migration_id}'"
    contacts = sfdc.sfdc_client.query(soql)

    contacts_for_update = []
    for c in contacts:
        p = {
            'Id': c['Id'],
            'Subscription__c': c[field_name]['Subscription__c']
        }

        contacts_for_update.append(p)

    logging.info(f"Updating Contacts ({len(contacts_for_update)})...")
    sfdc.sfdc_client.inner_client.bulk.Contact.update(contacts_for_update)


def get_products(row):
    o_id = row['Opportunity CaseSafe ID']
    asv_per_user = float(row['Avg ASV']) if row['Avg ASV'] != '#DIV/0!' else float(row['ASV'])
    user_count = int(row['# Unique Users'])

    if user_count == 0:
        return []

    wsi_full_license_count = int(row['Sum of WSI: Full Licene Type'])
    wsi_base_pool_count = int(row['Sum of WSI: Base + Content Pool'])
    wsi_token_count = int(row['Sum of WSI: Tokens'])
    refinitiv_count = int(row['Sum of Refiniv Real Time Research'])
    factset_count = int(row['Sum of Factset Real Time Research'])
    prm_count = int(row['Sum of Primary Research'])
    internal_insights_count = int(row['Sum of Internal Insights'])

    # Wall Street Insights: Base + Content Pool - WSI-P - 01t3g000000d3C5
    # Wall Street Insights: Full License - WSI-I - 01t3g000000d3C0
    # Wall Street Insights: Tokens - WSI-T - 01t3g000000d3CA

    # Real Time Research - Factset - BRM-F - 01t3g000000d3CK
    # Real Time Research - Refinitiv - BRM-R - 01t3g000000d3CF

    # Primary Research - PRM - 01t3g000000d3CP

    # Internal Insights - AII - 01t3g000000d3CU



    prod_list = []

    if wsi_full_license_count > 0:
        p = {
            'OpportunityId': o_id,
            'ASV__c': asv_per_user,
            'Quantity': wsi_full_license_count,
            'Product2Id': '01t3g000000d3C0',
            'Renewed_Product__c': True
        }
        prod_list.append(p)

    if wsi_base_pool_count > 0:
        p = {
            'OpportunityId': o_id,
            'ASV__c': asv_per_user,
            'Quantity': wsi_base_pool_count,
            'Product2Id': '01t3g000000d3C5',
            'Renewed_Product__c': True
        }
        prod_list.append(p)

    if wsi_token_count > 0:
        p = {
            'OpportunityId': o_id,
            'ASV__c': asv_per_user,
            'Quantity': wsi_token_count,
            'Product2Id': '01t3g000000d3CA',
            'Renewed_Product__c': True
        }
        prod_list.append(p)

    if refinitiv_count > 0:
        p = {
            'OpportunityId': o_id,
            'ASV__c': asv_per_user,
            'Quantity': refinitiv_count,
            'Product2Id': '01t3g000000d3CF',
            'Renewed_Product__c': True
        }
        prod_list.append(p)

    if factset_count > 0:
        p = {
            'OpportunityId': o_id,
            'ASV__c': asv_per_user,
            'Quantity': factset_count,
            'Product2Id': '01t3g000000d3CK',
            'Renewed_Product__c': True
        }
        prod_list.append(p)

    if prm_count > 0:
        p = {
            'OpportunityId': o_id,
            'ASV__c': asv_per_user,
            'Quantity': prm_count,
            'Product2Id': '01t3g000000d3CP',
            'Renewed_Product__c': True
        }
        prod_list.append(p)

    if internal_insights_count > 0:
        p = {
            'OpportunityId': o_id,
            'ASV__c': asv_per_user,
            'Quantity': internal_insights_count,
            'Product2Id': '01t3g000000d3CU',
            'Renewed_Product__c': True
        }
        prod_list.append(p)

    return prod_list

def process_sfdc_errors(sfdc_results):
    filename = f"migration_error_log_{date.today().isoformat()}.txt"
    err_path = Path(r"C:\Users\Kevin\Downloads\AlphaSense") / filename

    success_ids = []
    error_list = []

    index = 0
    for r in sfdc_results:
        if r['success']:
            success_ids.append(r['id'])
        else:
            for err in r['errors']:
                err_str =f"{index} | Status Code: {err['statusCode']} | Message: {err['message']}"
                error_list.append(err_str)

        index += 1

    logging.info(f"Last Transaction Error Count: {len(error_list)}")

    if error_list:
        with open(err_path, 'a') as err_file:
            err_file.writelines(error_list)

    return success_ids, error_list