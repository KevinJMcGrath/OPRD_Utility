import logging
import pprint

import sfdc

from datetime import date
from dateutil import parser
from dateutil.relativedelta import relativedelta
from pathlib import Path

def product_line_adjust():
    soql = "SELECT Id, (SELECT Id, Product_Family__c FROM OpportunityLineItems) "
    soql += "FROM Opportunity "
    soql += "WHERE RecordTypeId IN ('0123g000000PP5Q', '0123g000000PP5G') AND Opp_Product_Count__c > 0 "
    soql += "AND CALENDAR_YEAR(CloseDate) = 2021 AND Product_Line__c = null"

    opps = sfdc.sfdc_client.query(soql)

    for_update = []
    prod_line_map = {
        'AlphaSense + Stream': 0,
        'AlphaSense': 0,
        'Stream': 0,
        'Other': 0
    }
    for o in opps:
        has_as = False
        has_st = False
        prod_line = None

        for prod in o['OpportunityLineItems']['records']:
            fam = prod['Product_Family__c']
            if not fam:
                continue

            if fam.startswith('AlphaSense'):
                has_as = True
            elif fam.startswith('Stream'):
                has_st = True

        if has_as and has_st:
            prod_line = 'AlphaSense + Stream'
        elif has_as:
            prod_line = 'AlphaSense'
        elif has_st:
            prod_line = 'Stream'
        else:
            logging.info(f"Id {o['Id']} reported as 'Other' product line...")
            prod_line = 'Other'

        prod_line_map[prod_line] += 1

        payload = {
            'Id': o['Id'],
            'Product_Line__c': prod_line
        }
        for_update.append(payload)

    print('Product line stats: ')
    pprint.pprint(prod_line_map)
    logging.info(f"Updating {len(for_update)} Opportunites...")

    i = 0
    step = 100
    while True:
        start_i = i
        end_i = i + (step - 1)

        if start_i > len(for_update):
            break

        logging.info(f"Sending rows {start_i} to {end_i}...")

        results = sfdc.sfdc_client.inner_client.bulk.Opportunity.update(for_update[start_i:end_i])

        process_sfdc_errors(results)

        i += step

    logging.info('Done!')

def bizable():

    soql = "SELECT Id, bizible2__Bizible_Opportunity_Amount__c, ASV__c FROM Opportunity " \
           "WHERE bizible2__Bizible_Opportunity_Amount__c = NULL AND ASV__c != NULL AND" \
           " CALENDAR_YEAR(CreatedDate) >= 2021 "

    opps = sfdc.sfdc_client.query(soql)

    for_update = []
    for o in opps:
        p = {
            'Id': o['Id'],
            'bizible2__Bizible_Opportunity_Amount__c': float(o['ASV__c'])
        }

        for_update.append(p)

    error_list = []
    logging.info(f'Updating Opps {len(for_update)}...')
    i = 0
    step = 100
    while True:
        start_i = i
        end_i = i + (step - 1)

        if start_i > len(for_update):
            break

        logging.info(f"Sending rows {start_i} to {end_i}...")

        results = sfdc.sfdc_client.inner_client.bulk.Opportunity.update(for_update[start_i:end_i])

        process_sfdc_errors(results)

        i += step

    logging.info('Done!')

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

def match_empty_subs():

    soql = "SELECT Id, Description__c FROM Subscription__c WHERE Decision_Maker__c = NULL AND Migration_Id__c != null"

    subs = sfdc.sfdc_client.query(soql)

    soql = "SELECT Id, Subscription__c FROM Opportunity WHERE Type IN ('New Subscription', 'Upsell', 'Renewal')"

    opps = sfdc.sfdc_client.query(soql)

    opp_sub_map = {}
    for s in subs:
        opp_id = ''
        d = s['Description__c']
        s_id = s['Id']

        if 15 <= len(d) <= 18 and d.startswith('006'):
            renewal_id = d
        elif ':' in d:
            d_temp = d.split(':')[1].strip()

            if 15 <= len(d_temp) <= 18:
                renewal_id = d_temp

        if renewal_id:
            opp_sub_map[renewal_id] = s_id

def fix_subscriptions():
    soql = "SELECT Id, Description__c FROM Subscription__c WHERE Decision_Maker__c = null AND Description__c != null"

    subs = sfdc.sfdc_client.query(soql)

    opp_sub_map = {}
    for s in subs:
        renewal_id = ''
        d = s['Description__c']
        s_id = s['Id']

        if 15 <= len(d) <= 18 and d.startswith('006'):
            renewal_id = d
        elif ':' in d:
            d_temp = d.split(':')[1].strip()

            if 15 <= len(d_temp) <= 18:
                renewal_id = d_temp

        if renewal_id:
            opp_sub_map[renewal_id] = s_id

    logging.info(f"Opp Count: {len(opp_sub_map)}")



    soql = f"SELECT Id, Primary_Contact__c, Decision_Maker__c FROM Opportunity WHERE Type = 'Renewal' " \
           f"AND LastModifiedDate = THIS_MONTH AND Subscription__c = null"

    opps = sfdc.sfdc_client.query(soql)

    subs_for_update = []
    opps_for_update = []
    for o in opps:
        o_id = o['Id']

        if not o_id in opp_sub_map:
            logging.warning(f"Opportunity {o_id} not found in Sub Map")
            continue

        sub_id = opp_sub_map[o_id]

        p = o['Primary_Contact__c']
        d = o['Decision_Maker__c']

        p_o = {
            'Id': o_id,
            'Subscription__c': sub_id
        }

        opps_for_update.append(p_o)

        p_s = {
            'Id': sub_id
        }

        if d:
            p_s['Decision_Maker__c'] = d
        elif p:
            p_s['Decision_Maker__c'] = p
        else:
            logging.info('No Primary or DM - Skipping')

        subs_for_update.append(p_s)

    logging.info('Updating Subs...')

    sfdc.sfdc_client.inner_client.bulk.Subscription__c.update(subs_for_update)

    logging.info('Updating Opps...')
    i = 0
    step = 100
    while True:
        start_i = i
        end_i = i + (step - 1)

        if start_i > len(opps_for_update):
            break

        logging.info(f"Sending rows {start_i} to {end_i}...")

        sfdc.sfdc_client.inner_client.bulk.Opportunity.update(opps_for_update[start_i:end_i])

        i += step

    logging.info('Done!')



def update_decision_makers():
    soql = "SELECT Id, Subscription__c, Primary_Contact__c, Decision_Maker__c FROM Opportunity WHERE Subscription__c != null AND " \
           " Type = 'Renewal' AND Primary_Contact__c != NULL AND Subscription__r.Decision_Maker__c = NULL"

    opps = sfdc.sfdc_client.query(soql)

    for_update = []
    for o in opps:
        s = o['Subscription__c']
        p = o['Primary_Contact__c']
        d = o['Decision_Maker__c']

        pl = {
            'Id': s
        }

        if d:
            pl['Decision_Maker__c'] = d
        elif p:
            pl['Decision_Maker__c'] = p
        else:
            logging.info('No Primary or DM - Skipping')
            continue

        for_update.append(pl)

    sfdc.sfdc_client.inner_client.bulk.Subscription__c.update(for_update)



def update_quota_comms_credit():
    soql = f"SELECT Id, Quota_Credit__c FROM Opportunity WHERE Quota_Credit__c = 0 AND LastModifiedDate = THIS_MONTH"
    soql += " AND Type NOT IN ('Renewal', 'Evergreen', 'Renewal (contract/email required)', 'Renewal (full term)')"

    opps = sfdc.sfdc_client.query(soql)

    for_update = []
    for o in opps:
        p = {
            'Id': o['Id'],
            'Quota_Credit__c': -99
        }

        for_update.append(p)

    i = 0
    step = 100
    while True:
        start_i = i
        end_i = i + (step - 1)

        if start_i > len(for_update):
            break

        logging.info(f"Sending rows {start_i} to {end_i}...")

        sfdc.sfdc_client.inner_client.bulk.Opportunity.update(for_update[start_i:end_i])

        i += step

def set_decision_maker(migration_id: str):
    soql = f"SELECT Id, Primary_Contact__c, Subscription__c FROM Opportunity " \
           f"WHERE Type = 'Renewal' AND Migration_Id__c = '{migration_id}'"

    opps = sfdc.sfdc_client.query(soql)

    subs_for_update = []
    for o in opps:
        if not o['Primary_Contact__c']:
            continue

        p = {
            'Id': o['Subscription__c'],
            'Decision_Maker__c': o['Primary_Contact__c']
        }

        subs_for_update.append(p)

    sfdc.sfdc_client.inner_client.bulk.Subscription__c.update(subs_for_update)
    logging.info('Done!')

def copy_contacts_to_sub(opp_id: str, is_renewal: bool=False):
    field_name = 'Renewal_Opportunity__c' if is_renewal else 'Opportunity__c'

    soql = f"SELECT Id, Subscription__c FROM Opportunity WHERE Id = '{opp_id}'"
    opp = sfdc.sfdc_client.query(soql)[0]
    sub_id = opp['Subscription__c']

    logging.info('Querying for Contacts...')
    soql = f"SELECT Id FROM Contact WHERE {field_name} = '{opp_id}'"
    contacts = sfdc.sfdc_client.query(soql)

    for_update = []
    for c in contacts:
        p = {
            'Id': c['Id'],
            'Subscription__c': sub_id
        }

        for_update.append(p)

    logging.info(f'Updating Contacts ({len(for_update)})...')
    sfdc.sfdc_client.inner_client.bulk.Contact.update(for_update)