import logging

from datetime import datetime
from dateutil.relativedelta import relativedelta
from faker import Faker

import sfdc

fake = Faker()

def setup_test_records():
    aid = create_account()

    if not aid:
        logging.warning('Account cound not be created. Halting.')
        return

    cid = create_contact(aid)

    if not cid:
        logging.warning('Contact cound not be created. Halting.')
        rollback(aid)
        return

    oid = create_opportunity(aid, cid)

    if not oid:
        logging.warning('Opportunity could not be created. Halting.')
        rollback(aid, cid)
        return

    logging.info('Done!')


def rollback(account_id: str, contact_id: str=''):
    logging.info('Rolling back inserted records...')

    if contact_id:
        sfdc.sfdc_client.inner_client.Contact.delete(contact_id)

    sfdc.sfdc_client.inner_client.Account.delete(account_id)

def create_opportunity(account_id: str, contact_id: str):
    oid = ''
    close_date_str = datetime.now().isoformat().split('T')[0]
    contract_end_date_str = (datetime.now() + relativedelta(years=1, days=-1)).isoformat().split('T')[0]

    opp_payload = {
        'Name': f'Test Opp {datetime.now().isoformat()}',
        'RecordTypeId': '0123g000000PP5Q',
        'AccountId': account_id,
        'Type': 'New Subscription',
        'StageName': '3. Preparing for Trial Check-In',
        'CloseDate': close_date_str,
        'Decision_Maker__c': contact_id,
        'Contract_Start_Date__c': close_date_str,
        'Contract_End_Date__c': contract_end_date_str
    }

    logging.info('Creating Opportunity...')
    result = sfdc.sfdc_client.inner_client.Opportunity.create(opp_payload)

    if result['success']:
        oid = result['id']
        logging.info(f"Opportunity Id: {oid}")
    else:
        for err in result['errors']:
            logging.error(err)

    return oid

def create_contact(account_id: str):
    cid = ''
    first_name = fake.first_name()
    last_name = fake.last_name()
    email = fake.ascii_company_email()

    contact_payload = {
        'AccountId': account_id,
        'FirstName': first_name,
        'LastName': last_name,
        'Email': email,
        'Type__c': 'Other',
        'LeadSource': 'Other',
        'Status_of_User__c': 'Inactive'
    }

    logging.info('Creating Contact...')
    result = sfdc.sfdc_client.inner_client.Contact.create(contact_payload)

    if result['success']:
        cid = result['id']
        logging.info(f"Contact Id: {cid}")
    else:
        for err in result['errors']:
            logging.error(err)

    return cid


def create_account():
    aid = ''
    company_name = fake.company()

    account_payload = {
        'Name': f"OPRD - {company_name}",
        'Type': 'AlphaSense Internal'
    }

    logging.info('Creating Account...')
    result = sfdc.sfdc_client.inner_client.Account.create(account_payload)


    if result['success']:
        aid = result['id']
        logging.info(f"Account Id: {aid}")
    else:
        for err in result['errors']:
            logging.error(err)



    return aid