import logging

import sfdc

def breakdown_test_objects(account_id: str, del_contacts: bool=True, del_acct: bool=True):

    # Assets
    logging.info('Deleting Assets...')
    soql = f"SELECT Id FROM Asset WHERE AccountId = '{account_id}'"
    sfdc.sfdc_client.inner_client.bulk.Asset.delete(create_payload_list(soql))

    # Subscription
    logging.info('Deleting Subscriptions...')
    soql = f"SELECT Id FROM Subscription__c WHERE Account__c = '{account_id}'"
    sfdc.sfdc_client.inner_client.bulk.Subscription__c.delete(create_payload_list(soql))

    # Opportunities
    logging.info('Deleting Opportunities...')
    soql = f"SELECT Id FROM Opportunity WHERE AccountId = '{account_id}'"
    sfdc.sfdc_client.inner_client.bulk.Opportunity.delete(create_payload_list(soql))

    if del_contacts:
        # Contacts
        logging.info('Deleting Contacts...')
        soql = f"SELECT Id FROM Contact WHERE AccountId = '{account_id}'"
        sfdc.sfdc_client.inner_client.bulk.Contact.delete(create_payload_list(soql))

    if del_acct and del_contacts:
        # Account
        logging.info('Deleting Account...')
        sfdc.sfdc_client.inner_client.Account.delete(account_id)

    logging.info('Done.')


def create_payload_list(soql):
    payload_list = []
    records = sfdc.sfdc_client.query(soql)

    for r in records:
        p = { 'Id': r['Id'] }
        payload_list.append(p)

    return payload_list


