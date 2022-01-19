import logging

import sfdc

from dateutil import parser
from dateutil.relativedelta import relativedelta

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