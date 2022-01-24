import csv
import logging

import sfdc

from datetime import date
from dateutil import parser
from dateutil.relativedelta import relativedelta
from functools import total_ordering
from pathlib import Path

renewal_types = ['Renewal', 'Renewal (full term)', 'Renewal (contract/email required)', 'Evergreen',
                     'Pending Partial Cancel']
new_biz_types = ['New Subscription', 'New Business', 'Upsell', 'Up Sell', 'Up Sell/New - Deferred']

@total_ordering
class Opp:
    def __init__(self, o):
        # opps
        # self.o = opp_record
        self.previous_opp = None
        self.related_opps = []

        # contacts
        self.primary_contact = o['Primary_Contact__r']
        self.decision_maker = o['Decision_Maker__r']
        self.new_biz_contacts = o['Contacts__r']['records'] if o['Contacts__r'] else None
        self.renewal_contacts = o['Contacts1__r']['records'] if o['Contacts1__r'] else None
        self.ocrs = o['OpportunityContactRoles']['records'] if o['OpportunityContactRoles'] else None

        # bool
        self.is_curr_renewal = False
        self.is_new_business = False
        self.is_closed = o['IsClosed']
        self.is_won = o['IsWon']

        # dates
        self.close_date = parser.parse(o['CloseDate'])
        self.created_date = parser.parse(o['CreatedDate'])
        self.eff_date = parser.parse(o['Effective_Date__c']) if o['Effective_Date__c'] else None
        self.c_start_date = parser.parse(o['Contract_Start_Date__c']) if o['Contract_Start_Date__c'] else None
        self.c_end_date = parser.parse(o['Contract_End_Date__c']) if o['Contract_End_Date__c'] else None

        # numbers
        self.contract_months = int(float(o['Contract_Length_Months__c']))

        # strings
        self.id = o['Id']
        self.name = o['Name']
        self.account_id = o['AccountId']
        self.sub_id = o['Subscription__c']
        self.opp_type = o['Type']
        self.type = ''

        self.process_opp(o)

    def __lt__(self, other):
        return self.created_date < other.created_date

    @property
    def start_date(self):
        if self.c_start_date:
            return self.c_start_date
        elif self.eff_date:
            return self.eff_date
        else:
            return self.close_date

    @property
    def end_date(self):
        c_mons = self.contract_months if self.contract_months else 12

        if self.c_end_date:
            return self.c_end_date
        elif self.eff_date:
            return self.eff_date + relativedelta(months=c_mons, days=-1)
        else:
            return self.close_date + relativedelta(months=c_mons, days=-1)

    def process_opp(self, o):
        if self.opp_type in renewal_types:
            self.type = 'renewal'
        elif self.opp_type in new_biz_types:
            self.type = 'new'
        else:
            self.type = 'other'

        self.is_curr_renewal = (o['RecordTypeId'] and o['RecordTypeId'].startswith('0123g000000PP5G')
                                and not self.is_closed)

    # determines if this opp has matching start/end dates with a renewal, indicating
    # the upsell was added at the time of renewal and is therefore likely to not be
    # a new subscription
    def is_cotermed_opp(self, o_comp, opp_type=None):
        # Trivially, every opp co-terms with itself.
        if self.id == o_comp.id:
            return False

        # only match opps in the collection if they have a specified type
        if opp_type and o_comp.type != opp_type:
            return False

        if self.start_date == o_comp.start_date and self.end_date == o_comp.end_date:
            # logging.info(f"{self.id} appears to co-terminate with {o_comp.id}")
            return True

        return False

    def new_biz_opp(self):
        # Need to compare the start/end dates with other opps in the collection
        # to determine if the opp co-terms with any others.
        if self.opp_type == 'New Business' or self.opp_type == 'New Subscription':
            self.is_new_business = True
        elif self.type == 'new' and self.contract_months % 12 == 0:
            self.is_new_business = True

    def is_related_by_ocrs(self, o_comp):
        pay_roles = ['Paying User', 'User']
        overlapping_ids = 0
        this_paying_comp_former = 0
        this_former_comp_paying = 0
        this_paying_comp_paying = 0
        this_former_comp_former = 0

        # Trivially, every opp co-terms with itself.
        if self.id == o_comp.id:
            return False

        # One or the other opp has no OCRs
        if not (self.ocrs and o_comp.ocrs):
            return False

        for ocr_this in self.ocrs:
            this_id = ocr_this['ContactId']
            this_role = ocr_this['Role']

            # Experimental - only consider Paying users for the source opp
            if this_role not in pay_roles:
                continue

            for ocr_comp in o_comp.ocrs:
                comp_id = ocr_comp['ContactId']
                comp_role = ocr_comp['Role']

                if this_id != comp_id:
                    continue

                overlapping_ids += 1

                if this_role in pay_roles and comp_role == 'Former User (Canceled)':
                    this_paying_comp_former += 1
                elif this_role == 'Former User (Canceled)' and comp_role in pay_roles:
                    this_former_comp_paying += 1
                elif this_role in pay_roles and comp_role in pay_roles:
                    this_paying_comp_paying += 1
                elif this_role == 'Former User (Canceled)' and comp_role == 'Former User (Canceled)':
                    this_former_comp_former += 1

        if overlapping_ids > 0:
            logging.info(f"{self.id} ({self.opp_type}) related to {o_comp.id} ({o_comp.opp_type}) - Total: {overlapping_ids} | TPCF: {this_paying_comp_former} | TFCP: {this_former_comp_former} | TPCP: {this_paying_comp_paying} | TFCF: {this_former_comp_former}")
            self.related_opps.append((o_comp, overlapping_ids, this_paying_comp_former, this_former_comp_paying, this_paying_comp_paying, this_former_comp_former))
            return True

        return False



def recon_account(account_id: str, output_to_csv: bool=False):


    # Get all opportunity data
    soql = f"SELECT Id, AccountId, Name, CreatedDate, CloseDate, Type, Effective_Date__c, Subscription__c, "
    soql += f"Primary_Contact__c, Primary_Contact__r.Name, Primary_Contact__r.Email, "
    soql += f"Decision_Maker__c, Decision_Maker__r.Name, Decision_Maker__r.Email, "
    soql += f"Contract_Length_Months__c, IsClosed, IsWon, Contract_Start_Date__c, Contract_End_Date__c, "
    soql += f"RecordTypeId, "
    soql += f"(SELECT Id, Name, Email, Status_of_User__c FROM Contacts__r), " # non-renewals
    soql += f"(SELECT Id, Name, Email, Status_of_User__c FROM Contacts1__r), " # renewals
    soql += f"(SELECT Id, ContactId, OpportunityId, Role FROM OpportunityContactRoles) "
    soql += f"FROM Opportunity WHERE AccountId = '{account_id}' AND (IsClosed = false OR IsWon = true) "
    soql += "ORDER BY Effective_Date__c DESC, Id_18__c DESC"

    logging.info('Loading Opportunities from Salesforce...')
    opps = sfdc.sfdc_client.query(soql)

    opp_collection = []
    for o in opps:
        opp = Opp(o)

        opp_collection.append(opp)

    logging.info('Analyzing Opportunities...')

    for idx, o in enumerate(opp_collection):
        if o.is_curr_renewal:
            logging.info(f"{idx}) Top level renewal - Id: {o.id}")
        else:
            continue

        is_cotermed = False
        is_related = False
        for o_2 in opp_collection[idx:]:
            is_cotermed = o.is_cotermed_opp(o_2)
            is_related = o.is_related_by_ocrs(o_2)

    if output_to_csv:
        output_recon_csv(opp_collection)

def output_recon_csv(opp_collection):
    filename = 'sanofi.csv'
    output_path = Path(r"C:\Users\Kevin\Downloads\AlphaSense") / filename

    with open(output_path, 'w', newline='') as csvfile:
        headers = ['Renewal Id', 'Prior Opp Id', 'Type', 'CloseDate', 'Contract Start Date', 'Contract End Date', 'OCR Count', 'Note' ]

        writer = csv.writer(csvfile)

        writer.writerow(headers)

        row_coll = []
        for o in opp_collection:
            if not o.is_curr_renewal:
                continue


            row = [o.id, o.id, o.opp_type, o.close_date, o.start_date, o.end_date, 0, 'Current Renewal']
            row_coll.append(row)

            for o_r, total, tpcf, tfcp, tpcp, tfcf in o.related_opps:
                # (o_comp, overlapping_ids, this_paying_comp_former, this_former_comp_paying, this_paying_comp_paying, this_former_comp_former)
                row = [o_r.id, o.id, o_r.opp_type, o_r.close_date, o_r.start_date, o_r.end_date, total, '']
                row_coll.append(row)


        writer.writerows(row_coll)

    logging.info('Done!')