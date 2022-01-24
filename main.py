import package_logger

from sfdc import setup, breakdown, convert_newsub, convert_renewals, adjustments, account_recon

package_logger.initialize_logging()

def test_record_maintenance():
    aid = '0013g00000W3cGZ'
    # breakdown.breakdown_test_objects(aid)

    setup.setup_test_records()

def conversion():
    # New Sub Record Type Id: 0123g000000PP5Q
    # Renewal Record Type Id: 0123g000000PP5G

    status_set = ['Migration Ready - Batch 2']
    migration_id = 'b3-multi2'
    skip_prods = False

    # convert_renewals.convert_renewals(status_set=status_set, migration_id=migration_id, skip_products=skip_prods)
    # convert_renewals.link_contacts_to_sub(migration_id, True)

    convert_renewals.convert_renewals_from_SFDC()

def adjust():
    adjustments.product_line_adjust()

if __name__ == '__main__':
    acct_id = '0013200001FCjZT' #  Sanofi Genzyme
    account_recon.recon_account(account_id=acct_id, output_to_csv=True)
    # adjust()
    # conversion()