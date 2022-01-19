import package_logger

from sfdc import setup, breakdown, convert_newsub, convert_renewals, adjustments

package_logger.initialize_logging()

def test_record_maintenance():
    aid = '0013g00000W3cGZ'
    # breakdown.breakdown_test_objects(aid)

    setup.setup_test_records()

def conversion():
    # New Sub Record Type Id: 0123g000000PP5Q
    # Renewal Record Type Id: 0123g000000PP5G

    status_set = ['no product in migration - batch 2']
    migration_id = 'b3-noprod2'
    skip_prods = True

    convert_renewals.convert_renewals(status_set=status_set, migration_id=migration_id, skip_products=skip_prods)


def adjust():
    opp_id = '0063g00000AlPL4AAN'
    adjustments.copy_contacts_to_sub(opp_id=opp_id, is_renewal=True)

if __name__ == '__main__':
    conversion()