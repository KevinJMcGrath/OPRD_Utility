import package_logger

from sfdc import setup, breakdown

package_logger.initialize_logging()

def run_main():
    aid = '0013g00000W3cGZ'
    # breakdown.breakdown_test_objects(aid)

    setup.setup_test_records()


if __name__ == '__main__':
    run_main()