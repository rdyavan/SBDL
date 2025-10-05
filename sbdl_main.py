import sys

from lib.logger import Log4j
from lib import utils

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: sdbl {local, qa, prod} {load_date} : arguments are missing")
        sys.exit(-1)

    job_run_env = sys.argv[1].upper()
    local_date = sys.argv[2]

    spark = utils.get_spark_session(job_run_env)
    logger = Log4j(spark)

    logger.info("Finished creating spark session successfully.")


