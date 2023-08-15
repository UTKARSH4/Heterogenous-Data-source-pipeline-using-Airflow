import logging.config

logging.config.fileConfig('properties/configuration/logging.config')

loggers = logging.getLogger('Validate')


def get_current_date(spark):
    try:
        loggers.warning('started get_current_date method....')
        output = spark.sql("""select current_date""")
        loggers.warning("Validating spark {}".format(str(output.collect())))

    except Exception as e:
        loggers.error("error in get_current_date", str(e))

        raise

    else:
        loggers.warning('validation done..')