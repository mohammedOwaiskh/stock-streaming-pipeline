class Log4j2(object):

    def __init__(self, spark, name: str):
        log4j2 = spark._jvm.org.apache.log4j
        self.log = log4j2.LogManager.getLogger(name)

    def debug(self, message: str):
        self.log.debug(message)

    def info(self, message: str):
        self.log.info(message)

    def warn(self, message: str):
        self.log.warn(message)

    def error(self, message: str):
        self.log.error(message)

    def fatal(self, message: str):
        self.log.fatal(message)

    def is_debug_enabled(self) -> bool:
        return self.log.isDebugEnabled()

    def is_info_enabled(self) -> bool:
        return self.log.isInfoEnabled()


if __name__ == "__main__":
    log = Log4j2(__name__)

    print(log.is_info_enabled())
    log.info("Test Info")
    log.error("Test Error")
    log.warn("Test Warn")
    log.fatal("Test Fatal")
    print(log.is_debug_enabled())
    log.debug("Test Debug")
