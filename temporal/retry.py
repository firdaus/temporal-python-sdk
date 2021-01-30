import asyncio
import calendar
import time

INITIAL_DELAY_SECONDS = 3
BACK_OFF_MULTIPLIER = 2
MAX_DELAY_SECONDS = 5 * 60
RESET_DELAY_AFTER_SECONDS = 10 * 60


def retry(logger=None):
    def wrapper(fp):
        async def retry_loop(*args, **kwargs):
            last_failed_time = -1
            while True:
                try:
                    await fp(*args, **kwargs)
                    logger.debug("@retry decorated function %s exited, ending retry loop", fp.__name__)
                    break
                except Exception as ex:
                    now = calendar.timegm(time.gmtime())
                    if last_failed_time == -1 or (now - last_failed_time) > RESET_DELAY_AFTER_SECONDS:
                        delay_seconds = INITIAL_DELAY_SECONDS
                    else:
                        delay_seconds = delay_seconds * BACK_OFF_MULTIPLIER
                    if delay_seconds > MAX_DELAY_SECONDS:
                        delay_seconds = MAX_DELAY_SECONDS
                    last_failed_time = now
                    logger.error("%s failed: %s, retrying in %d seconds", fp.__name__, ex,
                                 delay_seconds, exc_info=True)
                    await asyncio.sleep(delay_seconds)

        return retry_loop

    return wrapper


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger("retry-test")

    @retry(logger=logger)
    async def main():
        raise Exception("blah")

    asyncio.run(main())
