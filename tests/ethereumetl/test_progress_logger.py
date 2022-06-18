from etherdata.utility.progress_logger import ProgressLogger


def test_progress_logger():
    logger_mock = LoggerMock()
    progress_logger = ProgressLogger(logger=logger_mock, log_item_step=1000)

    progress_logger.start()
    [progress_logger.track(100) for _ in range(100)]
    progress_logger.finish()

    assert len(logger_mock.logs) == 12
    assert logger_mock.logs[0] == 'Started work.'
    assert logger_mock.logs[1] == '1000 items processed.'
    assert logger_mock.logs[11].startswith('Finished work. Total items processed: 10000. Took ')


def test_progress_logger_with_total_items():
    logger_mock = LoggerMock()
    progress_logger = ProgressLogger(logger=logger_mock, log_percentage_step=5)

    progress_logger.start(total_items=1234)
    [progress_logger.track(99) for _ in range(100)]
    progress_logger.finish()

    assert len(logger_mock.logs) == 102
    assert logger_mock.logs[0] == 'Started work. Items to process: 1234.'
    assert logger_mock.logs[1] == '99 items processed. Progress is 8%.'
    assert logger_mock.logs[100] == '9900 items processed. Progress is 802%!!!'
    assert logger_mock.logs[101].startswith('Finished work. Total items processed: 9900. Took ')


class LoggerMock:
    def __init__(self):
        self.logs = []

    def info(self, message):
        self.logs.append(message)
