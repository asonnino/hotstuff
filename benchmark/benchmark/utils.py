class Color:
    HEADER = '\033[95m'
    OK_BLUE = '\033[94m'
    OK_GREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    END = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


class Print:
    @staticmethod
    def info(message):
        print(message)

    @staticmethod
    def important(message):
        print(f'{Color.OK_GREEN}{message}{Color.END}')

    @staticmethod
    def error(message, cause=None):
        print(f'\n{Color.BOLD}{Color.FAIL}ERROR{Color.END}: {message}\n')
        if cause is not None:
            print(f'Caused by: \n{cause}\n')
