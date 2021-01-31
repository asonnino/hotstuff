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
    def heading(message):
        print(f'{Color.OK_GREEN}{message}{Color.END}')

    @staticmethod
    def error(message, cause=None):
        assert isinstance(cause, Exception) or cause is None
        print(f'\n{Color.BOLD}{Color.FAIL}ERROR{Color.END}: {message}\n')
        if cause is not None:
            # TODO: Ugly, write it better.
            deeper_cause = f'  2: {cause.cause}\n' if hasattr(cause, 'cause') else ''
            print(
                'Caused by: \n'
                f'  0: {type(cause)}\n'
                f'  1: {cause}\n'
                f'{deeper_cause}'
            )


class PathMaker:
    @staticmethod
    def binary_path():
        return '../target/release/'

    @staticmethod
    def node_crate_path():
        return '../node'

    @staticmethod
    def committee_file():
        return '.committee.json'

    @staticmethod
    def parameters_file():
        return '.parameters.json'

    @staticmethod
    def key_file(i): 
        return f'.node-{i}.json'

    @staticmethod
    def db_path(i): 
        return f'.db-{i}'

    @staticmethod
    def node_log_file(i): 
        return f'logs/node-{i}.log'

    @staticmethod
    def client_log_file(i): 
        return f'logs/client-{i}.log'
