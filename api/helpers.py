from dotenv import dotenv_values


CONFIG = dotenv_values('_CI/.env')


def get_server_host():
    return CONFIG['SERVER_HOST_NAME']
