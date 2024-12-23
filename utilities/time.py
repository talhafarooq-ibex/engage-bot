from datetime import datetime


def current_time():
    now = datetime.now()
    date_time = now.strftime("%d/%m/%Y %H:%M:%S")

    return date_time