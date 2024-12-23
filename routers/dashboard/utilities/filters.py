from datetime import datetime


def str_to_datetime(timestamp_str):
    return datetime.strptime(timestamp_str, "%d/%m/%Y %H:%M:%S")

def filter_records_analytics(message_records, start_date, end_date):
    start_datetime = datetime.strptime(start_date, "%d/%m/%Y %H:%M:%S")
    end_datetime = datetime.strptime(end_date, "%d/%m/%Y %H:%M:%S")

    previous_end_datetime = start_datetime
    previous_start_datetime = previous_end_datetime - (end_datetime - start_datetime)

    current_records = [
        msg for msg in message_records
        if start_datetime <= str_to_datetime(msg['latest_timestamp']) < end_datetime
    ]

    previous_records = [
        msg for msg in message_records
        if previous_start_datetime <= str_to_datetime(msg['latest_timestamp']) < previous_end_datetime
    ]

    return current_records, previous_records

def filter_records_session(message_records, start_date, end_date):
    start_datetime = datetime.strptime(start_date, "%d/%m/%Y %H:%M:%S")
    end_datetime = datetime.strptime(end_date, "%d/%m/%Y %H:%M:%S")

    previous_end_datetime = start_datetime
    previous_start_datetime = previous_end_datetime - (end_datetime - start_datetime)

    current_records = [
        msg for msg in message_records
        if (msg['latest_timestamp'] and start_datetime <= str_to_datetime(msg['latest_timestamp']) < end_datetime)
        or (not msg['latest_timestamp'] and start_datetime <= str_to_datetime(msg['created_date']) < end_datetime)
    ]

    previous_records = [
        msg for msg in message_records
        if (msg['latest_timestamp'] and previous_start_datetime <= str_to_datetime(msg['latest_timestamp']) < previous_end_datetime)
        or (not msg['latest_timestamp'] and previous_start_datetime <= str_to_datetime(msg['created_date']) < previous_end_datetime)
    ]

    return current_records, previous_records