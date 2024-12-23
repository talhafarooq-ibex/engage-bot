from collections import Counter, defaultdict
from datetime import datetime, timedelta

from decorators.jwt import jwt_token
from decorators.key import x_app_key
from decorators.teams import x_super_team
from decouple import config
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from utilities.database import connect
from utilities.validation import check_required_fields, validate_inputs

from routers.dashboard.utilities.filters import \
    filter_records_session as filter_records
from routers.dashboard.utilities.filters import str_to_datetime

slug_db = config("SLUG_DATABASE")

display_transfer_message_english = config("DISPLAY_TRANSFER_MESSAGE_ENGLISH")
display_transfer_message_arabic = config("DISPLAY_TRANSFER_MESSAGE_ARABIC")

display_bye_message_english = config("DISPLAY_HUMAN_END_MESSAGE_ENGLISH")
display_bye_message_arabic = config("DISPLAY_HUMAN_END_MESSAGE_ARABIC")

dashboard_router = APIRouter()

@dashboard_router.get('/total_sessions')
@x_super_team
@x_app_key
@jwt_token
async def total_sessions(request: Request):
    try:
        data = request.query_params

        required_fields = ['bot_id', 'start_date', 'end_date']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = "An error occurred: missing parameter(s)")
        
        bot_id, start_date, end_date = data.get('bot_id'), data.get('start_date'), data.get('end_date')

        company_id = request.headers.get('x-super-team')

        result = await validate_inputs(company_id, bot_id, None)
        if not result:
            raise HTTPException(status_code = 400, detail = "An error occurred: invalid parameter(s)")
        else:
            bots_record, _, _ = result

        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)

        profiles_collections = db['profiles']

        profiles_records = await profiles_collections.find({}).to_list(length=None)

        current_records, previous_records = filter_records(profiles_records, start_date, end_date)

        total_current_sessions = len(current_records)
        total_previous_sessions = len(previous_records)

        if total_previous_sessions == 0:
            percentage_change = 0.0 if total_current_sessions == 0 else 100.0
        else:
            percentage_change = ((total_current_sessions - total_previous_sessions) / total_previous_sessions) * 100

        return JSONResponse(content={
            "detail": {
                "total_sessions": total_current_sessions,
                "percentage_change": round(percentage_change, 2)
            }
        }, status_code=200)
    
    except Exception as e:
        raise HTTPException(status_code = 500, detail=f"An error occurred: {str(e)}") from e

@dashboard_router.get('/engaged_session_rate')
@x_super_team
@x_app_key
@jwt_token
async def engaged_session_rate(request: Request):
    try:
        data = request.query_params

        required_fields = ['bot_id', 'start_date', 'end_date']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = "An error occurred: missing parameter(s)")
        
        bot_id, start_date, end_date = data.get('bot_id'), data.get('start_date'), data.get('end_date')

        company_id = request.headers.get('x-super-team')

        result = await validate_inputs(company_id, bot_id, None)
        if not result:
            raise HTTPException(status_code = 400, detail = "An error occurred: invalid parameter(s)")
        else:
            bots_record, _, _ = result

        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)

        profiles_collections = db['profiles']

        profiles_records = await profiles_collections.find({}).to_list(length=None)
        current_records, previous_records = filter_records(profiles_records, start_date, end_date)

        outside_records = [
            msg for msg in current_records
            if not msg['latest_timestamp'] 
        ]

        if current_records:     
            registered_profiles_current = len(current_records)
        else:
            registered_profiles_current = 0

        if previous_records:
            registered_profiles_previous = len(previous_records)
        else:
            registered_profiles_previous = 0

        if registered_profiles_previous == 0:
            percentage_change = 0.0 if registered_profiles_current == 0 else 100.0
        else:
            percentage_change = ((registered_profiles_current - registered_profiles_previous) / registered_profiles_previous) * 100

        if not len(profiles_records):
            engaged_rate = 0.0

        if registered_profiles_current:
            registered_non_engaged_profiles_current = len(outside_records)
            engaged_rate = (registered_profiles_current / (registered_profiles_current + registered_non_engaged_profiles_current)) * 100

        return JSONResponse(content={
            "detail": {
                "engaged_session_rate": round(engaged_rate, 2),
                "percentage_change": round(percentage_change, 2)
            }
        }, status_code=200)
    
    except Exception as e:
        raise HTTPException(status_code = 500, detail=f"An error occurred: {str(e)}") from e

@dashboard_router.get('/total_users')
@x_super_team
@x_app_key
@jwt_token
async def total_users(request: Request):
    try:
        data = request.query_params

        required_fields = ['bot_id', 'start_date', 'end_date']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = "An error occurred: missing parameter(s)")
        
        bot_id, start_date, end_date = data.get('bot_id'), data.get('start_date'), data.get('end_date')

        company_id = request.headers.get('x-super-team')

        result = await validate_inputs(company_id, bot_id, None)
        if not result:
            raise HTTPException(status_code = 400, detail = "An error occurred: invalid parameter(s)")
        else:
            bots_record, _, _ = result

        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)

        profiles_collections = db['profiles']

        profiles_records = await profiles_collections.find({}).to_list(length=None)
        current_records, previous_records = filter_records(profiles_records, start_date, end_date)

        if current_records:     
            unique_emails_current = {msg['email'] for msg in current_records}
            unique_emails_count_current = len(unique_emails_current)
        else:
            unique_emails_count_current = 0

        if previous_records:
            unique_emails_previous = {msg['email'] for msg in previous_records}
            unique_emails_count_previous = len(unique_emails_previous)
        else:
            unique_emails_count_previous = 0

        if unique_emails_count_previous == 0:
            percentage_change = 0.0 if unique_emails_count_current == 0 else 100.0
        else:
            percentage_change = ((unique_emails_count_current - unique_emails_count_previous) / unique_emails_count_previous) * 100

        return JSONResponse(content={
            "detail": {
                "total_users": unique_emails_count_current,
                "percentage_change": round(percentage_change, 2)
            }
        }, status_code=200)
    
    except Exception as e:
        raise HTTPException(status_code = 500, detail=f"An error occurred: {str(e)}") from e

@dashboard_router.get('/average_session_per_user')
@x_super_team
@x_app_key
@jwt_token
async def average_session_per_user(request: Request):
    try:
        data = request.query_params

        required_fields = ['bot_id', 'start_date', 'end_date']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = "An error occurred: missing parameter(s)")
        
        bot_id, start_date, end_date = data.get('bot_id'), data.get('start_date'), data.get('end_date')

        company_id = request.headers.get('x-super-team')

        result = await validate_inputs(company_id, bot_id, None)
        if not result:
            raise HTTPException(status_code = 400, detail = "An error occurred: invalid parameter(s)")
        else:
            bots_record, _, _ = result

        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)

        profiles_collections = db['profiles']

        profiles_records = await profiles_collections.find({}).to_list(length=None)
        current_records, previous_records = filter_records(profiles_records, start_date, end_date)

        if current_records:     
            unique_emails_current = {msg['email'] for msg in current_records}
            unique_emails_count_current = len(unique_emails_current)

            average_sessions_current = round(len(current_records) / unique_emails_count_current, 2)
        else:
            average_sessions_current = 0

        if previous_records:
            unique_emails_previous = {msg['email'] for msg in previous_records}
            unique_emails_count_previous = len(unique_emails_previous)

            average_sessions_previous = round(len(previous_records) / unique_emails_count_previous, 2)
        else:
            average_sessions_previous = 0

        if average_sessions_previous == 0:
            percentage_change = 0.0 if average_sessions_current == 0 else 100.0
        else:
            percentage_change = ((average_sessions_current - average_sessions_previous) / average_sessions_previous) * 100

        return JSONResponse(content={
            "detail": {
                "average_session_per_user": average_sessions_current,
                "percentage_change": round(percentage_change, 2)
            }
        }, status_code=200)
    
    except Exception as e:
        raise HTTPException(status_code = 500, detail=f"An error occurred: {str(e)}") from e

@dashboard_router.get('/user_retention_rate')
@x_super_team
@x_app_key
@jwt_token
async def user_retention_rate(request: Request):
    try:
        data = request.query_params

        required_fields = ['bot_id', 'start_date', 'end_date']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = "An error occurred: missing parameter(s)")
        
        bot_id, start_date, end_date = data.get('bot_id'), data.get('start_date'), data.get('end_date')

        company_id = request.headers.get('x-super-team')

        result = await validate_inputs(company_id, bot_id, None)
        if not result:
            raise HTTPException(status_code = 400, detail = "An error occurred: invalid parameter(s)")
        else:
            bots_record, _, _ = result

        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)

        profiles_collections = db['profiles']

        profiles_records = await profiles_collections.find({}).to_list(length=None)
        current_records, previous_records = filter_records(profiles_records, start_date, end_date)

        if current_records:     
            emails_current = [msg['email'] for msg in current_records]
            unique_emails_current = list({msg['email'] for msg in current_records})

            email_counts = Counter(emails_current)
            retained_emails_current = [email for email, count in email_counts.items() if count > 1]

            retention_rate_current = round((len(retained_emails_current) / len(unique_emails_current)) * 100, 2)
        else:
            retention_rate_current = 0

        if previous_records:
            emails_previous = [msg['email'] for msg in previous_records]
            unique_emails_previous = list({msg['email'] for msg in previous_records})

            email_counts = Counter(emails_previous)
            retained_emails_previous = [email for email, count in email_counts.items() if count > 1]

            retention_rate_previous = round((len(retained_emails_previous) / len(unique_emails_previous)) * 100, 2)
        else:
            retention_rate_previous = 0

        if retention_rate_previous == 0:
            percentage_change = 0.0 if retention_rate_current == 0 else 100.0
        else:
            percentage_change = ((retention_rate_current - retention_rate_previous) / retention_rate_previous) * 100

        return JSONResponse(content={
            "detail": {
                "retention_rate": retention_rate_current,
                "percentage_change": round(percentage_change, 2)
            }
        }, status_code=200)
    
    except Exception as e:
        raise HTTPException(status_code = 500, detail=f"An error occurred: {str(e)}") from e

@dashboard_router.get('/session_containment_rate')
@x_super_team
@x_app_key
@jwt_token
async def session_containment_rate(request: Request):
    try:
        data = request.query_params

        required_fields = ['bot_id', 'start_date', 'end_date']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = "An error occurred: missing parameter(s)")
        
        bot_id, start_date, end_date = data.get('bot_id'), data.get('start_date'), data.get('end_date')

        company_id = request.headers.get('x-super-team')

        result = await validate_inputs(company_id, bot_id, None)
        if not result:
            raise HTTPException(status_code = 400, detail = "An error occurred: invalid parameter(s)")
        else:
            bots_record, _, _ = result

        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)

        messages_collections = db['messages']

        messages_records = await messages_collections.find({}).to_list(length=None)

        start_datetime = datetime.strptime(start_date, "%d/%m/%Y %H:%M:%S")
        end_datetime = datetime.strptime(end_date, "%d/%m/%Y %H:%M:%S")

        previous_filter = start_datetime - (end_datetime - start_datetime)

        latest_timestamp = datetime.now()

        current_filtered_records = [
            msg for msg in messages_records
            if (msg['end_conversation'] or datetime.strptime(msg['latest_timestamp'], '%d/%m/%Y %H:%M:%S') + timedelta(minutes=int(msg['timeout'])) < latest_timestamp) 
            and not msg['transfer_conversation'] 
            and str_to_datetime(msg['latest_timestamp']) >= start_datetime 
            and str_to_datetime(msg['latest_timestamp']) < end_datetime 
        ]

        current_records = [
            msg for msg in messages_records
            if str_to_datetime(msg['latest_timestamp']) >= start_datetime
            and str_to_datetime(msg['latest_timestamp']) < end_datetime
        ]

        previous_filtered_records = [
            msg for msg in messages_records
            if (msg['end_conversation'] or datetime.strptime(msg['latest_timestamp'], '%d/%m/%Y %H:%M:%S') + timedelta(minutes=int(msg['timeout'])) < latest_timestamp)
            and not msg['transfer_conversation'] 
            and str_to_datetime(msg['latest_timestamp']) >= previous_filter  
            and str_to_datetime(msg['latest_timestamp']) < start_datetime  
        ]

        total_current_sessions = len(current_filtered_records)
        total_previous_sessions = len(previous_filtered_records)

        if total_previous_sessions == 0:
            percentage_change = 0.0 if total_current_sessions == 0 else 100.0
        else:
            percentage_change = ((total_current_sessions - total_previous_sessions) / total_previous_sessions) * 100

        if len(messages_records):
            if total_current_sessions:
                containment_rate = total_current_sessions / len(current_records) * 100
            else:
                containment_rate = 0.0
        else:
            containment_rate = 0.0

        return JSONResponse(content={
            "detail": {
                "containment_rate": round(containment_rate, 2),
                "percentage_change": round(percentage_change, 2)
            }
        }, status_code=200)
    
    except Exception as e:
        raise HTTPException(status_code = 500, detail=f"An error occurred: {str(e)}") from e

@dashboard_router.get('/session_summary')
@x_super_team
@x_app_key
@jwt_token
async def session_summary(request: Request):
    try:
        data = request.query_params

        required_fields = ['bot_id', 'start_date', 'end_date']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = "An error occurred: missing parameter(s)")
        
        bot_id, start_date, end_date = data.get('bot_id'), data.get('start_date'), data.get('end_date')

        company_id = request.headers.get('x-super-team')

        result = await validate_inputs(company_id, bot_id, None)
        if not result:
            raise HTTPException(status_code = 400, detail = "An error occurred: invalid parameter(s)")
        else:
            bots_record, _, _ = result

        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)

        profiles_collections = db['profiles']

        profiles_records = await profiles_collections.find({}).to_list(length=None)

        start_datetime = datetime.strptime(start_date, "%d/%m/%Y %H:%M:%S")
        end_datetime = datetime.strptime(end_date, "%d/%m/%Y %H:%M:%S")

        filtered_records = []

        for msg in profiles_records:
            if msg['latest_timestamp']:
                msg_timestamp = str_to_datetime(msg['latest_timestamp'])
                if start_datetime <= msg_timestamp <= end_datetime:
                    filtered_records.append(msg)
            elif msg['created_date']:
                msg_created_date = str_to_datetime(msg['created_date'])
                if start_datetime <= msg_created_date <= end_datetime:
                    filtered_records.append(msg)

        if filtered_records:
            total_sessions = len(filtered_records)

            unique_emails = {msg['email'] for msg in filtered_records}
            unique_emails_count = len(unique_emails)

            average_sessions = round(total_sessions / unique_emails_count, 2) if unique_emails_count else 0

            engaged_sessions = [msg for msg in filtered_records if msg['latest_timestamp']]
            total_engaged_sessions = len(engaged_sessions)

            unique_emails = {msg['email'] for msg in engaged_sessions}
            unique_emails_count = len(unique_emails)

            avg_engaged_sessions = round(total_engaged_sessions / unique_emails_count, 2) if unique_emails_count else 0
        else:
            total_sessions = 0
            average_sessions = 0
            total_engaged_sessions = 0
            avg_engaged_sessions = 0

        return JSONResponse(
            content = {
                "detail": {'total_sessions': total_sessions, 'average_sessions': average_sessions, 'total_engaged_sessions': total_engaged_sessions, 
                'avg_engaged_sessions': avg_engaged_sessions}}, 
            status_code = 200)
    
    except Exception as e:
        raise HTTPException(status_code = 500, detail=f"An error occurred: {str(e)}") from e
    
@dashboard_router.get('/total_sessions_graph')
@x_super_team
@x_app_key
@jwt_token
async def total_sessions_graph(request: Request):
    try:
        data = request.query_params

        # Change: Expecting start_date and end_date instead of filter_days
        required_fields = ['bot_id', 'start_date', 'end_date']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code=400, detail="An error occurred: missing parameter(s)")

        bot_id = data.get('bot_id')
        start_date_str = data.get('start_date')
        end_date_str = data.get('end_date')

        # Parse start_date and end_date
        start_datetime = datetime.strptime(start_date_str, "%d/%m/%Y %H:%M:%S")
        end_datetime = datetime.strptime(end_date_str, "%d/%m/%Y %H:%M:%S")
        company_id = request.headers.get('x-super-team')

        result = await validate_inputs(company_id, bot_id, None)
        if not result:
            raise HTTPException(status_code = 400, detail = "An error occurred: invalid parameter(s)")
        else:
            bots_record, _, _ = result

        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)

        messages_collections = db['messages']
        messages_records = await messages_collections.find({}).to_list(length=None)

        # Initialize sessions_per_day dictionary for the date range
        sessions_per_day = {}
        current_date = start_datetime
        while current_date <= end_datetime:
            date_str = current_date.strftime('%Y-%m-%d')
            sessions_per_day[date_str] = 0
            current_date += timedelta(days=1)

        # Filter messages based on start_date and end_date
        for msg in messages_records:
            if msg['latest_timestamp']:
                msg_datetime = str_to_datetime(msg['latest_timestamp'])
                if start_datetime <= msg_datetime <= end_datetime:
                    msg_date_str = msg_datetime.strftime('%Y-%m-%d')
                    if msg_date_str in sessions_per_day:
                        sessions_per_day[msg_date_str] += 1

        return JSONResponse(content={"detail": sessions_per_day}, status_code=200)

    except Exception as e:
        raise HTTPException(status_code = 500, detail=f"An error occurred: {str(e)}") from e

@dashboard_router.get('/total_messages')
@x_super_team
@x_app_key
@jwt_token
async def total_messages(request: Request):
    try:
        data = request.query_params

        # Expecting start_date and end_date instead of filter_days
        required_fields = ['bot_id', 'start_date', 'end_date']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code=400, detail="An error occurred: missing parameter(s)")

        bot_id = data.get('bot_id')
        start_date_str = data.get('start_date')
        end_date_str = data.get('end_date')

        # Parse start_date and end_date
        start_datetime = datetime.strptime(start_date_str, "%d/%m/%Y %H:%M:%S")
        end_datetime = datetime.strptime(end_date_str, "%d/%m/%Y %H:%M:%S")
        company_id = request.headers.get('x-super-team')

        result = await validate_inputs(company_id, bot_id, None)
        if not result:
            raise HTTPException(status_code = 400, detail = "An error occurred: invalid parameter(s)")
        else:
            bots_record, _, _ = result

        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)

        messages_collections = db['messages']
        message_records = await messages_collections.find({}).to_list(length=None)

        # Initialize daily message counts for the date range
        daily_messages = defaultdict(lambda: {'inbound': 0, 'outbound': 0})

        current_date = start_datetime
        while current_date <= end_datetime:
            date_str = current_date.strftime('%Y-%m-%d')
            daily_messages[date_str] = {'inbound': 0, 'outbound': 0}
            current_date += timedelta(days=1)

        # Filter and aggregate messages within the date range
        for record in message_records:
            record_date = str_to_datetime(record['latest_timestamp']).strftime('%Y-%m-%d')
            if record_date in daily_messages:
                for message in record['roles']:
                    if message['type'] == 'human':
                        daily_messages[record_date]['inbound'] += 1
                    elif message['type'] in ['human-agent', 'ai-agent']:
                        daily_messages[record_date]['outbound'] += 1

        # Format the response
        summary = {
            day: {
                'total': daily_messages[day]['inbound'] + daily_messages[day]['outbound'],
                'inbound': daily_messages[day]['inbound'],
                'outbound': daily_messages[day]['outbound']
            }
            for day in daily_messages
        }

        return JSONResponse(content={"detail": summary}, status_code=200)

    except Exception as e:
        raise HTTPException(status_code = 500, detail=f"An error occurred: {str(e)}") from e

@dashboard_router.get('/users')
@x_super_team
@x_app_key
@jwt_token
async def users(request: Request):
    try:
        data = request.query_params

        required_fields = ['bot_id', 'start_date', 'end_date', 'engaged_sessions']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = "An error occurred: missing parameter(s)")
            
        bot_id, engaged_sessions = data.get('bot_id'), data.get('engaged_sessions')
        start_date, end_date = data.get('start_date'), data.get('end_date')

        company_id = request.headers.get('x-super-team')

        result = await validate_inputs(company_id, bot_id, None)
        if not result:
            raise HTTPException(status_code = 400, detail = "An error occurred: invalid parameter(s)")
        else:
            bots_record, _, _ = result

        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)

        profiles_collections = db['profiles']

        profiles_records = await profiles_collections.find({}).to_list(length=None)

        start_datetime = datetime.strptime(start_date, "%d/%m/%Y %H:%M:%S")
        end_datetime = datetime.strptime(end_date, "%d/%m/%Y %H:%M:%S")

        if engaged_sessions == '1':  
            filtered_records = [
                msg for msg in profiles_records
                if msg['latest_timestamp'] and start_datetime <= str_to_datetime(msg['latest_timestamp']) <= end_datetime
            ]
        elif engaged_sessions == '0':
            filtered_records = []
            for msg in profiles_records:
                if msg['latest_timestamp']:
                    if start_datetime <= str_to_datetime(msg['latest_timestamp']) <= end_datetime:
                        filtered_records.append(msg)
                else:
                    if msg['created_date'] and start_datetime <= str_to_datetime(msg['created_date']) <= end_datetime:
                        filtered_records.append(msg)
        else:
            raise HTTPException(status_code=400, detail="An error occurred: invalid 'engaged_sessions' parameter")

        if filtered_records:     
            emails_current = [msg['email'] for msg in filtered_records]
            
            email_counts = Counter(emails_current)
            returning_users = len([email for email, count in email_counts.items() if count > 1])
            new_users = len([email for email, count in email_counts.items() if count == 1])

            total = new_users + returning_users
            
            return JSONResponse(content={"detail": {'total': total, 'new_users': new_users, 'returning_users': returning_users}}, status_code=200)
        else:
            return JSONResponse(content={"detail": 0}, status_code=200)

    except Exception as e:
        raise HTTPException(status_code = 500, detail=f"An error occurred: {str(e)}") from e
        
@dashboard_router.get('/messages')
@x_super_team
@x_app_key
@jwt_token
async def messages(request: Request):
    try:
        data = request.query_params

        required_fields = ['bot_id', 'start_date', 'end_date']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = "An error occurred: missing parameter(s)")
        
        bot_id, start_date, end_date = data.get('bot_id'), data.get('start_date'), data.get('end_date')

        company_id = request.headers.get('x-super-team')

        result = await validate_inputs(company_id, bot_id, None)
        if not result:
            raise HTTPException(status_code = 400, detail = "An error occurred: invalid parameter(s)")
        else:
            bots_record, _, _ = result

        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)

        messages_collections = db['messages']

        message_records = await messages_collections.find({}).to_list(length = None)

        start_datetime = datetime.strptime(start_date, "%d/%m/%Y %H:%M:%S")
        end_datetime = datetime.strptime(end_date, "%d/%m/%Y %H:%M:%S")

        current_records = [
            msg for msg in message_records
            if start_datetime <= str_to_datetime(msg['latest_timestamp']) <= end_datetime
        ]

        inbound, outbound = 0, 0

        for record in current_records:
            for message in record['roles']:
                if message['type'] == 'human':
                    inbound += 1
                elif message['type'] == 'human-agent' or message['type'] == 'ai-agent':
                    outbound += 1

        total = inbound + outbound

        return JSONResponse(content={"detail": {'total': total, 'inbound': inbound, 'outbound': outbound}}, status_code=200)

    except Exception as e:
        raise HTTPException(status_code = 500, detail=f"An error occurred: {str(e)}") from e   

@dashboard_router.get('/session_expiry_reason')
@x_super_team
@x_app_key
@jwt_token
async def session_expiry_reason(request: Request):
    try:
        data = request.query_params

        required_fields = ['bot_id', 'start_date', 'end_date']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = "An error occurred: missing parameter(s)")
        
        bot_id, start_date, end_date = data.get('bot_id'), data.get('start_date'), data.get('end_date')

        company_id = request.headers.get('x-super-team')

        result = await validate_inputs(company_id, bot_id, None)
        if not result:
            raise HTTPException(status_code = 400, detail = "An error occurred: invalid parameter(s)")
        else:
            bots_record, _, _ = result

        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)

        messages_collections = db['messages']

        message_records = await messages_collections.find({}).to_list(length=None)

        start_datetime = datetime.strptime(start_date, "%d/%m/%Y %H:%M:%S")
        end_datetime = datetime.strptime(end_date, "%d/%m/%Y %H:%M:%S")

        current_records = [
            msg for msg in message_records
            if start_datetime <= str_to_datetime(msg['latest_timestamp']) <= end_datetime
        ]

        expired_sessions, agent_takeover, go_to_agent_action, close_session_action, user_closed_session = 0, 0, 0, 0, 0

        if current_records:
            for record in current_records:
                timestamp_format = '%d/%m/%Y %H:%M:%S'
                latest_timestamp = datetime.now()
                expiration_time = datetime.strptime(record['latest_timestamp'], timestamp_format) + timedelta(minutes=int(record['timeout']))

                if expiration_time < latest_timestamp and not record['end_conversation'] and not record['transfer_conversation'] and not record['human_intervention']:
                    expired_sessions += 1
                elif record['transfer_conversation'] and not record['human_intervention']:
                    go_to_agent_action += 1
                elif not record['transfer_conversation'] and not record['human_intervention'] and record['end_conversation']:
                    user_closed_session += 1
                elif (record['transfer_conversation'] or record['human_intervention']) and record['end_conversation']:
                    close_session_action += 1

        return JSONResponse(
            content={
                "expired_sessions": expired_sessions, 
                'go_to_agent_action': go_to_agent_action, 
                'user_closed_session': user_closed_session, 
                'close_session_action': close_session_action, 
                'agent_takeover': agent_takeover
            }, 
            status_code=200
        )

    except Exception as e:
        raise HTTPException(status_code = 500, detail=f"An error occurred: {str(e)}") from e

@dashboard_router.get('/session_duration')
@x_super_team
@x_app_key
@jwt_token
async def session_duration(request: Request):
    try:
        data = request.query_params

        required_fields = ['bot_id', 'start_date', 'end_date']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = "An error occurred: missing parameter(s)")
        
        bot_id, start_date, end_date = data.get('bot_id'), data.get('start_date'), data.get('end_date')

        company_id = request.headers.get('x-super-team')

        result = await validate_inputs(company_id, bot_id, None)
        if not result:
            raise HTTPException(status_code = 400, detail = "An error occurred: invalid parameter(s)")
        else:
            bots_record, _, _ = result

        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)

        messages_collections = db['messages']

        message_records = await messages_collections.find({}).to_list(length=None)

        start_datetime = datetime.strptime(start_date, "%d/%m/%Y %H:%M:%S")
        end_datetime = datetime.strptime(end_date, "%d/%m/%Y %H:%M:%S")

        current_records = [
            msg for msg in message_records
            if start_datetime <= str_to_datetime(msg['latest_timestamp']) <= end_datetime
        ]

        avg_session_duration = 0.0
        if current_records:
            for record in current_records:
                if record['end_conversation']:
                    first = record['roles'][0]['timestamp']
                    last = record['roles'][-1]['timestamp']

                    format_str = "%d/%m/%Y %H:%M:%S"
                    dt1 = datetime.strptime(first, format_str)
                    dt2 = datetime.strptime(last, format_str)

                    time_difference = dt2 - dt1
                    avg_session_duration += time_difference.total_seconds()

            average_seconds = avg_session_duration / len(current_records)

            hours, remainder = divmod(average_seconds, 3600)
            minutes, seconds = divmod(remainder, 60)

            avg_session_duration = f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"

        go_to_agent_action, total = 0.0, 0
        if current_records:
            for record in current_records:
                if record['transfer_conversation'] and not record['human_intervention']:
                    total += 1

                    first = record['roles'][0]['timestamp']

                    for role in record['roles']:
                        if role['text'] == display_transfer_message_english or role['text'] == display_transfer_message_arabic:
                            last = role['timestamp']

                    format_str = "%d/%m/%Y %H:%M:%S"
                    dt1 = datetime.strptime(first, format_str)
                    dt2 = datetime.strptime(last, format_str)

                    time_difference = dt2 - dt1
                    go_to_agent_action += time_difference.total_seconds()

            if go_to_agent_action:
                average_seconds = go_to_agent_action / total

                hours, remainder = divmod(average_seconds, 3600)
                minutes, seconds = divmod(remainder, 60)

                go_to_agent_action = f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"
            else:
                go_to_agent_action = '00:00:00'

        user_closed_session, total = 0.0, 0
        if current_records:
            for record in current_records:
                if not record['transfer_conversation'] and not record['human_intervention'] and record['end_conversation']:
                    total += 1

                    first = record['roles'][0]['timestamp']

                    for role in record['roles']:
                        if role['text'] == display_bye_message_english or role['text'] == display_bye_message_arabic:
                            last = role['timestamp']

                    format_str = "%d/%m/%Y %H:%M:%S"
                    dt1 = datetime.strptime(first, format_str)
                    dt2 = datetime.strptime(last, format_str)

                    time_difference = dt2 - dt1
                    user_closed_session += time_difference.total_seconds()

            if user_closed_session:
                average_seconds = user_closed_session / total

                hours, remainder = divmod(average_seconds, 3600)
                minutes, seconds = divmod(remainder, 60)

                user_closed_session = f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"
            else:
                user_closed_session = '00:00:00'

        close_session_action, total = 0.0, 0
        if current_records:
            for record in current_records:
                first, last = '', ''
                if (record['transfer_conversation'] or record['human_intervention']) and record['end_conversation'] and not record['agent_expiry']:
                    total += 1

                    first = record['roles'][0]['timestamp']

                    for role in record['roles']:
                        if role['text'] == display_transfer_message_english or role['text'] == display_transfer_message_arabic:
                            last = role['timestamp']

                    if not last:
                        continue

                    format_str = "%d/%m/%Y %H:%M:%S"
                    dt1 = datetime.strptime(first, format_str)
                    dt2 = datetime.strptime(last, format_str)

                    time_difference = dt2 - dt1
                    close_session_action += time_difference.total_seconds()

            if close_session_action:
                average_seconds = close_session_action / total

                hours, remainder = divmod(average_seconds, 3600)
                minutes, seconds = divmod(remainder, 60)

                close_session_action = f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"
            else:
                close_session_action = '00:00:00'

        expired_sessions = '00:10:00'

        return JSONResponse(
            content={
                "avg_session_duration": avg_session_duration, 
                "expired_sessions": expired_sessions, 
                'go_to_agent_action': go_to_agent_action, 
                'user_closed_session': user_closed_session, 
                'close_session_action': close_session_action
            }, 
            status_code=200
        )

    except Exception as e:
        raise HTTPException(status_code = 500, detail=f"An error occurred: {str(e)}") from e