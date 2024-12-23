from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse
from decouple import config
from datetime import datetime, timedelta
from collections import defaultdict

from decorators.jwt import jwt_token
from decorators.key import x_app_key
from decorators.teams import x_super_team
from utilities.database import connect
from utilities.validation import check_required_fields

analytics_router = APIRouter()

slug_db = config("SLUG_DATABASE")

display_transfer_message_english = config("DISPLAY_TRANSFER_MESSAGE_ENGLISH")
display_transfer_message_arabic = config("DISPLAY_TRANSFER_MESSAGE_ARABIC")

display_bye_message_english = config("DISPLAY_HUMAN_END_MESSAGE_ENGLISH")
display_bye_message_arabic = config("DISPLAY_HUMAN_END_MESSAGE_ARABIC")

def str_to_datetime(timestamp_str):
    return datetime.strptime(timestamp_str, "%d/%m/%Y %H:%M:%S")

def filter_records(message_records, start_date, end_date):
    start_datetime = datetime.strptime(start_date, "%d/%m/%Y %H:%M:%S")
    end_datetime = datetime.strptime(end_date, "%d/%m/%Y %H:%M:%S")
    print ()

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

@analytics_router.get('/total_bot_conversations')
@x_super_team
@x_app_key
@jwt_token
async def total_bot_conversations(request: Request):
    try:
        data = request.query_params

        required_fields = ['bot_id', 'start_date', 'end_date']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")
        
        bot_id, start_date, end_date = data.get('bot_id'), data.get('start_date'), data.get('end_date')

        company_id = request.headers.get('x-super-team')

        db = await connect()

        bots_collections = db['bots']
            
        bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 1})

        if bots_record:
            slug = bots_record['bot_name'] + slug_db
            db = await connect(slug)

            messages_collections = db['messages']
            profiles_collections = db['profiles']

            message_records = await messages_collections.find({}).to_list(length = None)
            current_records, previous_records = filter_records(message_records, start_date, end_date)

            web, whatsapp, sdk, total = 0, 0, 0, 0
            for record in current_records:
                profiles_record = await profiles_collections.find_one({'session_id': record['session_id']})
                
                try:
                    if profiles_record['queue'] == 'web':
                        web += 1
                        total += 1
                    elif profiles_record['queue'] == 'whatsapp':
                        whatsapp += 1
                        total += 1
                    elif profiles_record['queue'] == 'sdk':
                        sdk += 1
                        total += 1
                except:
                    web += 1
                    total += 1

            results = {'total': total, 'web': web, 'whatsapp': whatsapp, 'sdk': sdk}

            if len(previous_records) == 0:
                percentage = 0.0 if len(current_records) == 0 else 100.0
            else:
                percentage = round(((len(current_records) - len(previous_records)) / len(previous_records)) * 100, 1)
            
            return JSONResponse(content={"detail": results, "percentage": percentage}, status_code = 200)
        else:
            raise HTTPException(status_code = 404, detail = "An error occurred: bot doesn\'t exist")

    except HTTPException as e:
            raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")

@analytics_router.get('/total_bots_agent')
@x_super_team
@x_app_key
@jwt_token
async def total_bots_agent(request: Request):
    try:
        data = request.query_params

        required_fields = ['bot_id']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")
        
        bot_id = data.get('bot_id')

        company_id = request.headers.get('x-super-team')

        db = await connect()

        bots_collections = db['bots']
        workspaces_collections = db['workspace']
            
        bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 1})

        if bots_record:
            workspaces_records = await workspaces_collections.find({"company_id": company_id, "bot_id": bot_id, "is_active": 1}).to_list(length=None)
            
            slug = bots_record['bot_name'] + slug_db
            db = await connect(slug)
            messages_collections = db['messages']

            available_workspaces, total_workspaces = 0, 0
            for record in workspaces_records:

                total_workspaces += int(record['sessions_limit'])
                available_agents = int(record['sessions_limit'])

                message_records = await messages_collections.find({'workspace_id': record['workspace_id']}).to_list(length=None)
                for message in message_records:
                    timestamp_format = '%d/%m/%Y %H:%M:%S'
                    latest_timestamp = datetime.now()
                    expiration_time = datetime.strptime(message['latest_timestamp'], timestamp_format) + timedelta(minutes = int(message['timeout']))
                    
                    if not message['transfer_conversation']:
                        if not message['end_conversation'] and expiration_time > latest_timestamp:
                            available_agents -= 1

                available_workspaces += available_agents
                   
            return JSONResponse(content={"detail": {"total_agents": total_workspaces, "available_agents": available_workspaces}}, status_code = 200)
        else:
            raise HTTPException(status_code = 404, detail = "An error occurred: bot doesn\'t exist")
            
    except HTTPException as e:
            raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")

@analytics_router.get('/avg_bot_conversation_time')
@x_super_team
@x_app_key
@jwt_token
async def avg_bot_conversation_time(request: Request):
    try:
        data = request.query_params

        required_fields = ['bot_id', 'start_date', 'end_date']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")
        
        bot_id, start_date, end_date = data.get('bot_id'), data.get('start_date'), data.get('end_date')

        company_id = request.headers.get('x-super-team')

        db = await connect()

        bots_collections = db['bots']
            
        bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 1})

        if bots_record:
            slug = bots_record['bot_name'] + slug_db
            db = await connect(slug)

            messages_collections = db['messages']

            total, difference = 0, []

            message_records = await messages_collections.find({}).to_list(length = None)
            current_records, previous_records = filter_records(message_records, start_date, end_date)

            # print (len(previous_records), len(current_records))
            if len(previous_records) == 0:
                if len(current_records) == 0:
                    percentage = 0.0
                    detail = 0.0
                else:
                    difference, total = [], 0
                    for record in current_records:
                        timestamp_format = '%d/%m/%Y %H:%M:%S'
                        latest_timestamp = datetime.now()

                        expiration_time = datetime.strptime(record['latest_timestamp'], timestamp_format) + timedelta(minutes = int(record['timeout']))

                        if (record['end_conversation'] or record['transfer_conversation'] or record['human_intervention'] 
                            or expiration_time < latest_timestamp):
                            
                            
                            for role in record['roles']:
                                if role['type'] == 'human':
                                    first = role['timestamp']
                                    break
                            
                            # for role in record['roles'][::-1]:
                            #     if role['type'] == 'ai-agent':
                            #         last = role['timestamp']
                            #         break
                            last = record['roles'][-1]['timestamp']

                            total += 1

                            format_str = "%d/%m/%Y %H:%M:%S"
                            dt1 = datetime.strptime(first, format_str)
                            dt2 = datetime.strptime(last, format_str)

                            time_difference = dt2 - dt1
                            difference.append(time_difference.total_seconds() / 60)

                    
                    if total:
                        detail = round(sum(difference)/total, 2)
                    else:
                        detail = 0.0

                    percentage = 100.0
            else:
                if len(current_records) != 0:
                    difference, total = [], 0
                    for record in current_records:
                        timestamp_format = '%d/%m/%Y %H:%M:%S'
                        latest_timestamp = datetime.now()

                        expiration_time = datetime.strptime(record['latest_timestamp'], timestamp_format) + timedelta(minutes = int(record['timeout']))
                        if (record['end_conversation'] or record['transfer_conversation'] or record['human_intervention'] 
                            or expiration_time < latest_timestamp):
                       
                            for role in record['roles']:
                                if role['type'] == 'human':
                                    first = role['timestamp']
                                    break
                            
                            last = record['roles'][-1]['timestamp']
                            # for role in record['roles'][::-1]:
                            #     if role['type'] == 'ai-agent':
                            #         last = role['timestamp']
                            #         break

                            total += 1

                            format_str = "%d/%m/%Y %H:%M:%S"
                            dt1 = datetime.strptime(first, format_str)
                            dt2 = datetime.strptime(last, format_str)

                            time_difference = dt2 - dt1
                            difference.append(time_difference.total_seconds() / 60)

                    if total:
                        detail = round(sum(difference)/total, 2)
                    else: 
                        detail = 0.0
                else:
                    detail = 0

                difference, total = [], 0
                for record in previous_records:
                    timestamp_format = '%d/%m/%Y %H:%M:%S'
                    latest_timestamp = datetime.now()

                    expiration_time = datetime.strptime(record['latest_timestamp'], timestamp_format) + timedelta(minutes = int(record['timeout']))
                    if (record['end_conversation'] or record['transfer_conversation'] or record['human_intervention'] 
                        or expiration_time < latest_timestamp):
                        
                        for role in record['roles']:
                            if role['type'] == 'human':
                                first = role['timestamp']
                                break
                        
                        # for role in record['roles'][::-1]:
                        #     if role['type'] == 'ai-agent':
                        #         last = role['timestamp']
                        #         break
                        last = record['roles'][-1]['timestamp']
                        total += 1

                        format_str = "%d/%m/%Y %H:%M:%S"
                        dt1 = datetime.strptime(first, format_str)
                        dt2 = datetime.strptime(last, format_str)

                        time_difference = dt2 - dt1
                        difference.append(time_difference.total_seconds() / 60) 

                previous_difference = round(sum(difference)/total, 1)

                if detail and previous_difference:
                    percentage = round((detail - previous_difference) / previous_difference * 100, 1)
                elif detail:
                    percentage = 100.0
                elif previous_difference:
                    percentage = -100.0
                else:
                    percentage = 0

            return JSONResponse(content={"detail": detail, "percentage": percentage}, status_code = 200)
        else:
            raise HTTPException(status_code = 404, detail = "An error occurred: bot doesn\'t exist")
        
    except HTTPException as e:
            raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")

@analytics_router.get('/avg_wait_time')
@x_super_team
@x_app_key
@jwt_token
async def avg_wait_time(request: Request):
    try:
        data = request.query_params

        required_fields = ['bot_id', 'start_date', 'end_date']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")
        
        bot_id, start_date, end_date = data.get('bot_id'), data.get('start_date'), data.get('end_date')

        company_id = request.headers.get('x-super-team')

        db = await connect()

        bots_collections = db['bots']
            
        bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 1})

        if not bots_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: bot doesn\'t exist")

        if bots_record:
            slug = bots_record['bot_name'] + slug_db
            db = await connect(slug)

            messages_collections = db['messages']

            message_records = await messages_collections.find({}).to_list(length = None)
            current_records, previous_records = filter_records(message_records, start_date, end_date)

            if len(previous_records) == 0:
                if len(current_records) == 0:
                    percentage = 0.0
                    detail = 0.0
                else:
                    total, difference = 0, []
                    for record in current_records:
                        roles = [role for role in record['roles']]

                        transfer_timestamp = [role['timestamp'] for role in roles if role['type'] == 'human' and (
                            role['text'] == display_transfer_message_english or role['text'] == display_transfer_message_arabic)]

                        if transfer_timestamp:
                            connection_timestamp = [role['timestamp'] for role in roles if role['type'] == 'human-agent']

                            if connection_timestamp:

                                dt1 = str_to_datetime(transfer_timestamp[0])
                                dt2 = str_to_datetime(connection_timestamp[0])

                                time_difference = dt2 - dt1
                                difference.append(time_difference.total_seconds() / 60)
                                total += 1

                    if total:
                        detail = round(sum(difference)/total, 2)
                    else:
                        detail = 0.0
                    
                    percentage = 100.0
            else:
                total, difference = 0, []
                for record in current_records:
                    roles = [role for role in record['roles']]

                    transfer_timestamp = [role['timestamp'] for role in roles if role['type'] == 'human' and (
                        role['text'] == display_transfer_message_english or role['text'] == display_transfer_message_arabic)]

                    if transfer_timestamp:
                        connection_timestamp = [role['timestamp'] for role in roles if role['type'] == 'human-agent']

                        if connection_timestamp:

                            dt1 = str_to_datetime(transfer_timestamp[0])
                            dt2 = str_to_datetime(connection_timestamp[0])

                            time_difference = dt2 - dt1
                            difference.append(time_difference.total_seconds() / 60)
                            total += 1

                if total:
                    detail = round(sum(difference)/total, 2)
                else:
                    detail = 0.0
                
                total, difference = 0, []
                for record in previous_records:
                    roles = [role for role in record['roles']]

                    transfer_timestamp = [role['timestamp'] for role in roles if role['type'] == 'human' and (
                        role['text'] == display_transfer_message_english or role['text'] == display_transfer_message_arabic)]

                    if transfer_timestamp:
                        connection_timestamp = [role['timestamp'] for role in roles if role['type'] == 'human-agent']

                        if connection_timestamp:

                            dt1 = str_to_datetime(transfer_timestamp[0])
                            dt2 = str_to_datetime(connection_timestamp[0])

                            time_difference = dt2 - dt1
                            difference.append(time_difference.total_seconds() / 60)
                            total += 1

                if total:
                    previous_difference = round(sum(difference)/total, 1)
                else:
                    previous_difference = 0.0

                if detail and previous_difference:
                    percentage = round((detail - previous_difference) / previous_difference * 100, 2)
                elif detail:
                    percentage = 100.0
                elif previous_difference:
                    percentage = -100.0
                else:
                    percentage = 0

            return JSONResponse(content={"detail": detail, "percentage": percentage}, status_code = 200)
        else:
            raise HTTPException(status_code = 404, detail = "An error occurred: bot doesn\'t exist")
            
    except HTTPException as e:
            raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")

@analytics_router.get('/sentiments_analysis_bot_csat')
@x_super_team
@x_app_key
@jwt_token
async def sentiments_analysis_bot_csat(request: Request):
    try:
        data = request.query_params

        required_fields = ['bot_id', 'start_date', 'end_date']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")
        
        bot_id, start_date, end_date = data.get('bot_id'), data.get('start_date'), data.get('end_date')

        company_id = request.headers.get('x-super-team')

        db = await connect()

        bots_collections = db['bots']
            
        bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 1})

        if bots_record:
            slug = bots_record['bot_name'] + slug_db
            db = await connect(slug)

            messages_collections = db['messages']

            message_records = await messages_collections.find({}).to_list(length = None)
            current_records, previous_records = filter_records(message_records, start_date, end_date)

            if len(previous_records) == 0:
                if len(current_records) == 0:
                    neutral, positive, negative, interactions = 0, 0, 0, 0
                    neutral_percentage, positive_percentage, negative_percentage = 0.0, 0.0, 0.0
                else:
                    neutral, positive, negative, interactions = 0, 0, 0, 0

                    for message in current_records:
                        if message['sentiment'] == 'Neutral':
                            neutral += 1
                        elif message['sentiment'] == 'Positive':
                            positive += 1
                        elif message['sentiment'] == 'Negative':
                            negative += 1

                    interactions = neutral + positive + negative
                    
                    if neutral:
                        neutral_percentage = 100.0
                    else:
                        neutral_percentage = 0.0

                    if positive:
                        positive_percentage = 100.0
                    else:
                        positive_percentage = 0.0
                    
                    if negative:
                        negative_percentage = 100.0
                    else:
                        negative_percentage = 0.0
                        
            else:
                neutral, positive, negative = 0, 0, 0

                for message in current_records:
                    if message['sentiment'] == 'Neutral':
                        neutral += 1
                    elif message['sentiment'] == 'Positive':
                        positive += 1
                    elif message['sentiment'] == 'Negative':
                        negative += 1

                interactions = neutral + positive + negative

                neutral1, positive1, negative1 = 0, 0, 0

                for message in previous_records:
                    if message['sentiment'] == 'Neutral':
                        neutral1 += 1
                    elif message['sentiment'] == 'Positive':
                        positive1 += 1
                    elif message['sentiment'] == 'Negative':
                        negative1 += 1

                if neutral1 and neutral:
                    neutral_percentage = round((neutral - neutral1) / neutral1 * 100, 1)
                elif neutral1 and not neutral:
                    neutral_percentage = -100.0
                elif not neutral1 and neutral:
                    neutral_percentage = 100.0
                else:
                    neutral_percentage = 0

                if positive1 and positive:
                    positive_percentage = round((positive - positive1) / positive1 * 100, 1)
                elif positive1 and not positive:
                    positive_percentage = -100.0
                elif not positive1 and positive:
                    positive_percentage = 100.0
                else:
                    positive_percentage = 0

                if negative1 and negative:
                    negative_percentage = round((negative - negative1) / negative1 * 100, 1)
                elif negative1 and not negative:
                    negative_percentage = -100.0
                elif not negative1 and negative:
                    negative_percentage = 100.0
                else:
                    negative_percentage = 0

            return JSONResponse(content={
                "interactions": interactions, "positive": positive, "positive_percentage": positive_percentage, 
                "negative": negative, "negative_percentage": negative_percentage, "neutral": neutral, "neutral_percentage": neutral_percentage}, 
                status_code = 200)
        else:
            raise HTTPException(status_code = 404, detail = "An error occurred: bot doesn\'t exist")
                 
    except HTTPException as e:
            raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")

@analytics_router.get('/bot_csat')
@x_super_team
@x_app_key
@jwt_token
async def bot_csat(request: Request):
    try:
        data = request.query_params

        required_fields = ['bot_id', 'start_date', 'end_date']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")
        
        bot_id, start_date, end_date = data.get('bot_id'), data.get('start_date'), data.get('end_date')

        company_id = request.headers.get('x-super-team')

        db = await connect()

        bots_collections = db['bots']
            
        bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 1})

        if bots_record:
            slug = bots_record['bot_name'] + slug_db
            db = await connect(slug)

            csat_collections = db['csat']

            start_datetime = datetime.strptime(start_date, "%d/%m/%Y %H:%M:%S")
            end_datetime = datetime.strptime(end_date, "%d/%m/%Y %H:%M:%S")

            csat_records = await csat_collections.find({}).to_list(length=None)

            current_records = [
                msg for msg in csat_records
                if start_datetime <= str_to_datetime(msg['created_date']) < end_datetime
            ]

            if current_records:
                one, two, three, four, five = 0, 0, 0, 0, 0
                for record in current_records:
                    if record['score'] == '1':
                        one += 1
                    if record['score'] == '2':
                        two += 1
                    if record['score'] == '3':
                        three += 1
                    if record['score'] == '4':
                        four += 1
                    if record['score'] == '5':
                        five += 1
            else:
                one, two, three, four, five = 0, 0, 0, 0, 0

            return JSONResponse(content={"one": one, "two": two, "three": three, "four": four, "five": five}, status_code = 200)
        else:
            raise HTTPException(status_code = 404, detail = "An error occurred: bot doesn\'t exist")
                 
    except HTTPException as e:
            raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")

@analytics_router.get('/sentiments_analysis_agent_csat')
@x_super_team
@x_app_key
@jwt_token
async def sentiments_analysis_agent_csat(request: Request):
    try:
        data = request.query_params

        required_fields = ['bot_id', 'start_date', 'end_date']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")
        
        bot_id, start_date, end_date = data.get('bot_id'), data.get('start_date'), data.get('end_date')

        company_id = request.headers.get('x-super-team')

        db = await connect()

        bots_collections = db['bots']
            
        bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 1})

        if bots_record:
            slug = bots_record['bot_name'] + slug_db
            db = await connect(slug)

            messages_collections = db['messages']

            message_records = await messages_collections.find({}).to_list(length=None)

            message_records = await messages_collections.find({}).to_list(length = None)
            current_records, previous_records = filter_records(message_records, start_date, end_date)

            if len(previous_records) == 0:
                if len(current_records) == 0:
                    neutral, positive, negative, interactions = 0, 0, 0, 0
                    neutral_percentage, positive_percentage, negative_percentage = 0.0, 0.0, 0.0
                else:
                    neutral, positive, negative, interactions = 0, 0, 0, 0

                    for message in current_records:
                        try:
                            if message['agent_sentiment'] == 'Neutral':
                                neutral += 1
                            elif message['agent_sentiment'] == 'Positive':
                                positive += 1
                            elif message['agent_sentiment'] == 'Negative':
                                negative += 1
                        except:
                            pass

                    interactions = neutral + positive + negative
                    
                    if neutral:
                        neutral_percentage = 100.0
                    else:
                        neutral_percentage = 0.0

                    if positive:
                        positive_percentage = 100.0
                    else:
                        positive_percentage = 0.0
                    
                    if negative:
                        negative_percentage = 100.0
                    else:
                        negative_percentage = 0.0
                        
            else:
                neutral, positive, negative = 0, 0, 0

                for message in current_records:
                    try:
                        if message['agent_sentiment'] == 'Neutral':
                            neutral += 1
                        elif message['agent_sentiment'] == 'Positive':
                            positive += 1
                        elif message['agent_sentiment'] == 'Negative':
                            negative += 1
                    except:
                        pass

                interactions = neutral + positive + negative

                neutral1, positive1, negative1 = 0, 0, 0

                for message in previous_records:
                    try:
                        if message['agent_sentiment'] == 'Neutral':
                            neutral += 1
                        elif message['agent_sentiment'] == 'Positive':
                            positive += 1
                        elif message['agent_sentiment'] == 'Negative':
                            negative += 1
                    except:
                        pass

                if neutral1 and neutral:
                    neutral_percentage = round((neutral - neutral1) / neutral1 * 100, 1)
                elif neutral1 and not neutral:
                    neutral_percentage = -100.0
                elif not neutral1 and neutral:
                    neutral_percentage = 100.0
                else:
                    neutral_percentage = 0

                if positive1 and positive:
                    positive_percentage = round((positive - positive1) / positive1 * 100, 1)
                elif positive1 and not positive:
                    positive_percentage = -100.0
                elif not positive1 and positive:
                    positive_percentage = 100.0
                else:
                    positive_percentage = 0

                if negative1 and negative:
                    negative_percentage = round((negative - negative1) / negative1 * 100, 1)
                elif negative1 and not negative:
                    negative_percentage = -100.0
                elif not negative1 and negative:
                    negative_percentage = 100.0
                else:
                    negative_percentage = 0

            return JSONResponse(content={
                "interactions": interactions, "positive": positive, "positive_percentage": positive_percentage, "negative": negative, 
                "negative_percentage": negative_percentage, "neutral": neutral, "neutral_percentage": neutral_percentage}, 
                status_code = 200)
        else:
            raise HTTPException(status_code = 404, detail = "An error occurred: bot doesn\'t exist")
                 
    except HTTPException as e:
            raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")

@analytics_router.get('/tags_analytics')
@x_super_team
@x_app_key
@jwt_token
async def tags_analytics(request: Request):
    try:
        data = request.query_params

        required_fields = ['bot_id', 'start_date', 'end_date']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")
        
        bot_id, start_date, end_date = data.get('bot_id'), data.get('start_date'), data.get('end_date')

        company_id = request.headers.get('x-super-team')

        db = await connect()

        bots_collections = db['bots']
            
        bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 1})

        if bots_record:
            slug = bots_record['bot_name'] + slug_db
            db = await connect(slug)

            messages_collections = db['messages']

            message_records = await messages_collections.find({}).to_list(length=None)

            start_datetime = datetime.strptime(start_date, "%d/%m/%Y %H:%M:%S")
            end_datetime = datetime.strptime(end_date, "%d/%m/%Y %H:%M:%S")

            filtered_records = [
                msg for msg in message_records
                if start_datetime <= str_to_datetime(msg['latest_timestamp']) < end_datetime
            ]

            information, investor, creditor, sql, customer_support = 0, 0, 0, 0, 0

            if filtered_records:
                for message in filtered_records:
                    if message['tags']: 
                        for tag in message['tags']:
                            if tag == 'information':
                                information += 1
                            elif tag == 'investor':
                                investor += 1
                            elif tag == 'creditor':
                                creditor += 1
                            elif tag == 'sql':
                                sql += 1
                            elif tag == 'customer_support':
                                customer_support += 1

            return JSONResponse(content={"detail": {
                'Information': information, 'Investor': investor, 'Creditor': creditor, 'SQL': sql, 'Customer_Support': customer_support}}, 
                status_code = 200)
        else:
            raise HTTPException(status_code = 404, detail = "An error occurred: bot doesn\'t exist")
    
    except HTTPException as e:
            raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")

@analytics_router.get('/total_tokens')
@x_super_team
@x_app_key
@jwt_token
async def total_tokens(request: Request):
    try:
        data = request.query_params

        required_fields = ['bot_id', 'start_date', 'end_date']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")
        
        bot_id, start_date, end_date = data.get('bot_id'), data.get('start_date'), data.get('end_date')

        company_id = request.headers.get('x-super-team')

        db = await connect()

        bots_collections = db['bots']
            
        bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 1})

        if bots_record:
            slug = bots_record['bot_name'] + slug_db
            db = await connect(slug)

            messages_collections = db['messages']

            message_records = await messages_collections.find({}).to_list(length=None)

            start_datetime = datetime.strptime(start_date, "%d/%m/%Y %H:%M:%S")
            end_datetime = datetime.strptime(end_date, "%d/%m/%Y %H:%M:%S")

            filtered_records = [
                msg for msg in message_records
                if start_datetime <= str_to_datetime(msg['latest_timestamp']) < end_datetime
            ]

            if filtered_records:       
                output_tokens = [role['output_tokens'] for record in filtered_records for role in record['roles'] if role['type'] == 'ai-agent']

                output_tokens = []
                for record in filtered_records:
                    for role in record['roles']:
                        if role['type'] == 'ai-agent':
                            if role['output_tokens']:
                                output_tokens.append(role['output_tokens'])
                            else:
                                output_tokens.append(0)                    
                
                input_tokens = []
                for record in filtered_records:
                    for role in record['roles']:
                        if role['type'] == 'human':
                            if role['input_tokens']:
                                input_tokens.append(role['input_tokens'])
                            else:
                                input_tokens.append(0)

                return JSONResponse(content={"detail": sum(output_tokens)+sum(input_tokens)}, status_code = 200)
            else:
                return JSONResponse(content={"detail": 0}, status_code = 200)
        else:
            raise HTTPException(status_code = 404, detail = "An error occurred: bot doesn\'t exist")
    
    except HTTPException as e:
            raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")

@analytics_router.get('/average_tokens_per_conversation')
@x_super_team
@x_app_key
@jwt_token
async def average_tokens_per_conversation(request: Request):
    try:
        data = request.query_params

        required_fields = ['bot_id', 'start_date', 'end_date']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")
        
        bot_id, start_date, end_date = data.get('bot_id'), data.get('start_date'), data.get('end_date')

        company_id = request.headers.get('x-super-team')

        db = await connect()

        bots_collections = db['bots']
            
        bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 1})

        if bots_record:
            slug = bots_record['bot_name'] + slug_db
            db = await connect(slug)

            messages_collections = db['messages']

            message_records = await messages_collections.find({}).to_list(length=None)

            start_datetime = datetime.strptime(start_date, "%d/%m/%Y %H:%M:%S")
            end_datetime = datetime.strptime(end_date, "%d/%m/%Y %H:%M:%S")

            filtered_records = [
                msg for msg in message_records
                if start_datetime <= str_to_datetime(msg['latest_timestamp']) < end_datetime
            ]

            if filtered_records:
                output_tokens = []
                for record in filtered_records:
                    for role in record['roles']:
                        if role['type'] == 'ai-agent':
                            if role['output_tokens']:
                                output_tokens.append(role['output_tokens'])
                            else:
                                output_tokens.append(0)                    
                
                input_tokens = []
                for record in filtered_records:
                    for role in record['roles']:
                        if role['type'] == 'human':
                            if role['input_tokens']:
                                input_tokens.append(role['input_tokens'])
                            else:
                                input_tokens.append(0)

                return JSONResponse(content={"detail": round((sum(output_tokens)+sum(input_tokens))/len(filtered_records), 2)}, status_code = 200)
            else:
                return JSONResponse(content={"detail": 0}, status_code = 200) 
        else:
            raise HTTPException(status_code = 404, detail = "An error occurred: bot doesn\'t exist")
    
    except HTTPException as e:
            raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")

@analytics_router.get('/tokens_per_day')
@x_super_team
@x_app_key
@jwt_token
async def tokens_per_day(request: Request):
    try:
        data = request.query_params

        required_fields = ['bot_id', 'start_date', 'end_date']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")
        
        bot_id, start_date, end_date = data.get('bot_id'), data.get('start_date'), data.get('end_date')

        company_id = request.headers.get('x-super-team')

        db = await connect()

        bots_collections = db['bots']
            
        bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 1})

        if bots_record:
            slug = bots_record['bot_name'] + slug_db
            db = await connect(slug)

            messages_collections = db['messages']

            message_records = await messages_collections.find({}).to_list(length=None)

            start_datetime = datetime.strptime(start_date, "%d/%m/%Y %H:%M:%S")
            end_datetime = datetime.strptime(end_date, "%d/%m/%Y %H:%M:%S")

            # difference_in_days = abs((end_datetime - start_datetime).days)
            difference_in_days = abs((end_datetime - start_datetime).days)
            if not difference_in_days:
                difference_in_days = 1

            filtered_records = [
                msg for msg in message_records
                if start_datetime <= str_to_datetime(msg['latest_timestamp']) < end_datetime
            ]

            if filtered_records:       
                output_tokens = []
                for record in filtered_records:
                    for role in record['roles']:
                        if role['type'] == 'ai-agent':
                            if role['output_tokens']:
                                output_tokens.append(role['output_tokens'])
                            else:
                                output_tokens.append(0)                    
                
                input_tokens = []
                for record in filtered_records:
                    for role in record['roles']:
                        if role['type'] == 'human':
                            if role['input_tokens']:
                                input_tokens.append(role['input_tokens'])
                            else:
                                input_tokens.append(0)

                return JSONResponse(content={"detail": round(sum(output_tokens)+sum(input_tokens) / difference_in_days, 2)}, status_code = 200)
            else:
                return JSONResponse(content={"detail": 0}, status_code = 200)
        else:
            raise HTTPException(status_code = 404, detail = "An error occurred: bot doesn\'t exist")
    
    except HTTPException as e:
            raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")        

@analytics_router.get('/session_time_out')
@x_super_team
@x_app_key
@jwt_token
async def session_time_out(request: Request):
    try:
        data = request.query_params

        required_fields = ['bot_id', 'start_date', 'end_date']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")
        
        bot_id, start_date, end_date = data.get('bot_id'), data.get('start_date'), data.get('end_date')

        company_id = request.headers.get('x-super-team')

        db = await connect()

        bots_collections = db['bots']
            
        bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 1})

        if bots_record:
            slug = bots_record['bot_name'] + slug_db
            db = await connect(slug)

            messages_collections = db['messages']

            message_records = await messages_collections.find({}).to_list(length=None)

            message_records = await messages_collections.find({}).to_list(length = None)
            current_records, previous_records = filter_records(message_records, start_date, end_date)

            if len(previous_records) == 0:
                if len(current_records) == 0:
                    percentage = 0.0
                    detail = 0
                else:
                    percentage = 100.0
                    detail = 0
                    for record in current_records:
                        timestamp_format = '%d/%m/%Y %H:%M:%S'
                        latest_timestamp = datetime.now()
                        expiration_time = datetime.strptime(record['latest_timestamp'], timestamp_format) + timedelta(minutes = int(record['timeout']))
                        
                        if (expiration_time < latest_timestamp and not record['end_conversation'] and not record['transfer_conversation'] 
                            and not record['human_intervention'] and not record['agent_expiry']):
                            detail += 1       
            else:
                detail = 0
                for record in current_records:
                    timestamp_format = '%d/%m/%Y %H:%M:%S'
                    latest_timestamp = datetime.now()
                    expiration_time = datetime.strptime(record['latest_timestamp'], timestamp_format) + timedelta(minutes = int(record['timeout']))
                    
                    if (expiration_time < latest_timestamp and not record['end_conversation'] and not record['transfer_conversation'] 
                        and not record['human_intervention'] and not record['agent_expiry']):

                        detail += 1

                previous_difference = 0
                for record in previous_records:
                    timestamp_format = '%d/%m/%Y %H:%M:%S'
                    latest_timestamp = datetime.now()
                    expiration_time = datetime.strptime(record['latest_timestamp'], timestamp_format) + timedelta(minutes = int(record['timeout']))
                    
                    if (expiration_time < latest_timestamp and not record['end_conversation'] and not record['transfer_conversation'] 
                        and not record['human_intervention'] and not record['agent_expiry']):

                        previous_difference += 1

                if detail and previous_difference:
                    percentage = round((detail - previous_difference) / previous_difference * 100, 1)
                elif detail and not previous_difference:
                    percentage = 100.0
                elif not detail and previous_difference:
                    percentage = -100.0
                else:
                    percentage = 0.0

            return JSONResponse(content={"detail": detail, "percentage": percentage}, status_code = 200)
        else:
            raise HTTPException(status_code = 404, detail = "An error occurred: bot doesn\'t exist")
    
    except HTTPException as e:
            raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")

@analytics_router.get('/bot_escalation_rate')
@x_super_team
@x_app_key
@jwt_token
async def bot_escalation_rate(request: Request):
    try:
        data = request.query_params

        required_fields = ['bot_id', 'start_date', 'end_date']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")
        
        bot_id, start_date, end_date = data.get('bot_id'), data.get('start_date'), data.get('end_date')

        company_id = request.headers.get('x-super-team')

        db = await connect()

        bots_collections = db['bots']
            
        bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 1})

        if bots_record:
            slug = bots_record['bot_name'] + slug_db
            db = await connect(slug)

            messages_collections = db['messages']

            message_records = await messages_collections.find({}).to_list(length=None)

            message_records = await messages_collections.find({}).to_list(length = None)
            current_records, previous_records = filter_records(message_records, start_date, end_date)

            if len(previous_records) == 0:
                if len(current_records) == 0:
                    percentage = 0.0
                    detail = 0.0
                else:
                    total = [record for record in current_records]
                    transfer_conversations = [record for record in current_records if record['transfer_conversation']]

                    detail = round((len(transfer_conversations)/len(total))*100, 2)
                    percentage = 100.0
            else:
                total = [record for record in current_records]
                transfer_conversations = [record for record in current_records if record['transfer_conversation']]

                if total:
                    detail = round((len(transfer_conversations)/len(total))*100, 2)
                else:
                    detail = 0.0

                total = [record for record in previous_records]
                transfer_conversations = [record for record in previous_records if record['transfer_conversation']]

                if total:
                    previous_difference = round((len(transfer_conversations)/len(total))*100, 1)
                else:
                    previous_difference = 0.0

                if detail and previous_difference:
                    percentage = round((detail - previous_difference) / previous_difference * 100, 2)
                elif detail and not previous_difference:
                    percentage = 100.0
                elif previous_difference and not detail:
                    percentage = -100.0
                else:
                    percentage = 0.0

            return JSONResponse(content={"detail": detail, "percentage": percentage}, status_code = 200)
        else:
            raise HTTPException(status_code = 404, detail = "An error occurred: bot doesn\'t exist")
    
    except HTTPException as e:
            raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")

@analytics_router.get('/conversations_over_time')
@x_super_team
@x_app_key
@jwt_token
async def conversations_over_time(request: Request):
    try:
        data = request.query_params

        required_fields = ['bot_id', 'start_date', 'end_date']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code=400, detail="An error occurred: missing parameter(s)")

        bot_id = data.get('bot_id')
        start_date_str = data.get('start_date')
        end_date_str = data.get('end_date')

        # Parse dates
        start_datetime = datetime.strptime(start_date_str, "%d/%m/%Y %H:%M:%S")
        end_datetime = datetime.strptime(end_date_str, "%d/%m/%Y %H:%M:%S")

        db = await connect()

        bots_collections = db['bots']
        bots_record = await bots_collections.find_one({"company_id": request.headers.get('x-super-team'), "bot_id": bot_id, "is_active": 1})

        if not bots_record:
            raise HTTPException(status_code=404, detail="An error occurred: bot doesn't exist")

        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)
        messages_collections = db['messages']

        message_records = await messages_collections.find({}).to_list(length=None)

        # Filter messages by timestamp
        filtered_records = [
            msg for msg in message_records
            if start_datetime <= str_to_datetime(msg['latest_timestamp']) <= end_datetime
        ]

        # Initialize daily conversations
        daily_conversations = defaultdict(int)
        current_date = start_datetime
        while current_date <= end_datetime:
            date_str = current_date.strftime('%Y-%m-%d')
            daily_conversations[date_str] = 0
            current_date += timedelta(days=1)

        # Aggregate filtered records
        for record in filtered_records:
            timestamp = str_to_datetime(record['latest_timestamp'])
            date_str = timestamp.strftime('%Y-%m-%d')
            daily_conversations[date_str] += 1

        return JSONResponse(content={"detail": dict(daily_conversations)}, status_code=200)

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@analytics_router.get('/human_transfer_rate')
@x_super_team
@x_app_key
@jwt_token
async def human_transfer_rate(request: Request):
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

        db = await connect()
        bots_collections = db['bots']
        bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 1})

        if bots_record:
            slug = bots_record['bot_name'] + slug_db
            db = await connect(slug)

            messages_collections = db['messages']
            message_records = await messages_collections.find({}).to_list(length=None)

            # Initialize daily conversation stats
            daily_conversations = defaultdict(lambda: 0)

            # Iterate through the date range to initialize empty stats
            current_date = start_datetime
            while current_date <= end_datetime:
                date_str = current_date.strftime('%Y-%m-%d')
                daily_conversations[date_str] = 0
                current_date += timedelta(days=1)

            # Filter records within the given date range
            filtered_records = [
                msg for msg in message_records if start_datetime <= str_to_datetime(msg['latest_timestamp']) <= end_datetime
            ]

            if filtered_records:
                timestamps = []
                for record in filtered_records:
                    if record['transfer_conversation']:
                        roles = [role for role in record['roles']]
                        timestamps.append(roles[0]['timestamp'])

                # Calculate the number of transfers per day
                format_str = "%d/%m/%Y %H:%M:%S"
                for timestamp in timestamps:
                    dt = datetime.strptime(timestamp, format_str)
                    date_str = dt.strftime('%Y-%m-%d')
                    daily_conversations[date_str] += 1

            return JSONResponse(content={"detail": daily_conversations}, status_code=200)
        else:
            raise HTTPException(status_code=404, detail="An error occurred: bot doesn't exist")

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@analytics_router.get('/average_token_per_chat')
@x_super_team
@x_app_key
@jwt_token
async def average_token_per_chat(request: Request):
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

        db = await connect()
        bots_collections = db['bots']
        bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 1})

        if bots_record:
            slug = bots_record['bot_name'] + slug_db
            db = await connect(slug)

            messages_collections = db['messages']
            message_records = await messages_collections.find({}).to_list(length=None)

            # Initialize daily conversation stats
            daily_conversations = defaultdict(lambda: {'total_tokens': 0, 'chat_count': 0})

            # Iterate through the date range to initialize empty stats
            current_date = start_datetime
            while current_date <= end_datetime:
                date_str = current_date.strftime('%Y-%m-%d')
                daily_conversations[date_str] = {'total_tokens': 0, 'chat_count': 0}
                current_date += timedelta(days=1)

            # Filter records within the given date range
            filtered_records = [
                msg for msg in message_records if start_datetime <= str_to_datetime(msg['latest_timestamp']) <= end_datetime
            ]

            if filtered_records:
                for record in filtered_records:
                    total_tokens = 0
                    for role in record['roles']:
                        try:
                            total_tokens += role.get('input_tokens', 0) + role.get('output_tokens', 0)
                        except:
                            pass

                    if total_tokens > 0:
                        timestamp = str_to_datetime(record['roles'][0]['timestamp'])
                        date_str = timestamp.strftime('%Y-%m-%d')
                        daily_conversations[date_str]['total_tokens'] += total_tokens
                        daily_conversations[date_str]['chat_count'] += 1

            # Calculate average tokens per chat per day
            average_tokens_per_day = {}
            for date_str, data in daily_conversations.items():
                if data['chat_count'] > 0:
                    avg_tokens = data['total_tokens'] / data['chat_count']
                else:
                    avg_tokens = 0  
                average_tokens_per_day[date_str] = round(avg_tokens, 2)

            return JSONResponse(content={"detail": average_tokens_per_day}, status_code=200)
        else:
            raise HTTPException(status_code=404, detail="An error occurred: bot doesn't exist")

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@analytics_router.get('/total_bot_sessions_over_time')
@x_super_team
@x_app_key
@jwt_token
async def total_bot_sessions_over_time(request: Request):
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

        db = await connect()
        bots_collections = db['bots']
        bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 1})

        if bots_record:
            slug = bots_record['bot_name'] + slug_db
            db = await connect(slug)

            messages_collections = db['messages']
            message_records = await messages_collections.find({}).to_list(length=None)

            # Initialize daily conversation stats
            daily_conversations = defaultdict(lambda: {
                'human_transfer_rate': 0.0,
                'session_timeout': 0.0,
                'avg_handle_time': 0.0
            })

            # Iterate through the date range to initialize empty stats
            current_date = start_datetime
            while current_date <= end_datetime:
                date_str = current_date.strftime('%Y-%m-%d')
                daily_conversations[date_str] = {
                    'human_transfer_rate': 0.0,
                    'session_timeout': 0.0,
                    'avg_handle_time': 0.0
                }
                current_date += timedelta(days=1)

            # Filter records within the given date range
            filtered_records = [
                msg for msg in message_records if start_datetime <= str_to_datetime(msg['latest_timestamp']) <= end_datetime
            ]

            if filtered_records:
                daily_total_records = defaultdict(int)
                transfer_counts = defaultdict(int)

                # Count transfers and messages per day
                for record in filtered_records:
                    roles = record['roles']
                    timestamp = roles[0]['timestamp']
                    dt = str_to_datetime(timestamp)
                    date_str = dt.strftime('%Y-%m-%d')

                    daily_total_records[date_str] += 1

                    if record.get('transfer_conversation'):
                        transfer_counts[date_str] += 1

                # Calculate human transfer rate
                for date_str, transfer_count in transfer_counts.items():
                    total_records_on_date = daily_total_records[date_str]
                    if total_records_on_date > 0:
                        daily_conversations[date_str]['human_transfer_rate'] = round((transfer_count / total_records_on_date) * 100, 2)

                # Count timeouts and session stats
                daily_total_records = defaultdict(int)
                timeout_counts = defaultdict(int)

                for record in filtered_records:
                    roles = record['roles']
                    timestamp = roles[0]['timestamp']
                    dt = str_to_datetime(timestamp)
                    date_str = dt.strftime('%Y-%m-%d')

                    daily_total_records[date_str] += 1

                    timestamp_format = '%d/%m/%Y %H:%M:%S'
                    latest_timestamp = datetime.now()
                    expiration_time = datetime.strptime(record['latest_timestamp'], timestamp_format) + timedelta(minutes=int(record['timeout']))

                    if (expiration_time < latest_timestamp and not record['end_conversation'] and not record['transfer_conversation'] 
                        and not record['human_intervention'] and not record['agent_expiry']):
                        timeout_counts[date_str] += 1

                # Calculate session timeout rate
                for date_str, timeout_count in timeout_counts.items():
                    total_records_on_date = daily_total_records[date_str]
                    if total_records_on_date > 0:
                        daily_conversations[date_str]['session_timeout'] = round((timeout_count / total_records_on_date) * 100, 2)

                # Calculate average handle time (waiting time between transfer and agent connection)
                daily_wait_times = defaultdict(list)

                for record in filtered_records:
                    roles = record['roles']
                    transfer_timestamp = [role['timestamp'] for role in roles if role['type'] == 'human' and 
                                        (role['text'] == display_transfer_message_english or role['text'] == display_transfer_message_arabic)]

                    if transfer_timestamp:
                        connection_timestamp = [role['timestamp'] for role in roles if role['type'] == 'human-agent']
                        if connection_timestamp:
                            dt1 = str_to_datetime(transfer_timestamp[0])
                            dt2 = str_to_datetime(connection_timestamp[0])
                            time_difference = (dt2 - dt1).total_seconds() / 60

                            date_str = dt1.strftime('%Y-%m-%d')
                            daily_wait_times[date_str].append(time_difference)

                # Calculate average handle time per day
                for date_str, times in daily_wait_times.items():
                    if times:
                        avg_time = sum(times) / len(times)
                        daily_conversations[date_str]['avg_handle_time'] = round(avg_time, 2)

            return JSONResponse(content={"detail": daily_conversations}, status_code=200)

        else:
            raise HTTPException(status_code=404, detail="An error occurred: bot doesn't exist")

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@analytics_router.get('/peak_hours')
@x_super_team
@x_app_key
@jwt_token
async def peak_hours(request: Request):
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

        db = await connect()
        bots_collections = db['bots']
        bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 1})

        if bots_record:
            slug = bots_record['bot_name'] + slug_db
            db = await connect(slug)

            messages_collections = db['messages']
            message_records = await messages_collections.find({}).to_list(length=None)

            # Initialize peak hours schedule (hourly count for each day in range)
            week_schedule = {}
            current_date = start_datetime

            # Initialize schedule for each date in the range
            while current_date <= end_datetime:
                date_str = current_date.strftime('%Y-%m-%d')
                week_schedule[date_str] = [0] * 24
                current_date += timedelta(days=1)

            # Filter records based on start and end date
            filtered_records = [
                msg for msg in message_records if start_datetime <= str_to_datetime(msg['latest_timestamp']) <= end_datetime
            ]

            # Extract timestamps and count occurrences per hour
            timestamps = []
            for record in filtered_records:
                roles = [role for role in record['roles']]
                timestamps.append(roles[0]['timestamp'])

            # Count messages per hour for each day
            format_str = "%d/%m/%Y %H:%M:%S"
            for timestamp in timestamps:
                dt = datetime.strptime(timestamp, format_str)
                date_str = dt.strftime('%Y-%m-%d')
                hour = dt.hour
                if date_str in week_schedule:
                    week_schedule[date_str][hour] += 1

            return JSONResponse(content={"detail": week_schedule}, status_code=200)
        else:
            raise HTTPException(status_code=404, detail="An error occurred: bot doesn't exist")

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
