from pymongo import DESCENDING, ASCENDING
from math import ceil
from decouple import config
from datetime import datetime, timedelta
from fastapi import HTTPException

from utilities.database import connect
from utilities.redis import view_queue

slug_db = config("SLUG_DATABASE")

display_human_takeover_message_english = config("DISPLAY_HUMAN_TAKEOVER_MESSAGE_ENGLISH")
display_human_takeover_message_arabic = config("DISPLAY_HUMAN_TAKEOVER_MESSAGE_ARABIC")

human_agent_end_message = config("HUMAN_AGENT_END_MESSAGE")

transfer_queue = config("TRANSFER_QUEUE")

timestamp_format = '%d/%m/%Y %H:%M:%S'

async def active_sessions(
    bots_record, workspace_record, configuration_record,
    bot_display, agent_display, queue_display, takeover_display, limit, page, agent_id
):

    try:        
        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)

        messages_collections = db['messages']
        profiles_collections = db['profiles']
        
        if configuration_record['auto_assignment']:
            temp = await view_queue(transfer_queue)

        message_records = await messages_collections.find({"workspace_id": workspace_record['workspace_id']}).to_list(length=None)

        chats_history = []
        for record in message_records:
            latest_timestamp = datetime.now()

            profiles_record = await profiles_collections.find_one({'session_id': record['session_id']})

            sentiments = {'Neutral': 0, 'Positive': 0, 'Negative': 0}
            for score in record['roles']:
                if score['type'] == 'human':
                    try:
                        if score['sentiment'] == 'Neutral':
                            sentiments['Neutral'] += 1
                        elif score['sentiment'] == 'Positive':
                            sentiments['Positive'] += 1
                        elif score['sentiment'] == 'Negative':
                            sentiments['Negative'] += 1
                    except:
                        pass

            username = profiles_record['username']
            email = profiles_record['email']

            try:
                phone = profiles_record['phone']
            except:
                phone = None

            expiration_time = datetime.strptime(record['latest_timestamp'], timestamp_format) + timedelta(minutes = int(record['timeout']))
            if int(bot_display) and (expiration_time > latest_timestamp and not record['end_conversation'] 
                    and not record['transfer_conversation'] and not record['human_intervention']):

                chats_history.append({
                    'session_id': record['session_id'], 'username': username, 'email': email, 'phone': phone,
                    'last_message': record['roles'][-1]['text'], 'last_timestamp': record['roles'][-1]['timestamp'], 
                    'sentiment': sentiments, 'status': 'BOT'
                })

            if int(agent_display) and (record['transfer_conversation'] and not record['end_conversation']):
                for role in record['roles'][::-1]:
                    if role['type'] == 'human-agent':
                        if role['agent_id'] == agent_id:
                            chats_history.append({
                                'session_id': record['session_id'], 'username': username, 'email': email, 'phone': phone, 
                                'last_message': record['roles'][-1]['text'], 'last_timestamp': record['roles'][-1]['timestamp'], 
                                'sentiment': sentiments, 'status': 'AGENT'})
                            
                            break
            
            if int(queue_display):
                if not configuration_record['auto_assignment']:
                    if record['transfer_conversation'] and not record['end_conversation']:
                        agent_assigned = 0
                        for role in record['roles'][::-1]:
                            if role['type'] == 'human-agent':
                                agent_assigned = 1
                                break

                        if agent_assigned == 0: 
                            chats_history.append({
                                'session_id': record['session_id'], 'username': username, 'email': email, 'phone': phone,
                                'last_message': record['roles'][-1]['text'], 'last_timestamp': record['roles'][-1]['timestamp'], 
                                'sentiment': sentiments, 'status': 'IN QUEUE'
                            })
                else:
                    if record['session_id'] in temp:
                        chats_history.append({
                            'session_id': record['session_id'], 'username': username, 'email': email, 'phone': phone, 
                            'last_message': record['roles'][-1]['text'], 'last_timestamp': record['roles'][-1]['timestamp'], 
                            'sentiment': sentiments, 'status': 'IN QUEUE'
                        })

            if int(takeover_display) and (record['human_intervention'] and not record['end_conversation'] and not record['transfer_conversation']):
                for role in record['roles']:
                    if role['text'] == display_human_takeover_message_arabic or role['text'] == display_human_takeover_message_english:
                        if role['agent_id'] == agent_id:
                            chats_history.append({
                                'session_id': record['session_id'], 'username': username, 'email': email, 'phone': phone, 
                                'last_message': record['roles'][-1]['text'], 'last_timestamp': record['roles'][-1]['timestamp'], 
                                'sentiment': sentiments, 'status': 'TAKEOVER'
                            })
        
        start = (page - 1) * limit  
        end = start + limit 

        paginated_items = chats_history[start:end]
        total_items = len(chats_history)
        total_pages = ceil(total_items / limit)

        pagination_metadata = {
            "total_items": total_items,
            "total_pages": total_pages,
            "current_page": page,
            "limit": limit
        }
        
        return paginated_items, pagination_metadata
            
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")
    
async def inactive_sessions(
    bots_record, workspace_record,  
    bot_display, agent_display, limit, page,
    start_date_filter, end_date_filter, sentiment_filter, sort_filter, email_filter
):
    
    try:        
        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)

        messages_collections = db['messages']
        profiles_collections = db['profiles']

        date_format = "%d/%m/%Y"

        if start_date_filter and end_date_filter:
            try:
                start_of_day = datetime.strptime(start_date_filter, date_format).replace(hour=0, minute=0, second=0, microsecond=0)
                end_of_day = datetime.strptime(end_date_filter, date_format).replace(hour=23, minute=59, second=59, microsecond=999999) 

                if start_of_day > end_of_day:
                    raise HTTPException(status_code = 400, detail = "Start date cannot be after end date.")
            except ValueError:
                raise HTTPException(status_code = 400, detail="Invalid date format. Use 'DD/MM/YYYY'.")

        query = {"workspace_id": workspace_record['workspace_id']}

        if sentiment_filter:
            valid_sentiments = ['positive', 'negative', 'neutral']
            selected_sentiments = [s.capitalize() for s in sentiment_filter if s in valid_sentiments]
            if selected_sentiments:  
                query['sentiment'] = {"$in": selected_sentiments}

        sort_order = DESCENDING if sort_filter == 'newest' else ASCENDING if sort_filter == 'oldest' else DESCENDING

        message_records = (
            await messages_collections.find(query).sort("_id", sort_order).to_list(length=None)
        )

        chats_history = []
        for record in message_records:
            record_timestamp = datetime.strptime(record['latest_timestamp'], "%d/%m/%Y %H:%M:%S")

            if start_date_filter and end_date_filter:
                if not (start_of_day <= record_timestamp <= end_of_day):
                    continue

            latest_timestamp = datetime.now()
            expiration_time = datetime.strptime(record['latest_timestamp'], timestamp_format) + timedelta(minutes = int(record['timeout']))
            
            if expiration_time < latest_timestamp or record['end_conversation']:
                if not email_filter:
                    profiles_record = await profiles_collections.find_one({'session_id': record['session_id']})
                else:
                    profiles_record = await profiles_collections.find_one({'session_id': record['session_id'], 'email': email_filter})

                if not profiles_record:
                    continue

                username = profiles_record['username']
                email = profiles_record['email']

                try:
                    phone = profiles_record['phone']
                except:
                    phone = None

                if record['transfer_conversation'] or record['human_intervention']:
                    if record['end_conversation'] and int(agent_display):
                        chats_history.append({
                            'session_id': record['session_id'], 'username': username, 'email': email, 'phone': phone, 
                            'last_message': record['roles'][-1]['text'], 'last_timestamp': record['roles'][-1]['timestamp'], 
                            'conversation_sentiment': record['sentiment'], 'status': 'AGENT'
                        })
                else:
                    if int(bot_display):
                        chats_history.append({
                            'session_id': record['session_id'], 'username': username, 'email': email, 'phone': phone,
                            'last_message': record['roles'][-1]['text'], 'last_timestamp': record['roles'][-1]['timestamp'], 
                            'conversation_sentiment': record['sentiment'], 'status': 'BOT'
                        })
        
        start = (page - 1) * limit  
        end = start + limit 

        paginated_items = chats_history[start:end]
        total_items = len(chats_history)
        total_pages = ceil(total_items / limit)

        pagination_metadata = {
            "total_items": total_items,
            "total_pages": total_pages,
            "current_page": page,
            "limit": limit
        }
        
        return paginated_items, pagination_metadata
        
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")
    
async def session_details(
    bots_record, workspace_record, session_id, start_date, end_date        
):
    
    try:     
        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)

        messages_collections = db['messages']
        profiles_collections = db['profiles']
        summaries_collections = db['summary']
        csat_collections = db['csat']

        summaries_record = await summaries_collections.find_one({"session_id": session_id})

        if not summaries_record:
            summary = None
            suggestions = None
        else:
            try:
                summary = summaries_record['summary']
                suggestions = summaries_record['suggestions'][::-1]
            except:
                summary = None
                suggestions = None
        
        message_record = await messages_collections.find_one({"workspace_id": workspace_record['workspace_id'], "session_id": session_id})
        if not message_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: no conversation found for session id")
        
        csat_record = await csat_collections.find_one({'session_id': session_id})
        if csat_record:
            csat_score = csat_record['score']
        else:
            csat_score = None

        chats_history = []
        profiles_record = await profiles_collections.find_one({'session_id': session_id})

        username = profiles_record['username']
        email = profiles_record['email']

        try:
            phone = profiles_record['phone']
        except:
            phone = None

        for record in message_record['roles']:
            try:
                sentiment = record['sentiment']
            except:
                sentiment = None

            try:
                agent_name = record['agent_name']
                agent_id = record['agent_id']
                agent_email = record['agent_email']
            except:
                agent_id = None
                agent_email = None
                agent_name = None

            try:
                audio_file_path = record['audio_file_path']
            except:
                audio_file_path = None

            try:
                id = record['id']
            except:
                id = None

            if start_date and end_date:
                start_datetime = datetime.strptime(start_date, "%d/%m/%Y")
                end_datetime = datetime.strptime(end_date, "%d/%m/%Y") + timedelta(days=1) - timedelta(seconds=1)

                record_timestamp = datetime.strptime(record['timestamp'], "%d/%m/%Y %H:%M:%S") 
                if start_datetime <= record_timestamp <= end_datetime:
                    chats_history.append({
                        'type': record['type'], 'text': record['text'], 'timestamp': record['timestamp'], 'agent_name': agent_name, 
                        'agent_id': agent_id, 'agent_email': agent_email, 'sentiment': sentiment, 'audio_file_path': audio_file_path, 'id': id
                    })

            else:
                chats_history.append({
                    'type': record['type'], 'text': record['text'], 'timestamp': record['timestamp'], 'agent_name': agent_name, 
                    'agent_id': agent_id, 'agent_email': agent_email, 'sentiment': sentiment, 'audio_file_path': audio_file_path, 'id': id
                })
                
        if message_record['end_conversation'] and (message_record['transfer_conversation'] or message_record['human_intervention']):
            conversation_end = 1
        else:
            conversation_end = 0

        latest_timestamp = datetime.now()
        expiration_time = datetime.strptime(message_record['latest_timestamp'], timestamp_format) + timedelta(minutes = int(message_record['timeout']))
        
        if expiration_time < latest_timestamp or message_record['end_conversation']:
            overall_sentiment = message_record['sentiment']
            last_time = datetime.strptime(message_record['roles'][-1]['timestamp'], timestamp_format)
        else:
            overall_sentiment = None
            last_time = datetime.now()

        initial_time = datetime.strptime(message_record['roles'][0]['timestamp'], timestamp_format)

        time_difference = last_time - initial_time
        total_seconds = time_difference.total_seconds()
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)

        formatted_time_diff = f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"

        return username, email, phone, csat_score, overall_sentiment, conversation_end, summary, suggestions, formatted_time_diff, chats_history

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")