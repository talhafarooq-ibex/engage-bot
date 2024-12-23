import tiktoken, io, csv
from decouple import config
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse

from decorators.jwt import jwt_token
from decorators.key import x_app_key
from decorators.teams import x_super_team
from utilities.database import connect
from utilities.validation import check_required_fields, validate_inputs, validate_token
from routers.chats.utilities.session import active_sessions, inactive_sessions, session_details
from routers.chats.utilities.agent import agent_flow
from routers.chats.utilities.client import client_flow
from routers.chats.utilities.profile import create
from routers.chats.utilities.graph import client_graph

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

enc = tiktoken.get_encoding("cl100k_base")

chats_router = APIRouter()

slug_db = config("SLUG_DATABASE")

language_english = config("LANGUAGE_ENGLISH") 
display_language_english = config("DISPLAY_LANGUAGE_ENGLISH") 

language_arabic = config("LANGUAGE_ARABIC") 
display_language_arabic = config("DISPLAY_LANGUAGE_ARABIC") 

human_end_message = config("HUMAN_END_MESSAGE")
display_human_end_message_english = config("DISPLAY_HUMAN_END_MESSAGE_ENGLISH")
display_human_end_message_arabic = config("DISPLAY_HUMAN_END_MESSAGE_ARABIC")

transfer_message = config("TRANSFER_MESSAGE")
display_transfer_message_english = config("DISPLAY_TRANSFER_MESSAGE_ENGLISH")
display_transfer_message_arabic = config("DISPLAY_TRANSFER_MESSAGE_ARABIC")

human_takeover_message = config("HUMAN_TAKEOVER_MESSAGE")
display_human_takeover_message_english = config("DISPLAY_HUMAN_TAKEOVER_MESSAGE_ENGLISH")
display_human_takeover_message_arabic = config("DISPLAY_HUMAN_TAKEOVER_MESSAGE_ARABIC")

human_agent_end_message = config("HUMAN_AGENT_END_MESSAGE")
display_agent_end_message_english = config("DISPLAY_AGENT_END_MESSAGE_ENGLISH")
display_agent_end_message_arabic = config("DISPLAY_AGENT_END_MESSAGE_ARABIC")

prompt_goodbye_english = config("PROMPT_GOODBYE_ENGLISH")
prompt_goodbye_arabic = config("PROMPT_GOODBYE_ARABIC")

prompt_transfer_english = config("PROMPT_TRANSFER_ENGLISH")
prompt_transfer_arabic = config("PROMPT_TRANSFER_ARABIC")

prompt_summary_english = config("PROMPT_SUMMARY_ENGLISH")
prompt_summary_arabic = config("PROMPT_SUMMARY_ARABIC")

agent_arrival_english = config("AGENT_ARRIVAL_ENGLISH")
agent_arrival_arabic = config("AGENT_ARRIVAL_ARABIC")

transfer_queue = config("TRANSFER_QUEUE")
sentiment_url = config("SENTIMENT_URL")

sentiment_headers = {
    'x-app-key': 'eyJhbGciOiJodHRwOi8vd3d3LnczLm9yZy8yMDAxLzA0L3htbGRzaWctbW9yZSNobWFjLXNoYTI1NiIsInR5cCI6IkpXVCJ9.eyJodHRwOi8vc2NoZW1hcy54bWxzb2FwLm9yZy93cy8yMDA1LzA1L2lkZW50aXR5L2NsYWltcy9uYW1laWRlbnRpZmllciI6IjEiLCJodHRwOi8vc2NoZW1hcy54bWxzb2FwLm9yZy93cy8yMDA1LzA1L2lkZW50aXR5L2NsYWltcy9uYW1lIjoibXVoYW1tYWQucml4dmFuLndhaGVlZEBnbWFpbC5jb20iLCJleHAiOjE2NzYyMzA4MjYsImlzcyI6Imh0dHBzOi8vbG9jYWxob3N0OjQ0MzY5LyIsImF1ZCI6Imh0dHBzOi8vbG9jYWxob3N0OjQyMDAifQ.NlSFdJSUQfDF0_hbXkfL_smZkfV8b9KFt4ToBFZDzO0',
    'x-super-team': '100'
}

SUPPORTED_QUEUES = ['web', 'whatsapp', 'sdk']

timestamp_format = '%d/%m/%Y %H:%M:%S'

@chats_router.post('/active/get_all')
@x_super_team
@x_app_key
@jwt_token
async def active_get_all(request: Request):
    try:
        data = await request.form()

        required_fields = ['bot_id', 'workspace_id', 'bot_display', 'agent_display', 'queue_display', 'takeover_display', 'limit', 'page']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")

        bot_id, workspace_id, bot_display = data.get('bot_id'), data.get('workspace_id'), data.get('bot_display')
        agent_display, queue_display, takeover_display = data.get('agent_display'), data.get('queue_display'), data.get('takeover_display')
        limit, page = int(data.get('limit')), int(data.get('page'))

        company_id = request.headers.get('x-super-team')
        agent_id, _, _ = request.state.current_user, request.state.current_name, request.state.current_email

        result = await validate_inputs(company_id, bot_id, workspace_id)
        if not result:
            raise HTTPException(status_code = 400, detail = f"An error occurred: invalid parameter(s)")
        else:
            bots_record, workspace_record, configuration_record = result
        
        items, metadata = await active_sessions(
            bots_record, workspace_record, configuration_record,
            bot_display, agent_display, queue_display, takeover_display, limit, page, agent_id
        )
        
        return JSONResponse(
            content = {"detail": items, "pagination": metadata}, status_code = 200
        )

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")
    
@chats_router.post('/inactive/get_all')
@x_super_team
@x_app_key
@jwt_token
async def inactive_get_all(request: Request):
    try:
        data = await request.form()

        required_fields = ['bot_id', 'workspace_id', 'bot_display', 'agent_display', 'limit', 'page']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")

        bot_id, workspace_id, bot_display = data.get('bot_id'), data.get('workspace_id'), data.get('bot_display')
        agent_display, limit, page = data.get('agent_display'), int(data.get('limit')), int(data.get('page'))
        sort_filter, email_filter = data.get('sort_filter'), data.get('email_filter')
        sentiment_filter = data.getlist('sentiment_filter')
        start_date_filter, end_date_filter = data.get('start_date_filter'), data.get('end_date_filter')

        company_id = request.headers.get('x-super-team')

        result = await validate_inputs(company_id, bot_id, workspace_id)
        if not result:
            raise HTTPException(status_code = 400, detail = f"An error occurred: invalid parameter(s)")
        else:
            bots_record, workspace_record, _ = result
        
        items, metadata = await inactive_sessions(
            bots_record, workspace_record,  
            bot_display, agent_display, limit, page,
            start_date_filter, end_date_filter, sentiment_filter, sort_filter, email_filter
        )
        
        return JSONResponse(
            content = {"detail": items, "pagination": metadata}, status_code = 200
        )
        
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")
    
@chats_router.post('/get')
@x_super_team
@x_app_key
@jwt_token
async def get(request: Request):
    try:
        data = await request.form()

        required_fields = ['bot_id', 'workspace_id', 'session_id']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")

        bot_id, workspace_id, session_id = data.get('bot_id'), data.get('workspace_id'), data.get('session_id')
        start_date, end_date = data.get('start_date'), data.get('end_date')

        company_id = request.headers.get('x-super-team')

        result = await validate_inputs(company_id, bot_id, workspace_id)
        if not result:
            raise HTTPException(status_code = 400, detail = f"An error occurred: invalid parameter(s)")
        else:
            bots_record, workspace_record, _ = result
        
        username, email, phone, csat_score, overall_sentiment, conversation_end, summary, suggestions, formatted_time_diff, chats_history = await session_details(
            bots_record, workspace_record, session_id, start_date, end_date
            )

        return JSONResponse(
            content = {
                "username": username, "email": email, "phone": phone, "csat_score": csat_score, "csat_comment": None, 
                "sentiment": overall_sentiment, "conversation_end": conversation_end, 'summary': summary, 
                'suggestions': suggestions, 'session_time': formatted_time_diff, "detail": chats_history}, 
            status_code = 200)

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")

@chats_router.post('/export')
@x_super_team
@x_app_key
@jwt_token
async def export(request: Request):
    try:
        data = await request.form()

        required_fields = ['bot_id', 'workspace_id', 'session_id']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")

        bot_id, workspace_id, session_id = data.get('bot_id'), data.get('workspace_id'), data.get('session_id')

        company_id = request.headers.get('x-super-team')

        result = await validate_inputs(company_id, bot_id, workspace_id)
        if not result:
            raise HTTPException(status_code = 400, detail = f"An error occurred: invalid parameter(s)")
        else:
            bots_record, _, _ = result
        
        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)

        messages_collections = db['messages']
        profiles_collections = db['profiles']
        
        message_record = await messages_collections.find_one({"workspace_id": workspace_id, "session_id": session_id})
        if not message_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: no conversation found for session id")

        profiles_record = await profiles_collections.find_one({'session_id': session_id})
        email = profiles_record['email']

        output = io.StringIO()
        writer = csv.writer(output)

        writer.writerow(["Text", "Type", "Timestamp", "Sentiment", "Email"])

        for record in message_record['roles']:
            try:
                sentiment = record['sentiment']
            except:
                sentiment = None

            try:
                agent_email = record['agent_email']
            except:
                agent_email = None

            if record['type'] == 'human-agent':
                writer.writerow([record['text'], record['type'], record['timestamp'], sentiment, agent_email])
            elif record['type'] == 'human':
                writer.writerow([record['text'], record['type'], record['timestamp'], sentiment, email])
            else:
                writer.writerow([record['text'], record['type'], record['timestamp'], sentiment, None])

        output.seek(0)

        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=export_session.csv"}
        )

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")

@chats_router.post('/profile')
@x_super_team
@x_app_key
@jwt_token
async def profile(request: Request):
    try:
        data = await request.form()

        required_fields = ['username', 'token', 'queue']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")
        
        username, email, token, queue, phone = data.get('username'), data.get('email'), data.get('token'), data.get('queue'), data.get('phone')  

        result = await validate_token(token)
        if not result:
            raise HTTPException(status_code = 400, detail = f"An error occurred: invalid parameter(s)")
        else:
            bots_record, workspace_record, _ = result
        
        if queue not in SUPPORTED_QUEUES:
            raise HTTPException(status_code = 404, detail = "An error occurred: invalid parameter")
        
        session_id = await create(bots_record, workspace_record, phone, username, email, queue)

        return JSONResponse(content={"detail": session_id}, status_code = 200)  

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")
    
@chats_router.post('/human_agent')
@x_app_key
@jwt_token
async def human_agent(request: Request):
    try:
        data = await request.form()

        required_fields = ['token', 'text', 'session_id']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")

        token, session_id, text = data.get('token'), data.get('session_id'), data.get('text')

        agent_id, agent_name, agent_email = request.state.current_user, request.state.current_name, request.state.current_email

        result = await validate_token(token)
        if not result:
            raise HTTPException(status_code = 400, detail = f"An error occurred: invalid parameter(s)")
        else:
            bots_record, workspace_record, configuration_record = result

        await agent_flow(
            bots_record, workspace_record, configuration_record, 
            session_id, agent_name, agent_id, agent_email, text
        )

        return JSONResponse(content={"detail": "Response has been created"}, status_code = 200)

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")
    
@chats_router.post('/batch')
@x_app_key
@jwt_token
async def batch(request: Request):
    try:
        data = await request.form()

        required_fields = ['token', 'session_id', 'text']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")

        token, text, session_id = data.get('token'), data.get('text'), data.get('session_id')

        result = await validate_token(token)
        if not result:
            raise HTTPException(status_code = 400, detail = f"An error occurred: invalid parameter(s)")
        else:
            bots_record, workspace_record, configuration_record = result

        db = await connect()
        embeddings_collections = db['embeddings']
        embeddings_record = await embeddings_collections.find_one({'bot_id': workspace_record['bot_id'], 'workspace_id': workspace_record['workspace_id']})

        response = await client_flow(bots_record, workspace_record, embeddings_record, configuration_record, text, session_id)
        
        return JSONResponse(content={"detail": response}, status_code = 200)
    
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")
        
@chats_router.post('/whatsapp')
@x_app_key
@jwt_token
async def batch_whatsapp(request: Request):
    try:
        data = await request.form()

        required_fields = ['token', 'session_id', 'text']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")

        token, text, session_id = data.get('token'), data.get('text'), data.get('session_id')

        result = await validate_token(token)
        if not result:
            raise HTTPException(status_code = 400, detail = f"An error occurred: invalid parameter(s)")
        else:
            bots_record, workspace_record, configuration_record = result

        db = await connect()
        embeddings_collections = db['embeddings']
        embeddings_record = await embeddings_collections.find_one({'bot_id': workspace_record['bot_id'], 'workspace_id': workspace_record['workspace_id']})

        response = await client_flow(bots_record, workspace_record, embeddings_record, configuration_record, text, session_id)
        
        return JSONResponse(content={"detail": response}, status_code = 200)
    
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")

@chats_router.post('/graph')
@x_app_key
@jwt_token
async def batch_graph(request: Request):
    try:
        data = await request.form()

        required_fields = ['token', 'session_id', 'text']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")

        token, text, session_id = data.get('token'), data.get('text'), data.get('session_id')

        result = await validate_token(token)
        if not result:
            raise HTTPException(status_code = 400, detail = f"An error occurred: invalid parameter(s)")
        else:
            bots_record, workspace_record, configuration_record = result

        db = await connect()
        embeddings_collections = db['embeddings']
        embeddings_record = await embeddings_collections.find_one({'bot_id': workspace_record['bot_id'], 'workspace_id': workspace_record['workspace_id']})

        response = await client_graph(bots_record, workspace_record, embeddings_record, configuration_record, text, session_id)
        
        return JSONResponse(content={"detail": response}, status_code = 200)
    
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")