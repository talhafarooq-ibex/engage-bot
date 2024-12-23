import json, requests, uuid
from decouple import config
from datetime import datetime
from fastapi import HTTPException

from utilities.database import connect
from utilities.redis import enqueue, delete_from_queue
from routers.chats.utilities.summary import client_summary_anythingllm, client_summary_otherllms
from routers.chats.utilities.suggestions import client_suggestions_anythingllm, client_suggestions_otherllms

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

slug_db = config("SLUG_DATABASE")

language_english = config("LANGUAGE_ENGLISH") 
display_language_english = config("DISPLAY_LANGUAGE_ENGLISH") 

language_arabic = config("LANGUAGE_ARABIC") 
display_language_arabic = config("DISPLAY_LANGUAGE_ARABIC") 

human_takeover_message = config("HUMAN_TAKEOVER_MESSAGE")
display_human_takeover_message_english = config("DISPLAY_HUMAN_TAKEOVER_MESSAGE_ENGLISH")
display_human_takeover_message_arabic = config("DISPLAY_HUMAN_TAKEOVER_MESSAGE_ARABIC")

human_agent_end_message = config("HUMAN_AGENT_END_MESSAGE")
display_agent_end_message_english = config("DISPLAY_AGENT_END_MESSAGE_ENGLISH")
display_agent_end_message_arabic = config("DISPLAY_AGENT_END_MESSAGE_ARABIC")

sentiment_url = config("SENTIMENT_URL")
x_app_key = config("X_APP_KEY")

sentiment_headers = {
    'x-app-key': x_app_key,
    'x-super-team': '100'
}

async def agent_flow(
    bots_record, workspace_record, configuration_record, 
    session_id, agent_name, agent_id, agent_email, text
):
    
    try:        
        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)

        messages_collections = db['messages']
        
        message_record = await messages_collections.find_one({"workspace_id": workspace_record['workspace_id'], "session_id": session_id})

        if not message_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: no conversation found for session id") 
        
        if text == human_agent_end_message:
            await agent_goodbye(
                bots_record['bot_name'], workspace_record['workspace_id'], session_id, configuration_record['agent'], 
                configuration_record['auto_assignment'], agent_id, agent_email, agent_name
            )
        elif not message_record['transfer_conversation'] and text == human_takeover_message:
            await agent_takeover(
                bots_record['company_id'], bots_record['bot_id'], workspace_record['workspace_id'], 
                bots_record['bot_name'], session_id, configuration_record['agent'], configuration_record['auto_assignment'], 
                agent_id, agent_email, agent_name, workspace_record['llm']
            )
        else:
            await agent_message(
                workspace_record['workspace_id'], text, bots_record['bot_name'], session_id, configuration_record['agent'], 
                agent_id, agent_email, agent_name
            )

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")
    
async def agent_goodbye(
    bot_name, workspace_id, session_id, agent, auto_assignment, agent_id, agent_email, agent_name
):
    
    try:
        slug = bot_name + slug_db
        db = await connect(slug)

        messages_collections = db['messages']
        profiles_collections = db['profiles']
        
        message_record = await messages_collections.find_one({"workspace_id": workspace_id, "session_id": session_id})
        profiles_record = await profiles_collections.find_one({"workspace_id": workspace_id, "session_id": session_id})

        now = datetime.now()
        human_time = now.strftime("%d/%m/%Y %H:%M:%S")  

        if profiles_record['preference'] == language_english:
            if agent:
                message_record['roles'].append({
                    "type": 'human-agent', "text": display_agent_end_message_english, "timestamp": human_time, "agent_name": agent_name, 
                    "agent_id": agent_id, "agent_email": agent_email, "output_tokens": 0, "sentiment": "Neutral", 'id': str(uuid.uuid4())
                })
            else:
                message_record['roles'].append({
                    "type": 'human-agent', "text": display_agent_end_message_english, "timestamp": human_time, "agent_name": agent_name, 
                    "agent_id": agent_id, "agent_email": agent_email, "output_tokens": 0, "sentiment": None, 'id': str(uuid.uuid4())
                })

        elif profiles_record['preference'] == language_arabic:
            if agent:
                message_record['roles'].append({
                    "type": 'human-agent', "text": display_agent_end_message_arabic, "timestamp": human_time, "agent_name": agent_name, 
                    "agent_id": agent_id, "agent_email": agent_email, "output_tokens": 0, "sentiment": "Neutral", 'id': str(uuid.uuid4())
                })
            else:
                message_record['roles'].append({
                    "type": 'human-agent', "text": display_agent_end_message_arabic, "timestamp": human_time, "agent_name": agent_name, 
                    "agent_id": agent_id, "agent_email": agent_email, "output_tokens": 0, "sentiment": None, 'id': str(uuid.uuid4())
                })

        await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"end_conversation": 1}})

        await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})
        await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"latest_timestamp": human_time}})

        await profiles_collections.update_one({"_id": profiles_record["_id"]}, {"$set": {"latest_timestamp": human_time}})

        if auto_assignment:
            await delete_from_queue(session_id, f"{agent_id}:{agent_name}:{agent_email}")

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")
    
async def agent_takeover(
    company_id, bot_id, workspace_id, bot_name, session_id, agent, auto_assignment, agent_id, agent_email, agent_name, llm_choice
):
    
    try:
        slug = bot_name + slug_db
        db = await connect(slug)

        messages_collections = db['messages']
        profiles_collections = db['profiles']
        
        message_record = await messages_collections.find_one({"workspace_id": workspace_id, "session_id": session_id})
        profiles_record = await profiles_collections.find_one({"workspace_id": workspace_id, "session_id": session_id})

        if not llm_choice == 'anythingllm':
            history_collections = db['history']
            await history_collections.delete_many({'SessionId': session_id})

        now = datetime.now()
        human_time = now.strftime("%d/%m/%Y %H:%M:%S")  

        if profiles_record['preference'] == language_english:
            text = display_human_takeover_message_english
        elif profiles_record['preference'] == language_arabic:
            text = display_human_takeover_message_arabic

        if agent:                        
            message_record['roles'].append({
                "type": 'human-agent', "text": text, "timestamp": human_time, "agent_name": agent_name, "agent_id": agent_id, 
                "agent_email": agent_email, "output_tokens": 0, "sentiment": "Neutral", 'id': str(uuid.uuid4())
            })
        else:
            message_record['roles'].append({
                "type": 'human-agent', "text": text, "timestamp": human_time, "agent_name": agent_name, "agent_id": agent_id, 
                "agent_email": agent_email, "output_tokens": 0, "sentiment": None, 'id': str(uuid.uuid4())
            })

        await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})
        await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"latest_timestamp": human_time}})

        await profiles_collections.update_one({"_id": profiles_record["_id"]}, {"$set": {"latest_timestamp": human_time}})

        await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"human_intervention": 1}})

        if auto_assignment:
            await enqueue(session_id, f"{agent_id}:{agent_name}:{agent_email}")

        if llm_choice == 'anythingllm':
            await client_summary_anythingllm(company_id, bot_id, workspace_id, session_id)
            await client_suggestions_anythingllm(company_id, bot_id, workspace_id, session_id)
        else:
            await client_summary_otherllms(company_id, bot_id, workspace_id, session_id)
            await client_suggestions_otherllms(company_id, bot_id, workspace_id, session_id)
    
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")
    
async def agent_message(
    workspace_id, text, bot_name, session_id, agent, agent_id, agent_email, agent_name
):
    
    try:
        slug = bot_name + slug_db
        db = await connect(slug)

        messages_collections = db['messages']
        profiles_collections = db['profiles']
        
        message_record = await messages_collections.find_one({"workspace_id": workspace_id, "session_id": session_id})
        profiles_record = await profiles_collections.find_one({"workspace_id": workspace_id, "session_id": session_id})

        now = datetime.now()
        human_time = now.strftime("%d/%m/%Y %H:%M:%S") 

        message_record['roles'].append({
            "type": 'human-agent', "text": text, "timestamp": human_time, "agent_name": agent_name, "agent_id": agent_id, 
            "agent_email": agent_email, "output_tokens": 0, "sentiment": None, 'id': str(uuid.uuid4())
        })

        await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})
        await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"latest_timestamp": human_time}})

        await profiles_collections.update_one({"_id": profiles_record["_id"]}, {"$set": {"latest_timestamp": human_time}})

        if agent:
            data = {
                'text': text
            }

            try:
                sentiment = requests.post(sentiment_url, headers = sentiment_headers, data = data, timeout = 8, verify = False)
                sentiment_human = json.loads(sentiment.text)

                message_record = await messages_collections.find_one({'session_id': session_id})

                for role in message_record['roles']:
                    if not role['sentiment'] and role['type'] == 'human-agent':
                        role['sentiment'] = sentiment_human['sentiment'] 
            except:
                for role in message_record['roles']:
                    if not role['sentiment'] and role['type'] == 'human-agent':
                        role['sentiment'] = 'Neutral'     

                await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")