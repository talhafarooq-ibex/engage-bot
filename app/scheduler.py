import asyncio
import json
import uuid
from datetime import datetime, timedelta

import requests
from decouple import config
from fastapi import FastAPI
from langchain_groq import ChatGroq
from langchain_ollama import ChatOllama
from langchain_openai import ChatOpenAI
from routers.chats.utilities.summary import (client_summary_anythingllm,
                                             client_summary_otherllms)
from utilities.database import connect, database_names
from utilities.redis import dequeue, enqueue, view_queue
from utilities.time import current_time

app = FastAPI()

slug_db = config("SLUG_DATABASE")

language_english = config("LANGUAGE_ENGLISH") 
display_language_english = config("DISPLAY_LANGUAGE_ENGLISH") 

language_arabic = config("LANGUAGE_ARABIC") 
display_language_arabic = config("DISPLAY_LANGUAGE_ARABIC")

agent_arrival_english = config("AGENT_ARRIVAL_ENGLISH")
agent_arrival_arabic = config("AGENT_ARRIVAL_ARABIC")

transfer_queue = config("TRANSFER_QUEUE")

sentiment_url = config("SENTIMENT_URL")
x_app_key_var = config("X_APP_KEY")

sentiment_headers = {
    'x-app-key': x_app_key_var,
    'x-super-team': '100'
}

async def auto_assign_agents():
    try:
        db_names = await database_names()
        
        db_check = await connect()
        bots_collections = db_check['bots']
        configuration_collections = db_check['configuration'] 
        workspace_collections = db_check['workspace']    
    
        for db_name in db_names:
            temp = db_name.replace(slug_db, '')

            bots_record = await bots_collections.find_one({"bot_name": temp, 'is_active': 1})
            if not bots_record:
                pass
            
            bot_id = bots_record['bot_id']

            db = await connect(db_name)
            messages_collections = db['messages']   
            profiles_collections = db['profiles'] 
            agents_collections = db['agents']

            workspace_records = await workspace_collections.find({'bot_id': bot_id, 'is_active': 1}).to_list(length=None)
            for workspace_record in workspace_records:
                configuration_record = await configuration_collections.find_one({"bot_id": bot_id, "workspace_id": workspace_record['workspace_id']})

                if not configuration_record or not configuration_record['auto_assignment']:
                    continue

                agents_records = await agents_collections.find({"is_active": 1, "workspace_id": workspace_record['workspace_id']}).to_list(length=None)
                if not agents_records:
                    continue

                agent_sessions = {}

                for agent_record in agents_records:
                    agent_id = f"{agent_record['agent_id']}:{agent_record['agent_name']}:{agent_record['agent_email']}"
                    agent_queue = await view_queue(agent_id)
                    agent_sessions[agent_id] = agent_queue

                queue = transfer_queue + f":{bot_id}:{workspace_record['workspace_id']}"
                waiting_queue = await view_queue(queue)

                for session_id in waiting_queue:
                    message_record = await messages_collections.find_one({"session_id": session_id})

                    if not message_record:
                        continue 
                    workspace_id = message_record['workspace_id']  
                    workspace_record = await workspace_collections.find_one({'bot_id': bot_id, 'workspace_id': workspace_id})
                    sessions_limit = int(workspace_record['sessions_limit'])

                    eligible_agents = {agent: sessions for agent, sessions in agent_sessions.items() if len(sessions) < sessions_limit}

                    if not eligible_agents:
                        break

                    least_loaded_agent = min(eligible_agents, key=lambda x: len(agent_sessions[x]))
                    agent_sessions[least_loaded_agent].append(session_id)

                    queue = transfer_queue + f':{bot_id}:{workspace_id}'
                    await dequeue(queue)
                    await enqueue(session_id, least_loaded_agent)

                    message_record = await messages_collections.find_one({"session_id": session_id})

                    if workspace_record['llm'] == 'openai':
                        llm = ChatOpenAI(model_name = "gpt-3.5-turbo", temperature = 0, api_key = workspace_record['llm_api_key'], )
                    elif workspace_record['llm'] == 'ollama':
                        if workspace_record['llm_url']:
                            llm = ChatOllama(model = workspace_record['model'], base_url = workspace_record['llm_url'])
                        else:
                            llm = ChatOllama(model = workspace_record['model'])
                    elif workspace_record['llm'] == 'groq':
                        llm = ChatGroq(model = workspace_record['model'], api_key = workspace_record['llm_api_key'])

                    if workspace_record['llm'] == 'anythingllm':
                        headers = {
                            'accept': 'application/json',
                            'Authorization': f"Bearer {workspace_record['llm_api_key']}",
                            'Content-Type': 'application/json'
                        }

                        message_record = await messages_collections.find_one({'session_id': session_id})
                        url = f"{workspace_record['llm_url']}/api/v1/workspace/{workspace_record['model']}/thread/{message_record['slug']}/chat"

                        profiles_record = await profiles_collections.find_one({"session_id": session_id})
                        if profiles_record['preference'] == language_english:
                            data = {
                                "message": agent_arrival_english.format(agent_name = least_loaded_agent.split(':')[1]),
                                "mode": "chat"
                            }

                        elif profiles_record['preference'] == language_arabic:
                            data = {
                                "message": agent_arrival_arabic.format(agent_name = least_loaded_agent.split(':')[1]),
                                "mode": "chat"
                            }

                        response = requests.post(url, headers=headers, json=data)
                        response = response.json()
                    
                        response_time = current_time()

                        if ['agent']:  
                            message_record['roles'].append({
                                "type": 'human-agent', "text": response['textResponse'], "timestamp": response_time, "agent_name": least_loaded_agent.split(':')[1], 
                                "agent_id": least_loaded_agent.split(':')[0], "agent_email": least_loaded_agent.split(':')[2], "output_tokens": 0, 
                                "sentiment": "Neutral", 'id': str(uuid.uuid4())
                            })
                        else:
                            message_record['roles'].append({
                                "type": 'human-agent', "text": response['textResponse'], "timestamp": response_time, "agent_name": least_loaded_agent.split(':')[1],
                                "agent_id": least_loaded_agent.split(':')[0], "agent_email": least_loaded_agent.split(':')[2], "output_tokens": 0, "sentiment": None, 
                                'id': str(uuid.uuid4())
                            })

                    else:
                        profiles_record = await profiles_collections.find_one({"session_id": session_id})
                        if profiles_record['preference'] == language_english:
                            response = await llm.ainvoke(agent_arrival_english.format(agent_name = least_loaded_agent.split(':')[1]))

                        elif profiles_record['preference'] == language_arabic:
                            response = await llm.ainvoke(agent_arrival_arabic.format(agent_name = least_loaded_agent.split(':')[1]))

                        response_time = current_time()

                        if configuration_record['agent']:  
                            message_record['roles'].append({"type": 'human-agent', "text": response.content, "timestamp": response_time, "agent_name": least_loaded_agent.split(':')[1], "agent_id": least_loaded_agent.split(':')[0], "agent_email": least_loaded_agent.split(':')[2], "output_tokens": 0, "sentiment": "Neutral", 'id': str(uuid.uuid4())})
                        else:
                            message_record['roles'].append({"type": 'human-agent', "text": response.content, "timestamp": response_time, "agent_name": least_loaded_agent.split(':')[1], "agent_id": least_loaded_agent.split(':')[0], "agent_email": least_loaded_agent.split(':')[2], "output_tokens": 0, "sentiment": None, 'id': str(uuid.uuid4())})

                    await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})
                    await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"latest_timestamp": response_time}})

                    await profiles_collections.update_one({"_id": profiles_record["_id"]}, {"$set": {"latest_timestamp": response_time}})
    except:
        pass

async def sentiment_and_language_schedule():
    try:
        db_names = await database_names()

        db_check = await connect()
        bots_collections = db_check['bots']
        configuration_collections = db_check['configuration']

        for db_name in db_names:
            temp = db_name.replace(slug_db, '')
            
            bots_record = await bots_collections.find_one({"bot_name": temp, 'is_active': 1})
            if not bots_record:
                pass

            bot_id = bots_record['bot_id']

            db = await connect(db_name)
            messages_collections = db['messages']

            message_records = await messages_collections.find({"sentiment": None, "language": None}).to_list(length=None)
            for record in message_records:
                timestamp_format = '%d/%m/%Y %H:%M:%S'
                latest_timestamp = datetime.now()

                workspace_id = record['workspace_id']
                sentiments_record = await configuration_collections.find_one({"bot_id": bot_id, "workspace_id": workspace_id})

                if not sentiments_record:
                    continue

                try:
                    if sentiments_record['conversation']:
                        expiration_time = datetime.strptime(record['latest_timestamp'], timestamp_format) + timedelta(minutes=int(record['timeout']))
                        if record['end_conversation'] or expiration_time < latest_timestamp:
                            messages = [role['text'] for role in record['roles'] if role['type'] == 'human']
                            messages = '. '.join(messages)
                            data = {
                                'text': messages
                            }

                            response = requests.post(sentiment_url, headers = sentiment_headers, data = data, verify = False)
                            response = json.loads(response.text)
                            language = response['language']
                            sentiment = response['sentiment']

                            await messages_collections.update_one({"_id": record["_id"]}, {"$set": {"language": language, "sentiment": sentiment}})
                except:
                    pass
    except:
        pass

async def agent_sentiment_schedule():
    try:
        db_names = await database_names()
        
        db_check = await connect()
        bots_collections = db_check['bots']
        configuration_collections = db_check['configuration']

        for db_name in db_names:
            temp = db_name.replace(slug_db, '')
            
            bots_record = await bots_collections.find_one({"bot_name": temp, 'is_active': 1})
            if not bots_record:
                pass
            
            bot_id = bots_record['bot_id']

            db = await connect(db_name)
            messages_collections = db['messages']

            message_records = await messages_collections.find({"agent_sentiment": None, 'end_conversation': 1}).to_list(length=None)
            for record in message_records:
                workspace_id = record['workspace_id']

                sentiments_record = await configuration_collections.find_one({"bot_id": bot_id, "workspace_id": workspace_id})

                if not sentiments_record:
                    continue

                try:
                    if sentiments_record['agent'] and (record['transfer_conversation'] or record['human_intervention']):
                        messages = [role['text'] for role in record['roles'] if role['type'] == 'human-agent']

                        if not messages:
                            await messages_collections.update_one({"_id": record["_id"]}, {"$set": {"agent_sentiment": 'Neutral'}})
                            continue 

                        messages = '. '.join(messages)

                        data = {
                            'text': messages
                        }
                        response = requests.post(sentiment_url, headers = sentiment_headers, data = data, verify=False)
                        response = json.loads(response.text)
                        sentiment = response['sentiment']
                        try:
                            await messages_collections.update_one({"_id": record["_id"]}, {"$set": {"agent_sentiment": sentiment}})
                        except:
                            pass
                except:
                    pass
    except:
        pass

async def release_temp_memory():
    try:
        db_names = await database_names()

        db_check = await connect()
        bots_collections = db_check['bots']

        for db_name in db_names:

            temp = db_name.replace(slug_db, '')
            
            bots_record = await bots_collections.find_one({"bot_name": temp, 'is_active': 1})
            if not bots_record:
                pass
        
            db = await connect(db_name)
            history_collections = db['history']
            messages_collections = db['messages']
            message_records = await messages_collections.find().to_list(length=None)
            for record in message_records:
                timestamp_format = '%d/%m/%Y %H:%M:%S'
                latest_timestamp = datetime.now()

                try:
                    expiration_time = datetime.strptime(record['latest_timestamp'], timestamp_format) + timedelta(minutes=int(record['timeout']))
                    if record['end_conversation'] or expiration_time < latest_timestamp or record['transfer_conversation'] or record['human_intervention']:
                        history_records = await history_collections.find({'SessionId': record['session_id']}).to_list(length=None)

                        if history_records:
                            record_ids_to_remove = [record['_id'] for record in history_records]
                            await history_collections.delete_many({'_id': {'$in': record_ids_to_remove}})
                except:
                    pass
    except:
        pass

async def summary_expired_session():
    try:
        db_names = await database_names()
        
        db_main = await connect()
        bots_collections = db_main['bots']
        workspace_collections = db_main['workspace']
        configuration_collections = db_main['configuration']

        for db_name in db_names:

            temp = db_name.replace(slug_db, '')
            
            bots_record = await bots_collections.find_one({"bot_name": temp, 'is_active': 1})
            if not bots_record:
                pass

            db = await connect(db_name)
            
            messages_collections = db['messages']
            summaries_collections = db['summary']

            message_records = await messages_collections.find().to_list(length=None)
            
            for record in message_records:
                timestamp_format = '%d/%m/%Y %H:%M:%S'
                latest_timestamp = datetime.now()

                try:
                    expiration_time = datetime.strptime(record['latest_timestamp'], timestamp_format) + timedelta(minutes=int(record['timeout']))
                    if not record['end_conversation'] and expiration_time < latest_timestamp and not record['transfer_conversation'] and not record['human_intervention']:
                        name = db_name.replace(slug_db, '')
                        bots_record = await bots_collections.find_one({'bot_name': name, 'is_active': 1})

                        configuration_record = await configuration_collections.find_one({'bot_id': bots_record['bot_id'], 'workspace_id': record['workspace_id']})
                        if not configuration_record:
                            continue

                        if not configuration_record['summary']:
                            continue         
                        
                        summaries_record = await summaries_collections.find_one({"session_id": record['session_id']})

                        if summaries_record:
                            if summaries_record['summary']:
                                continue

                        workspace_record = await workspace_collections.find_one({'bot_id': bots_record['bot_id'], 'workspace_id': record['workspace_id']})

                        if workspace_record['llm'] == 'anythingllm':
                            await client_summary_anythingllm(bots_record['company_id'], bots_record['bot_id'], record['workspace_id'], record['session_id'])
                        else:
                            await client_summary_otherllms(bots_record['company_id'], bots_record['bot_id'], record['workspace_id'], record['session_id'])
                except:
                    pass
    except:
        pass

async def agent_expired_sessions():
    try:
        db_names = await database_names()

        db_check = await connect()
        bots_collections = db_check['bots']

        for db_name in db_names:

            temp = db_name.replace(slug_db, '')
            
            bots_record = await bots_collections.find_one({"bot_name": temp, 'is_active': 1})
            if not bots_record:
                pass
            
            db = await connect(db_name)
            messages_collections = db['messages']

            message_records = await messages_collections.find({"end_conversation": 0}).to_list(length = None)
            for record in message_records:
                if not record['human_intervention'] and not record['transfer_conversation']:
                    continue

                try:
                    if record['roles'][-1]['type'] == 'human' or record['roles'][-1]['type'] == 'ai-agent':
                        for message in record['roles']:
                            if message['type'] == 'human' or record['roles'][-1]['type'] == 'ai-agent':
                                latest_human_timestamp = message['timestamp']

                        timestamp_format = '%d/%m/%Y %H:%M:%S'
                        latest_timestamp = datetime.now()
                        expiration_time = datetime.strptime(latest_human_timestamp, timestamp_format) + timedelta(minutes=int(record['timeout']))

                        if expiration_time < latest_timestamp:
                            try:
                                await messages_collections.update_one({"_id": record["_id"]}, {"$set": {"agent_expiry": 1}})
                                await messages_collections.update_one({"_id": record["_id"]}, {"$set": {"end_conversation": 1}})
                            except:
                                pass   
                    elif record['roles'][-1]['type'] == 'human-agent':
                        for message in record['roles']:
                            if message['type'] == 'human-agent':
                                latest_human_timestamp = message['timestamp']

                        timestamp_format = '%d/%m/%Y %H:%M:%S'
                        latest_timestamp = datetime.now()
                        expiration_time = datetime.strptime(latest_human_timestamp, timestamp_format) + timedelta(minutes=int(record['timeout']))

                        if expiration_time < latest_timestamp:
                            try:
                                await messages_collections.update_one({"_id": record["_id"]}, {"$set": {"end_conversation": 1}})
                            except:
                                pass 
                except:
                    pass  
    except:
        pass

async def task_150_seconds():
    while True:
        await asyncio.gather(
            sentiment_and_language_schedule(),
            agent_sentiment_schedule(),
            release_temp_memory(),
            summary_expired_session()
        )
        await asyncio.sleep(150)

async def task_5_seconds():
    while True:
        await asyncio.gather(
            agent_expired_sessions(),
            auto_assign_agents()
        )
        await asyncio.sleep(5)

@app.on_event("startup")
async def startup():
    asyncio.create_task(task_150_seconds())
    asyncio.create_task(task_5_seconds())

@app.get("/")
async def index():
    return "Service is running"

@app.on_event("shutdown")
async def shutdown():
    pass

if __name__ == "__main__":
    app.run()