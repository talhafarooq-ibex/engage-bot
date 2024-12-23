import requests
from decouple import config
from datetime import datetime
from langchain_openai import ChatOpenAI
from langchain_groq import ChatGroq
from langchain_ollama import ChatOllama

from utilities.database import connect

host = config("DATABASE_HOST")
username = config("DATABASE_USERNAME")
password = config("DATABASE_PASSWORD")
database = config("DATABASE_NAME")
slug_db = config("SLUG_DATABASE")

slug_db = config("SLUG_DATABASE")

language_english = config("LANGUAGE_ENGLISH") 
language_arabic = config("LANGUAGE_ARABIC") 

prompt_summary_english = config("PROMPT_SUMMARY_ENGLISH")
prompt_summary_arabic = config("PROMPT_SUMMARY_ARABIC")

async def client_summary_anythingllm(company_id, bot_id, workspace_id, session_id):
    db = await connect()

    bots_collections = db['bots']
    workspace_collections = db['workspace']

    bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 1})

    workspace_record = await workspace_collections.find_one({
        "company_id": company_id, "bot_id": bot_id, "workspace_id": workspace_id, "is_active": 1
    })

    slug = bots_record['bot_name'] + slug_db
    db = await connect(slug)

    messages_collections = db['messages']
    profiles_collections = db['profiles']
    summaries_collections = db['summary']
    
    message_record = await messages_collections.find_one({"workspace_id": workspace_id, "session_id": session_id})
    profiles_record = await profiles_collections.find_one({"workspace_id": workspace_id, "session_id": session_id})

    messages = []
    for record in message_record['roles']:
        if record['type'] == 'human':
            messages.append(record['text'])

    messages = '. '.join(messages)

    headers = {
        'accept': 'application/json',
        'Authorization': f"Bearer {workspace_record['llm_api_key']}",
        'Content-Type': 'application/json'
    }

    message_record = await messages_collections.find_one({'session_id': session_id})
    url = f"{workspace_record['llm_url']}/api/v1/workspace/{workspace_record['model']}/thread/{message_record['slug']}/chat"

    now = datetime.now()
    human_time = now.strftime("%d/%m/%Y %H:%M:%S")

    profiles_record = await profiles_collections.find_one({"session_id": session_id})
    if profiles_record['preference'] == language_english:
        data = {
            "message": prompt_summary_english.format(messages=messages),
            "mode": "chat"
        }

    elif profiles_record['preference'] == language_arabic:
        data = {
            "message": prompt_summary_arabic.format(messages=messages),
            "mode": "chat"
        }

    response = requests.post(url, headers=headers, json=data)
    response = response.json()

    now = datetime.now()
    summary_time = now.strftime("%d/%m/%Y %H:%M:%S")   

    document = {
        'company_id': company_id, 'bot_id': bot_id, 'session_id': session_id, 'summary': response['textResponse'], 
        'suggestions': [], 'is_active': 1, 'created_date': summary_time
    }

    await summaries_collections.insert_one(document)

    return response['textResponse']

async def client_summary_otherllms(company_id, bot_id, workspace_id, session_id):
    db = await connect()

    bots_collections = db['bots']
    workspace_collections = db['workspace']

    bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 1})

    workspace_record = await workspace_collections.find_one({
        "company_id": company_id, "bot_id": bot_id, "workspace_id": workspace_id, "is_active": 1
    })

    slug = bots_record['bot_name'] + slug_db
    db = await connect(slug)

    messages_collections = db['messages']
    profiles_collections = db['profiles']
    summaries_collections = db['summary']
    
    message_record = await messages_collections.find_one({"workspace_id": workspace_id, "session_id": session_id})
    profiles_record = await profiles_collections.find_one({"workspace_id": workspace_id, "session_id": session_id})

    messages = []
    for record in message_record['roles']:
        if record['type'] == 'human':
            messages.append(record['text'])

    messages = '. '.join(messages)

    if workspace_record['llm'] == 'openai':
        llm = ChatOpenAI(model_name = "gpt-3.5-turbo", temperature = 0, api_key = workspace_record['llm_api_key'], )
    elif workspace_record['llm'] == 'ollama':
        if workspace_record['llm_url']:
            llm = ChatOllama(model = workspace_record['model'], base_url = workspace_record['llm_url'])
        else:
            llm = ChatOllama(model = workspace_record['model'])
    elif workspace_record['llm'] == 'groq':
        llm = ChatGroq(model = workspace_record['model'], api_key = workspace_record['llm_api_key'])

    now = datetime.now()
    human_time = now.strftime("%d/%m/%Y %H:%M:%S")

    profiles_record = await profiles_collections.find_one({"session_id": session_id})
    if profiles_record['preference'] == language_english:
        response = await llm.ainvoke(prompt_summary_english.format(messages=messages))

    elif profiles_record['preference'] == language_arabic:
        response = await llm.ainvoke(prompt_summary_arabic.format(messages=messages))

    now = datetime.now()
    summary_time = now.strftime("%d/%m/%Y %H:%M:%S")  

    document = {
        'company_id': company_id, 'bot_id': bot_id, 'session_id': session_id, 'summary': response.content, 
        'suggestions': [], 'is_active': 1, 'created_date': summary_time
    }

    await summaries_collections.insert_one(document)

    return response.content