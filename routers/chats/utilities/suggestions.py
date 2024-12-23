import requests
from decouple import config
from langchain_groq import ChatGroq
from langchain_ollama import ChatOllama
from langchain_openai import ChatOpenAI
from utilities.database import connect

host = config("DATABASE_HOST")
username = config("DATABASE_USERNAME")
password = config("DATABASE_PASSWORD")
database = config("DATABASE_NAME")
slug_db = config("SLUG_DATABASE")

language_english = config("LANGUAGE_ENGLISH") 
language_arabic = config("LANGUAGE_ARABIC") 

prompt_summary_suggestion_english = config("PROMPT_SUMMARY_SUGGESTION_ENGLISH")
prompt_summary_suggestion_arabic = config("PROMPT_SUMMARY_SUGGESTION_ARABIC")

prompt_message_suggestion_english = config("PROMPT_MESSAGE_SUGGESTION_ENGLISH")
prompt_message_suggestion_arabic = config("PROMPT_MESSAGE_SUGGESTION_ARABIC")
    
async def client_suggestions_anythingllm(company_id, bot_id, workspace_id, session_id):
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

    summaries_record = await summaries_collections.find_one({"session_id": session_id})
    summary = summaries_record['summary']

    headers = {
        'accept': 'application/json',
        'Authorization': f"Bearer {workspace_record['llm_api_key']}",
        'Content-Type': 'application/json'
    }

    summaries_record = await summaries_collections.find_one({"session_id": session_id})
    summary = summaries_record['summary']

    suggestions = summaries_record['suggestions']

    message_record = await messages_collections.find_one({'session_id': session_id})
    url = f"{workspace_record['llm_url']}/api/v1/workspace/{workspace_record['model']}/thread/{message_record['slug']}/chat"

    profiles_record = await profiles_collections.find_one({"session_id": session_id})
    if profiles_record['preference'] == language_english:
        data = {
            "message": prompt_summary_suggestion_english.format(summary = summary),
            "mode": "chat"
        }

    elif profiles_record['preference'] == language_arabic:
        data = {
            "message": prompt_summary_suggestion_arabic.format(summary = summary),
            "mode": "chat"
        }  

    try:  
        response = requests.post(url, headers=headers, json=data)
        response = response.json()  

        suggestions.append(response['textResponse'])

        await summaries_collections.update_one({"_id": summaries_record["_id"]}, {"$set": {"suggestions": suggestions}}) 

        return response['textResponse']
    except:
        return ''

async def client_suggestions_otherllms(company_id, bot_id, workspace_id, session_id):
    db = await connect()

    bots_collections = db['bots']
    workspace_collections = db['workspace']

    bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 1})

    workspace_record = await workspace_collections.find_one({
        "company_id": company_id, "bot_id": bot_id, "workspace_id": workspace_id, "is_active": 1
    })

    slug = bots_record['bot_name'] + slug_db
    db = await connect(slug)

    profiles_collections = db['profiles']
    summaries_collections = db['summary']
    
    profiles_record = await profiles_collections.find_one({"workspace_id": workspace_id, "session_id": session_id})

    summaries_record = await summaries_collections.find_one({"session_id": session_id})
    summary = summaries_record['summary']

    suggestions = summaries_record['suggestions']
    
    if workspace_record['llm'] == 'openai':
        llm = ChatOpenAI(model_name = "gpt-3.5-turbo", temperature = 0, api_key = workspace_record['llm_api_key'], )
    elif workspace_record['llm'] == 'ollama':
        if workspace_record['llm_url']:
            llm = ChatOllama(model = workspace_record['model'], base_url = workspace_record['llm_url'])
        else:
            llm = ChatOllama(model = workspace_record['model'])
    elif workspace_record['llm'] == 'groq':
        llm = ChatGroq(model = workspace_record['model'], api_key = workspace_record['llm_api_key'])

    profiles_record = await profiles_collections.find_one({"session_id": session_id})
    if profiles_record['preference'] == language_english:
        response = await llm.ainvoke(prompt_summary_suggestion_english.format(summary=summary)) 
    elif profiles_record['preference'] == language_arabic:
        response = await llm.ainvoke(prompt_summary_suggestion_arabic.format(summary=summary)) 

    suggestions.append(response.content)
    
    await summaries_collections.update_one({"_id": summaries_record["_id"]}, {"$set": {"suggestions": suggestions}}) 

    return response.content

async def client_message_suggestions_anythingllm(company_id, bot_id, workspace_id, session_id):
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

    summaries_record = await summaries_collections.find_one({"session_id": session_id})
    suggestions = summaries_record['suggestions']
    
    history = []
    for record in message_record['roles']:
        if record['type'] == 'human':
            history.append(record['text'])
    
    message = history.pop()  
    history = '. '.join(history)

    headers = {
        'accept': 'application/json',
        'Authorization': f"Bearer {workspace_record['llm_api_key']}",
        'Content-Type': 'application/json'
    }

    summaries_record = await summaries_collections.find_one({"session_id": session_id})

    message_record = await messages_collections.find_one({'session_id': session_id})
    url = f"{workspace_record['llm_url']}/api/v1/workspace/{workspace_record['model']}/thread/{message_record['slug']}/chat"

    profiles_record = await profiles_collections.find_one({"session_id": session_id})
    if profiles_record['preference'] == language_english:
        data = {
            "message": prompt_message_suggestion_english.format(history = history, message = message),
            "mode": "chat"
        }

    elif profiles_record['preference'] == language_arabic:
        data = {
            "message": prompt_message_suggestion_arabic.format(history = history, message = message),
            "mode": "chat"
        } 

    try:   
        response = requests.post(url, headers=headers, json=data)
        response = response.json()  

        suggestions.append(response['textResponse'])

        await summaries_collections.update_one({"_id": summaries_record["_id"]}, {"$set": {"suggestions": suggestions}})  

        return response['textResponse']
    except:
        return ''

async def client_message_suggestions_otherllms(company_id, bot_id, workspace_id, session_id):
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

    summaries_record = await summaries_collections.find_one({"session_id": session_id})
    suggestions = summaries_record['suggestions']
    
    history = []
    for record in message_record['roles']:
        if record['type'] == 'human':
            history.append(record['text'])
    
    message = history.pop()  
    history = '. '.join(history)

    if workspace_record['llm'] == 'openai':
        llm = ChatOpenAI(model_name = "gpt-3.5-turbo", temperature = 0, api_key = workspace_record['llm_api_key'], )
    elif workspace_record['llm'] == 'ollama':
        if workspace_record['llm_url']:
            llm = ChatOllama(model = workspace_record['model'], base_url = workspace_record['llm_url'])
        else:
            llm = ChatOllama(model = workspace_record['model'])
    elif workspace_record['llm'] == 'groq':
        llm = ChatGroq(model = workspace_record['model'], api_key = workspace_record['llm_api_key'])

    profiles_record = await profiles_collections.find_one({"session_id": session_id})
    if profiles_record['preference'] == language_english:
        response = await llm.ainvoke(prompt_message_suggestion_english.format(history = history, message = message)) 
    elif profiles_record['preference'] == language_arabic:
        response = await llm.ainvoke(prompt_message_suggestion_arabic.format(history = history, message = message)) 

    suggestions.append(response.content)
    
    await summaries_collections.update_one({"_id": summaries_record["_id"]}, {"$set": {"suggestions": suggestions}}) 

    return response.content