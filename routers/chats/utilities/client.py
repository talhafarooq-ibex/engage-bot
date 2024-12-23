import json
import urllib
import uuid
from datetime import datetime, timedelta

import pymongo
import requests
import tiktoken
import torch
import urllib3
from decouple import config
from fastapi import HTTPException
from lancedb.rerankers import LinearCombinationReranker
from langchain.chains import create_retrieval_chain
from langchain.chains.combine_documents import create_stuff_documents_chain
from langchain_chroma import Chroma
from langchain_community.embeddings import OllamaEmbeddings
from langchain_community.vectorstores import FAISS, LanceDB
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.runnables.history import RunnableWithMessageHistory
from langchain_groq import ChatGroq
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_mongodb.chat_message_histories import MongoDBChatMessageHistory
from langchain_ollama import ChatOllama
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from utilities.database import connect
from utilities.redis import enqueue
from utilities.time import current_time

from routers.chats.utilities.suggestions import (
    client_message_suggestions_anythingllm,
    client_message_suggestions_otherllms, client_suggestions_anythingllm,
    client_suggestions_otherllms)
from routers.chats.utilities.summary import (client_summary_anythingllm,
                                             client_summary_otherllms)

enc = tiktoken.get_encoding("cl100k_base")
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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

prompt_goodbye_english = config("PROMPT_GOODBYE_ENGLISH")
prompt_goodbye_arabic = config("PROMPT_GOODBYE_ARABIC")

prompt_transfer_english = config("PROMPT_TRANSFER_ENGLISH")
prompt_transfer_arabic = config("PROMPT_TRANSFER_ARABIC")

transfer_queue = config("TRANSFER_QUEUE")

sentiment_url = config("SENTIMENT_URL")
x_app_key_var = config("X_APP_KEY")

sentiment_headers = {
    'x-app-key': x_app_key_var,
    'x-super-team': '100'
}

timestamp_format = '%d/%m/%Y %H:%M:%S'

device = "cuda" if torch.cuda.is_available() else "cpu"

def get_limited_message_history(
        session_id, connection_string, database_name, collection_name
):
    history = MongoDBChatMessageHistory(
        session_id=session_id,
        connection_string=connection_string,
        database_name=database_name,
        collection_name=collection_name,
    )

    return history

async def client_flow(
    bots_record, workspace_record, embeddings_record, configuration_record, text, session_id
):
    try:      
        max_sessions = workspace_record['sessions_limit']
        
        if await agent_involved_chat(
            bots_record, workspace_record, configuration_record, session_id, text
        ):
            return "Response has been created"
        
        if await max_allowed_chats(workspace_record['workspace_id'], session_id, bots_record['bot_name'], max_sessions):
            return "No agent is available at the moment. Try again later!"

        if text in (language_arabic, language_english):
            response = await client_language_message(text, bots_record, workspace_record, configuration_record, session_id)
        
        elif text == human_end_message:
            response = await client_goodbye_message(bots_record, workspace_record, configuration_record, session_id)
        
        elif text == transfer_message:
            response = await client_transfer_message(bots_record, workspace_record, configuration_record, session_id)

        else:
            response = await client_conversation(text, bots_record, workspace_record, embeddings_record, configuration_record, session_id)

        return response

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")
    
async def agent_involved_chat(
    bots_record, workspace_record, configuration_record, session_id, text
):
    
    try:
        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)

        messages_collections = db['messages']
        profiles_collections = db['profiles']
        
        message_record = await messages_collections.find_one({"workspace_id": workspace_record['workspace_id'], "session_id": session_id})
        profiles_record = await profiles_collections.find_one({"workspace_id": workspace_record['workspace_id'], "session_id": session_id})

        if not message_record:
            return False
        
        if not (message_record['transfer_conversation'] or message_record['human_intervention']):
            return False
        
        if (message_record['transfer_conversation'] or message_record['human_intervention']) and text == human_end_message:
            await client_goodbye_message(bots_record, workspace_record, configuration_record, session_id)
            return True

        human_time = current_time()

        message_record['roles'].append({
            "type": 'human', "text": text, "timestamp": human_time, "input_tokens": 0, "sentiment": None, 'id': str(uuid.uuid4())
        })

        await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})
        await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"latest_timestamp": human_time}})

        profiles_record = await profiles_collections.find_one({'session_id': session_id})
        await profiles_collections.update_one({"_id": profiles_record["_id"]}, {"$set": {"latest_timestamp": human_time}})

        if configuration_record['client_query']:
            data = {
                'text': text
            }

            try:
                sentiment = requests.post(sentiment_url, headers = sentiment_headers, data = data, timeout = 8, verify = False)
                sentiment_human = json.loads(sentiment.text)

                message_record = await messages_collections.find_one({'session_id': session_id})

                for role in message_record['roles']:
                    if not role['sentiment'] and role['type'] == 'human':
                        role['sentiment'] = sentiment_human['sentiment'] 
            except:
                for role in message_record['roles']:
                    if not role['sentiment'] and role['type'] == 'human':
                        role['sentiment'] = 'Neutral'
                        
            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})                    
        
        if configuration_record['summary']:
            if workspace_record['llm'] == 'anythingllm':
                await client_message_suggestions_anythingllm(
                    workspace_record['company_id'], workspace_record['bot_id'], workspace_record['workspace_id'], session_id
                )
            else:
                await client_message_suggestions_otherllms(
                    workspace_record['company_id'], workspace_record['bot_id'], workspace_record['workspace_id'], session_id
                )

        return True

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")
    
async def max_allowed_chats(
    workspace_id, session_id, bot_name, max_sessions
):
    
    try:
        slug = bot_name + slug_db
        db = await connect(slug)

        messages_collections = db['messages']

        message_records = await messages_collections.find({'workspace_id': workspace_id}).to_list(length=None)
        message_record = await messages_collections.find_one({"workspace_id": workspace_id, "session_id": session_id})

        session_active = 0
        for record in message_records:
            latest_timestamp = datetime.now()
            expiration_time = datetime.strptime(record['latest_timestamp'], timestamp_format) + timedelta(minutes = int(record['timeout']))
            
            if not record['transfer_conversation'] or record['human_intervention']:
                if not record['end_conversation'] and expiration_time > latest_timestamp:
                    session_active += 1

        if int(max_sessions) <= session_active:
            if not message_record:
                return True
            
        return False

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")

async def llm_selection(
    workspace_record
):
    
    try:
        if workspace_record['llm'] == 'openai':
            llm = ChatOpenAI(
                model_name = workspace_record['model'], temperature = float(workspace_record['llm_temperature']), 
                api_key = workspace_record['llm_api_key']
            )
        elif workspace_record['llm'] == 'ollama':
            if workspace_record['llm_url']:
                llm = ChatOllama(model = workspace_record['model'], temperature = float(workspace_record['llm_temperature']), 
                base_url = workspace_record['llm_url']
            )
            else:
                llm = ChatOllama(model = workspace_record['model'], temperature = float(workspace_record['llm_temperature']))
        elif workspace_record['llm'] == 'groq':
            llm = ChatGroq(
                model = workspace_record['model'], temperature = float(workspace_record['llm_temperature']), 
                api_key = workspace_record['llm_api_key']
            )

        return llm

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")
    
async def embeddings_and_vectordb_selection(
    workspace_record
):
    
    try:
        if workspace_record['embeddings'] == 'openai':
            embeddings = OpenAIEmbeddings(model = workspace_record['embeddings_model'], openai_api_key = workspace_record['embeddings_api_key'])
        elif workspace_record['embeddings'] == 'huggingface':
            embeddings = HuggingFaceEmbeddings(
                model_name = workspace_record['embeddings_model'], model_kwargs = {'device': device}, encode_kwargs = {'normalize_embeddings': False}
            )
        elif workspace_record['embeddings'] == 'ollama':
            if workspace_record['embeddings_url']:
                embeddings = OllamaEmbeddings(model = workspace_record['embeddings_model'], base_url = workspace_record['embeddings_url'])
            else: 
                embeddings = OllamaEmbeddings(model = workspace_record['embeddings_model'])

        path = (
            f"library/{workspace_record['company_id']}/{workspace_record['bot_id']}/{workspace_record['workspace_id']}/embeddings"
        )

        if workspace_record['vectordb'] == 'faiss':
            vectorstore = FAISS.load_local(path, embeddings, allow_dangerous_deserialization = True)
        elif workspace_record['vectordb'] == 'chroma':
            vectorstore = Chroma(persist_directory = path, embedding_function = embeddings)
        elif workspace_record['vectordb'] == 'lancedb':
            reranker = LinearCombinationReranker(weight = 0.3)
            vectorstore = LanceDB(embedding = embeddings, uri = path, reranker = reranker)

        return embeddings, vectorstore

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")

async def anythingllm_connection(
    workspace_record, message_record
):
    try:
        headers = {
            'accept': 'application/json',
            'Authorization': f"Bearer {workspace_record['llm_api_key']}",
            'Content-Type': 'application/json'
        }

        if not message_record:
            url = f"{workspace_record['llm_url']}/api/v1/workspace/{workspace_record['model']}/thread/new"

            response = requests.post(url, headers = headers)
            response = response.json()

            new_slug = response['thread']['slug'] 

            url = f"{workspace_record['llm_url']}/api/v1/workspace/{workspace_record['model']}/thread/{new_slug}/chat"
        else:
            new_slug = message_record['slug']
            url = f"{workspace_record['llm_url']}/api/v1/workspace/{workspace_record['model']}/thread/{message_record['slug']}/chat"

        return url, headers, new_slug

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")

async def client_language_message(
    text, bots_record, workspace_record, configuration_record, session_id
):
    
    try:
        if text == language_arabic:
            display_message = display_language_arabic
        elif text == language_english:
            display_message = display_language_english

        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)

        messages_collections = db['messages']
        profiles_collections = db['profiles']
        
        message_record = await messages_collections.find_one({"workspace_id": workspace_record['workspace_id'], "session_id": session_id})
        profiles_record = await profiles_collections.find_one({"workspace_id": workspace_record['workspace_id'], "session_id": session_id})

        if workspace_record['llm'] == 'anythingllm':
            url, headers, new_slug = await anythingllm_connection(workspace_record, message_record)

        else:
            llm = await llm_selection(workspace_record)

            history_collections = db['history']
            history_records = await history_collections.find({'SessionId': session_id}, sort=[("_id", pymongo.DESCENDING)]).to_list(length=None)

            chat_limit = int(workspace_record['chat_limit']) * 2
            new_slug = None

            host = config("DATABASE_HOST")
            username = config("DATABASE_USERNAME")
            password = config("DATABASE_PASSWORD")

            username = urllib.parse.quote_plus(username)
            password = urllib.parse.quote_plus(password)

            if len(history_records) > chat_limit:
                records_to_remove = history_records[chat_limit-6:chat_limit-4]
                record_ids_to_remove = [record['_id'] for record in records_to_remove]

                await history_collections.delete_many({'_id': {'$in': record_ids_to_remove}})

            non_rag_prompt = workspace_record['system_prompt']

            qa_prompt = ChatPromptTemplate.from_messages(
                [
                    ("system", non_rag_prompt),
                    MessagesPlaceholder("chat_history"),
                    ("human", "{input}"),
                ]
            )

            runnable = qa_prompt | llm

            if username and password:
                chain_with_history = RunnableWithMessageHistory(
                    runnable,
                    lambda session_id: get_limited_message_history(
                        session_id = session_id,
                        connection_string = f"mongodb://{username}:{password}@{host}:27017",
                        database_name = f"{bots_record['bot_name'] + slug_db}",
                        collection_name = "history"
                    ),
                    input_messages_key="input",
                    history_messages_key="chat_history"
                )

            else:
                chain_with_history = RunnableWithMessageHistory(
                    runnable,
                    lambda session_id: get_limited_message_history(
                        session_id = session_id,
                        connection_string = f"mongodb://{host}:27017",
                        database_name = f"{bots_record['bot_name'] + slug_db}",
                        collection_name = "history"
                    ),
                    input_messages_key="input",
                    history_messages_key="chat_history"      
                )

        human_time = current_time()

        message_record = await messages_collections.find_one({"session_id": session_id})
        if message_record: 
            message_record['roles'].append({
                "type": 'human', "text": display_message, "timestamp": human_time, "input_tokens": None, 
                "sentiment": None, 'id': str(uuid.uuid4())
            })

            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})
            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"latest_timestamp": human_time}})

        else:
            document = {"session_id": session_id, "roles": [{
                "type": 'human', "text": display_message, "timestamp": human_time, "input_tokens": None, "sentiment": None, 
                "id": str(str(uuid.uuid4()))}], 'timeout': bots_record['timeout'], 'language': None, 'sentiment': None, 
                'agent_sentiment': None, 'tags': [None], 'slug': new_slug, 'workspace_id': workspace_record['workspace_id'], 
                'end_conversation': 0, 'transfer_conversation': 0, 'human_intervention': 0, 'agent_expiry': 0, 
                'latest_timestamp': human_time
            }
            
            await messages_collections.insert_one(document)

            profiles_record = await profiles_collections.find_one({'session_id': session_id})
            await profiles_collections.update_one({"_id": profiles_record["_id"]}, {"$set": {"preference": text}}) 

        profiles_record = await profiles_collections.find_one({'session_id': session_id})
        await profiles_collections.update_one({"_id": profiles_record["_id"]}, {"$set": {"latest_timestamp": human_time}})   

        if workspace_record['llm'] == 'anythingllm':
            data = {
                "message": display_message,
                "mode": "chat"
            }

            response = requests.post(url, headers=headers, json=data)
            response = response.json()

            input_tokens = len(enc.encode(display_message))
            output_tokens = len(enc.encode(response['textResponse']))

            bot_time = current_time()

            message_record = await messages_collections.find_one({"session_id": session_id})

            message_record['roles'].append({
                "type": 'ai-agent', "text": response['textResponse'], "timestamp": bot_time,
                "output_tokens": output_tokens, "sentiment": None, 'id': str(uuid.uuid4())
            })     
        
        else:                        
            response = chain_with_history.invoke({'input': display_message}, config={
                "configurable": {"session_id": session_id}
            })
            
            input_tokens, output_tokens = 0, 0
            if workspace_record['llm'] == 'ollama':
                input_tokens = response.response_metadata['prompt_eval_count']
                output_tokens = response.response_metadata['eval_count']
            else:
                input_tokens = response.response_metadata['token_usage']['prompt_tokens']
                output_tokens = response.response_metadata['token_usage']['completion_tokens']
    
            bot_time = current_time()

            message_record = await messages_collections.find_one({"session_id": session_id})

            message_record['roles'].append({
                "type": 'ai-agent', "text": response.content, "timestamp": bot_time,
                "output_tokens": output_tokens, "sentiment": None, 'id': str(uuid.uuid4())
            })

        profiles_record = await profiles_collections.find_one({'session_id': session_id})
        await profiles_collections.update_one({"_id": profiles_record["_id"]}, {"$set": {"latest_timestamp": bot_time}})  

        await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})
        await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"latest_timestamp": bot_time}})

        if configuration_record["client_query"]:
            data = {
                'text': display_message
            }

            try:
                sentiment = requests.post(sentiment_url, headers = sentiment_headers, data = data, timeout = 8, verify = False)
                sentiment_human = json.loads(sentiment.text)

                message_record = await messages_collections.find_one({'session_id': session_id})

                for role in message_record['roles']:
                    if not role['sentiment'] and role['type'] == 'human':
                        role['sentiment'] = sentiment_human['sentiment'] 
            except:
                for role in message_record['roles']:
                    if not role['sentiment'] and role['type'] == 'human':
                        role['sentiment'] = 'Neutral'
               
            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})   

        if configuration_record["bot_response"]:      
            if workspace_record['llm'] == 'anythingllm':   
                data = {
                    'text': response['textResponse']
                }
            else:           
                data = {
                    'text': response.content
                }

            try:
                sentiment = requests.post(sentiment_url, headers = sentiment_headers, data = data, timeout = 8, verify = False)
                sentiment_ai = json.loads(sentiment.text)

                message_record = await messages_collections.find_one({'session_id': session_id})

                for role in message_record['roles']:
                    if not role['sentiment'] and role['type'] == 'ai-agent':
                        role['sentiment'] = sentiment_ai['sentiment'] 
            except:
                for role in message_record['roles']:
                    if not role['sentiment'] and role['type'] == 'ai-agent':
                        role['sentiment'] = 'Neutral'                            

            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})

        message_record = await messages_collections.find_one({'session_id': session_id})

        for role in message_record['roles']:
            if role['type'] == 'human' and not role['input_tokens']:
                role['input_tokens'] = input_tokens

        await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})

        profiles_record = await profiles_collections.find_one({'session_id': session_id})
        await profiles_collections.update_one({"_id": profiles_record["_id"]}, {"$set": {"latest_timestamp": bot_time}})  

        if workspace_record['llm'] == 'anythingllm': 
            return response['textResponse']
        else:
            return response.content

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")
    
async def client_goodbye_message(
    bots_record, workspace_record, configuration_record, session_id
):
    
    try:
        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)

        messages_collections = db['messages']
        profiles_collections = db['profiles']
        
        message_record = await messages_collections.find_one({"workspace_id": workspace_record['workspace_id'], "session_id": session_id})
        profiles_record = await profiles_collections.find_one({"workspace_id": workspace_record['workspace_id'], "session_id": session_id})

        if workspace_record['llm'] != 'anythingllm':
            history_collections = db['history']
            await history_collections.delete_many({'SessionId': session_id})

        human_time = current_time()

        if profiles_record['queue'] == 'web':
            if profiles_record['preference'] == language_english:
                if workspace_record['llm'] == 'anythingllm':
                    input_tokens = len(enc.encode(prompt_goodbye_english)) + 3
                else:
                    input_tokens = None
                
                display_message = display_human_end_message_english

            elif profiles_record['preference'] == language_arabic:
                if workspace_record['llm'] == 'anythingllm':
                    input_tokens = len(enc.encode(prompt_goodbye_arabic)) + 3
                else:
                    input_tokens = None

                display_message = display_human_end_message_arabic

        if message_record: 
            if configuration_record["client_query"]:
                message_record['roles'].append({
                    "type": 'human', "text": display_message, "timestamp": human_time, "input_tokens": input_tokens, 
                    "sentiment": "Neutral", 'id': str(uuid.uuid4())
                })
            else:
                message_record['roles'].append({
                    "type": 'human', "text": display_message, "timestamp": human_time, "input_tokens": input_tokens, 
                    "sentiment": None, 'id': str(uuid.uuid4())
                })

            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})
            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"latest_timestamp": human_time}})

        else:
            if configuration_record["client_query"]:
                temp = {
                    "type": 'human', "text": display_message, "timestamp": human_time, "input_tokens": input_tokens, 
                    "sentiment": "Neutral", 'id': str(uuid.uuid4())
                }
            else:
                temp = {
                    "type": 'human', "text": display_message, "timestamp": human_time, "input_tokens": input_tokens, 
                    "sentiment": None, 'id': str(uuid.uuid4())
                }

            if workspace_record['llm'] == 'anythingllm':
                _, _, new_slug = await anythingllm_connection(workspace_record, message_record)
            else:
                new_slug = None
        
            document = {
                "session_id": session_id, "roles": [temp], 'timeout': bots_record['timeout'], 'language': None, 'sentiment': None, 
                'agent_sentiment': None, 'tags': [None], 'slug': new_slug, 'workspace_id': workspace_record['workspace_id'], 
                'end_conversation': 0, 'transfer_conversation': 0, 'human_intervention': 0, 'agent_expiry': 0, 
                'latest_timestamp': human_time
            }

            await messages_collections.insert_one(document)

        profiles_record = await profiles_collections.find_one({'session_id': session_id})
        await profiles_collections.update_one({"_id": profiles_record["_id"]}, {"$set": {"latest_timestamp": human_time}})

        if profiles_record['queue'] == 'web':
            response = await client_goodbye_web_response(bots_record, workspace_record, configuration_record, session_id)
        elif profiles_record['queue'] == 'whatsapp':
            message_record = await messages_collections.find_one({"session_id": session_id})
            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"end_conversation": 1}})
            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"latest_timestamp": human_time}})

            response = "Response has been created"

        return response

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")
    
async def client_goodbye_web_response(
    bots_record, workspace_record, configuration_record, session_id
):
    
    try:
        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)

        messages_collections = db['messages']
        profiles_collections = db['profiles']
        
        message_record = await messages_collections.find_one({"workspace_id": workspace_record['workspace_id'], "session_id": session_id})
        profiles_record = await profiles_collections.find_one({"workspace_id": workspace_record['workspace_id'], "session_id": session_id})

        if workspace_record['llm'] == 'anythingllm':
            if profiles_record['preference'] == language_english:
                data = {
                    "message": prompt_goodbye_english,
                    "mode": "chat"
                }
            elif profiles_record['preference'] == language_arabic:
                data = {
                    "message": prompt_goodbye_arabic,
                    "mode": "chat"
                }

            url, headers, _ = await anythingllm_connection(workspace_record, message_record)

            response = requests.post(url, headers=headers, json=data)
            response = response.json()

            output_tokens = len(enc.encode(response['textResponse']))

            bot_time = current_time()
            
            message_record = await messages_collections.find_one({"session_id": session_id})
            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"end_conversation": 1}})

            message_record['roles'].append({
                "type": 'ai-agent', "text": response['textResponse'], "timestamp": bot_time, "output_tokens": output_tokens,
                "sentiment": None, 'id': str(uuid.uuid4())
            })
        
        else:

            llm = await llm_selection(workspace_record)

            if profiles_record['preference'] == language_english:
                response = await llm.ainvoke(prompt_goodbye_english)
            elif profiles_record['preference'] == language_arabic:
                response = await llm.ainvoke(prompt_goodbye_arabic)

            bot_time = current_time()

            message_record = await messages_collections.find_one({"session_id": session_id})
            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"end_conversation": 1}})

            if workspace_record['llm'] == 'ollama':
                message_record['roles'].append({
                    "type": 'ai-agent', "text": response.content, "timestamp": bot_time, 
                    "output_tokens": response.response_metadata['eval_count'], "sentiment": None, 'id': str(uuid.uuid4())
                })
            else:
                message_record['roles'].append({
                    "type": 'ai-agent', "text": response.content, "timestamp": bot_time, 
                    "output_tokens": response.response_metadata['token_usage']['completion_tokens'], "sentiment": None, 'id': str(uuid.uuid4())
                })
            
        await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})
        await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"latest_timestamp": bot_time}})

        profiles_record = await profiles_collections.find_one({'session_id': session_id})
        await profiles_collections.update_one({"_id": profiles_record["_id"]}, {"$set": {"latest_timestamp": bot_time}})    

        if configuration_record["bot_response"]:    
            if workspace_record['llm'] == 'anythingllm':
                data = {
                    'text': response['textResponse']
                }
            else:             
                data = {
                    'text': response.content
                }

            try:
                sentiment = requests.post(sentiment_url, headers = sentiment_headers, data = data, timeout = 8, verify = False)
                sentiment_ai = json.loads(sentiment.text)

                message_record = await messages_collections.find_one({'session_id': session_id})

                for role in message_record['roles']:
                    if not role['sentiment'] and role['type'] == 'ai-agent':
                        role['sentiment'] = sentiment_ai['sentiment'] 
            except:
                for role in message_record['roles']:
                    if not role['sentiment'] and role['type'] == 'ai-agent':
                        role['sentiment'] = 'Neutral'                            

            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})

        if workspace_record['llm'] == 'anythingllm':
            await client_summary_anythingllm(
                workspace_record['company_id'], workspace_record['bot_id'], workspace_record['workspace_id'], session_id
            )

            return response['textResponse']

        else:
            message_record = await messages_collections.find_one({'session_id': session_id})

            for role in message_record['roles']:
                if role['type'] == 'human' and not role['input_tokens']:
                    if workspace_record['llm'] == 'ollama':
                        role['input_tokens'] = response.response_metadata['prompt_eval_count']
                    else:
                        role['input_tokens'] = response.response_metadata['token_usage']['prompt_tokens']

            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})
        
            await client_summary_otherllms(
                workspace_record["company_id"], workspace_record['bot_id'], workspace_record['workspace_id'], session_id
            )
        
            return response.content

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")
    
async def client_transfer_message(
    bots_record, workspace_record, configuration_record, session_id
):
    
    try:
        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)

        messages_collections = db['messages']
        profiles_collections = db['profiles']

        if workspace_record['llm'] != 'anythingllm':
            history_collections = db['history']
            await history_collections.delete_many({'SessionId': session_id})

        message_record = await messages_collections.find_one({"workspace_id": workspace_record['workspace_id'], "session_id": session_id})
        profiles_record = await profiles_collections.find_one({"workspace_id": workspace_record['workspace_id'], "session_id": session_id})

        human_time = current_time()

        if profiles_record['preference'] == language_english:
            if workspace_record['llm'] == 'anythingllm':
                input_tokens = len(enc.encode(prompt_transfer_english)) + 3
            else:
                input_tokens = None
            
            display_message = display_transfer_message_english

        elif profiles_record['preference'] == language_arabic:
            if workspace_record['llm'] == 'anythingllm':
                input_tokens = len(enc.encode(prompt_transfer_arabic)) + 3
            else:
                input_tokens = None

            display_message = display_transfer_message_arabic

        if message_record: 
            if configuration_record['client_query']:
                message_record['roles'].append({
                    "type": 'human', "text": display_message, "timestamp": human_time, "input_tokens": input_tokens, 
                    "sentiment": "Neutral", 'id': str(uuid.uuid4())
                })
            else:
                message_record['roles'].append({
                    "type": 'human', "text": display_message, "timestamp": human_time, "input_tokens": input_tokens, 
                    "sentiment": None, 'id': str(uuid.uuid4())
                })

            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})
            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"latest_timestamp": human_time}})
        else:
            if configuration_record['client_query']:
                temp = {
                    "type": 'human', "text": display_message, "timestamp": human_time, "input_tokens": input_tokens, 
                    "sentiment": "Neutral", 'id': str(uuid.uuid4())
                }
            else:
                temp = {
                    "type": 'human', "text": display_message, "timestamp": human_time, "input_tokens": input_tokens,
                    "sentiment": None, 'id': str(uuid.uuid4())
                }

            if workspace_record['llm'] == 'anythingllm':
                _, _, new_slug = await anythingllm_connection(workspace_record, message_record)
            else:
                new_slug = None
        
            document = {
                "session_id": session_id, "roles": [temp], 'timeout': bots_record['timeout'], 'language': None, 'sentiment': None,
                'agent_sentiment': None, 'tags': [None], 'slug': new_slug, 'workspace_id': workspace_record['workspace_id'], 'end_conversation': 0, 
                'transfer_conversation': 0, 'human_intervention': 0, 'agent_expiry': 0, 'latest_timestamp': human_time
            }

            await messages_collections.insert_one(document)

        profiles_record = await profiles_collections.find_one({'session_id': session_id})
        await profiles_collections.update_one({"_id": profiles_record["_id"]}, {"$set": {"latest_timestamp": human_time}})

        if profiles_record['queue'] == 'web':
            response = await client_transfer_web_response(bots_record, workspace_record, configuration_record, session_id)
        
        elif profiles_record['queue'] == 'whatsapp':
            message_record = await messages_collections.find_one({"session_id": session_id})
            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"transfer_conversation": 1}})
            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"end_conversation": 1}})
            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"latest_timestamp": human_time}})

            response = "Response has been created"
        
        return response

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")
    
async def client_transfer_web_response(
    bots_record, workspace_record, configuration_record, session_id
):
    
    try:
        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)

        messages_collections = db['messages']
        profiles_collections = db['profiles']
        
        message_record = await messages_collections.find_one({"workspace_id": workspace_record['workspace_id'], "session_id": session_id})
        profiles_record = await profiles_collections.find_one({"workspace_id": workspace_record['workspace_id'], "session_id": session_id})

        if workspace_record['llm'] == 'anythingllm':
            if profiles_record['preference'] == language_english:
                data = {
                    "message": prompt_transfer_english,
                    "mode": "chat"
                }
            elif profiles_record['preference'] == language_arabic:
                data = {
                    "message": prompt_transfer_arabic,
                    "mode": "chat"
                }

            url, headers, _ = await anythingllm_connection(workspace_record, message_record)

            response = requests.post(url, headers=headers, json=data)
            response = response.json()

            output_tokens = len(enc.encode(response['textResponse']))

            bot_time = current_time()
            
            message_record = await messages_collections.find_one({"session_id": session_id})

            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"transfer_conversation": 1}})                

            message_record['roles'].append({
                "type": 'ai-agent', "text": response['textResponse'], "timestamp": bot_time, "output_tokens": output_tokens, 
                "sentiment": None, 'id': str(uuid.uuid4())
            })

        else:
            llm = await llm_selection(workspace_record)

            response = None
            if profiles_record['preference'] == language_english:
                response = await llm.ainvoke(prompt_transfer_english)
            elif profiles_record['preference'] == language_arabic:
                response = await llm.ainvoke(prompt_transfer_arabic)

            bot_time = current_time()

            message_record = await messages_collections.find_one({"session_id": session_id})
            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"transfer_conversation": 1}})

            if workspace_record['llm'] == 'ollama':
                message_record['roles'].append({
                    "type": 'ai-agent', "text": response.content.replace('"', ''), "timestamp": bot_time, 
                    "output_tokens": response.response_metadata['eval_count'], "sentiment": None, 'id': str(uuid.uuid4())
                })
            else:
                message_record['roles'].append({
                    "type": 'ai-agent', "text": response.content.replace('"', ''), "timestamp": bot_time, 
                    "output_tokens": response.response_metadata['token_usage']['completion_tokens'], "sentiment": None, 
                    'id': str(uuid.uuid4())
                })

        if configuration_record['auto_assignment']:
            queue = transfer_queue + f":{workspace_record['bot_id']}:{workspace_record['workspace_id']}"
            await enqueue(session_id, queue)

        await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})
        await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"latest_timestamp": bot_time}})

        profiles_record = await profiles_collections.find_one({'session_id': session_id})
        await profiles_collections.update_one({"_id": profiles_record["_id"]}, {"$set": {"latest_timestamp": bot_time}})

        if configuration_record["bot_response"]:    
            if workspace_record['llm'] == 'anythingllm':
                data = {
                    'text': response['textResponse']
                }
            else:             
                data = {
                    'text': response.content
                }

            try:
                sentiment = requests.post(sentiment_url, headers = sentiment_headers, data = data, timeout = 8, verify = False)
                sentiment_ai = json.loads(sentiment.text)

                message_record = await messages_collections.find_one({'session_id': session_id})

                for role in message_record['roles']:
                    if not role['sentiment'] and role['type'] == 'ai-agent':
                        role['sentiment'] = sentiment_ai['sentiment'] 
            except:
                for role in message_record['roles']:
                    if not role['sentiment'] and role['type'] == 'ai-agent':
                        role['sentiment'] = 'Neutral'                            

            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})

        if workspace_record['llm'] == 'anythingllm':
            await client_summary_anythingllm(
                workspace_record['company_id'], workspace_record['bot_id'], workspace_record['workspace_id'], session_id
            )
            
            await client_suggestions_anythingllm(
                workspace_record['company_id'], workspace_record['bot_id'], workspace_record['workspace_id'], session_id
            )

            return response['textResponse']
        
        else:
            message_record = await messages_collections.find_one({'session_id': session_id})

            for role in message_record['roles']:
                if role['type'] == 'human' and not role['input_tokens']:
                    if workspace_record['llm'] == 'ollama':
                        role['input_tokens'] = response.response_metadata['prompt_eval_count']
                    else:
                        role['input_tokens'] = response.response_metadata['token_usage']['prompt_tokens']

            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})

            await client_summary_otherllms( 
                workspace_record["company_id"], workspace_record['bot_id'], workspace_record['workspace_id'], session_id
            )

            await client_suggestions_otherllms( 
                workspace_record["company_id"], workspace_record['bot_id'], workspace_record['workspace_id'], session_id
            )

            return response.content
    
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")
    
async def client_conversation(
    text, bots_record, workspace_record, embeddings_record, configuration_record, session_id
):
    
    try:
        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)

        chat_limit = int(workspace_record['chat_limit']) * 2

        if workspace_record['llm'] != 'anythingllm':
            history_collections = db['history']
            history_records = await history_collections.find({'SessionId': session_id}, sort=[("_id", pymongo.DESCENDING)]).to_list(length=None)

            if len(history_records) > chat_limit:
                records_to_remove = history_records[chat_limit-6:chat_limit-4]
                record_ids_to_remove = [record['_id'] for record in records_to_remove]

                await history_collections.delete_many({'_id': {'$in': record_ids_to_remove}})

        if workspace_record['llm'] == 'anythingllm':
            response = await anythingllm_conversation(
                text, bots_record, workspace_record, configuration_record, session_id
            )
        else:
            if embeddings_record:
                response = await embedding_conversation_chain(
                    bots_record, workspace_record, configuration_record, session_id, text
                )
            else:
                response = await non_embedding_conversation_chain(
                    bots_record, workspace_record, configuration_record, session_id, text                
                )
            
        return response
    
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")

async def anythingllm_conversation(
    text, bots_record, workspace_record, configuration_record, session_id
):
    
    try:
        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)

        messages_collections = db['messages']
        profiles_collections = db['profiles']
        
        message_record = await messages_collections.find_one({"workspace_id": workspace_record['workspace_id'], "session_id": session_id})
        profiles_record = await profiles_collections.find_one({"workspace_id": workspace_record['workspace_id'], "session_id": session_id})

        url, headers, new_slug = await anythingllm_connection(workspace_record, message_record)

        human_time = current_time()

        input_tokens = len(enc.encode(text))

        message_record = await messages_collections.find_one({"session_id": session_id})
        if message_record: 
            message_record['roles'].append({
                "type": 'human', "text": text, "timestamp": human_time, "input_tokens": input_tokens, "sentiment": None, 'id': str(uuid.uuid4())
            })

            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})
            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"latest_timestamp": human_time}})

        else:
            document = {"session_id": session_id, "roles": [{
                "type": 'human', "text": text, "timestamp": human_time, "input_tokens": input_tokens, "sentiment": None, 
                'id': str(uuid.uuid4())}], 'timeout': bots_record['timeout'], 'language': None, 'sentiment': None, 'agent_sentiment': None, 
                'tags': [None], 'slug': new_slug, 'workspace_id': workspace_record['workspace_id'], 'end_conversation': 0, 
                'transfer_conversation': 0, 'human_intervention': 0, 'agent_expiry': 0, 'latest_timestamp': human_time
            }

            await messages_collections.insert_one(document)

        profiles_record = await profiles_collections.find_one({'session_id': session_id})
        await profiles_collections.update_one({"_id": profiles_record["_id"]}, {"$set": {"latest_timestamp": human_time}})  
    
        data = {
            "message": text,
            "mode": "chat"
        }

        response = requests.post(url, headers=headers, json=data)
        response = response.json()

        output_tokens = len(enc.encode(response['textResponse']))

        bot_time = current_time()

        message_record = await messages_collections.find_one({"session_id": session_id})

        message_record['roles'].append({
            "type": 'ai-agent', "text": response['textResponse'], "timestamp": bot_time, "output_tokens": output_tokens, "sentiment": None, 
            'id': str(uuid.uuid4())
        })

        profiles_record = await profiles_collections.find_one({'session_id': session_id})
        await profiles_collections.update_one({"_id": profiles_record["_id"]}, {"$set": {"latest_timestamp": bot_time}})  

        await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})
        await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"latest_timestamp": bot_time}})

        if configuration_record['client_query']:
            data = {
                'text': text
            }

            try:
                sentiment = requests.post(sentiment_url, headers = sentiment_headers, data = data, timeout = 8, verify = False)
                sentiment_human = json.loads(sentiment.text)

                message_record = await messages_collections.find_one({'session_id': session_id})

                for role in message_record['roles']:
                    if not role['sentiment'] and role['type'] == 'human':
                        role['sentiment'] = sentiment_human['sentiment'] 
            except:
                for role in message_record['roles']:
                    if not role['sentiment'] and role['type'] == 'human':
                        role['sentiment'] = 'Neutral'
                        
            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})  

        if configuration_record['bot_response']:
            data = {
                'text': response['textResponse']
            }

            try:
                sentiment = requests.post(sentiment_url, headers = sentiment_headers, data = data, timeout = 8, verify = False)
                sentiment_ai = json.loads(sentiment.text)

                message_record = await messages_collections.find_one({'session_id': session_id})

                for role in message_record['roles']:
                    if not role['sentiment'] and role['type'] == 'ai-agent':
                        role['sentiment'] = sentiment_ai['sentiment'] 
            except:
                for role in message_record['roles']:
                    if not role['sentiment'] and role['type'] == 'ai-agent':
                        role['sentiment'] = 'Neutral'                            

            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})  

        return response['textResponse']
            
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")   

async def non_embedding_conversation_chain(
    bots_record, workspace_record, configuration_record, session_id, text
):
    
    try:
        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)

        messages_collections = db['messages']
        profiles_collections = db['profiles']
        
        message_record = await messages_collections.find_one({"workspace_id": workspace_record['workspace_id'], "session_id": session_id})
        profiles_record = await profiles_collections.find_one({"workspace_id": workspace_record['workspace_id'], "session_id": session_id})

        host = config("DATABASE_HOST")
        username = config("DATABASE_USERNAME")
        password = config("DATABASE_PASSWORD")

        username = urllib.parse.quote_plus(username)
        password = urllib.parse.quote_plus(password)

        non_rag_prompt = workspace_record['system_prompt']

        qa_prompt = ChatPromptTemplate.from_messages(
            [
                ("system", non_rag_prompt),
                MessagesPlaceholder("chat_history"),
                ("human", "{input}"),
            ]
        )

        llm = await llm_selection(workspace_record)

        runnable = qa_prompt | llm

        if username and password:
            chain_with_history = RunnableWithMessageHistory(
                runnable,
                lambda session_id: get_limited_message_history(
                    session_id = session_id,
                    connection_string = f"mongodb://{username}:{password}@{host}:27017",
                    database_name = f"{bots_record['bot_name'] + slug_db}",
                    collection_name = "history"
                ),
                input_messages_key="input",
                history_messages_key="chat_history"
            )

        else:
            chain_with_history = RunnableWithMessageHistory(
                runnable,
                lambda session_id: get_limited_message_history(
                    session_id = session_id,
                    connection_string = f"mongodb://{host}:27017",
                    database_name = f"{bots_record['bot_name'] + slug_db}",
                    collection_name = "history"
                ),
                input_messages_key="input",
                history_messages_key="chat_history" 
            )

        human_time = current_time()

        message_record = await messages_collections.find_one({"session_id": session_id})
        if message_record: 
            message_record['roles'].append({
                "type": 'human', "text": text, "timestamp": human_time, "input_tokens": None, "sentiment": None, 'id': str(uuid.uuid4())
            })

            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})
            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"latest_timestamp": human_time}})

        else:
            document = {"session_id": session_id, "roles": [{
                "type": 'human', "text": text, "timestamp": human_time, "input_tokens": None, "sentiment": None, 'id': str(uuid.uuid4())
                }], 'timeout': bots_record['timeout'], 'language': None, 'sentiment': None, 'agent_sentiment': None, 'tags': [None], 
                'slug': None, 'workspace_id': workspace_record['workspace_id'], 'end_conversation': 0, 'transfer_conversation': 0, 
                'human_intervention': 0, 'agent_expiry': 0, 'latest_timestamp': human_time}
            
            await messages_collections.insert_one(document)

        profiles_record = await profiles_collections.find_one({'session_id': session_id})
        await profiles_collections.update_one({"_id": profiles_record["_id"]}, {"$set": {"latest_timestamp": human_time}})                    

        response = chain_with_history.invoke({'input': text}, config={
            "configurable": {"session_id": session_id}
        })
        
        input_tokens, output_tokens = 0, 0
        if workspace_record['llm'] == 'ollama':
            input_tokens = response.response_metadata['prompt_eval_count']
            output_tokens = response.response_metadata['eval_count']
        else:
            input_tokens = response.response_metadata['token_usage']['prompt_tokens']
            output_tokens = response.response_metadata['token_usage']['completion_tokens']
    
        bot_time = current_time()

        message_record = await messages_collections.find_one({"session_id": session_id})

        message_record['roles'].append({
            "type": 'ai-agent', "text": response.content, "timestamp": bot_time, "output_tokens": output_tokens, 
            "sentiment": None, 'id': str(uuid.uuid4())
        })

        profiles_record = await profiles_collections.find_one({'session_id': session_id})
        await profiles_collections.update_one({"_id": profiles_record["_id"]}, {"$set": {"latest_timestamp": bot_time}})  

        await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})
        await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"latest_timestamp": bot_time}})

        if configuration_record['client_query']:
            data = {
                'text': text
            }

            try:
                sentiment = requests.post(sentiment_url, headers = sentiment_headers, data = data, timeout = 8, verify = False)
                sentiment_human = json.loads(sentiment.text)

                message_record = await messages_collections.find_one({'session_id': session_id})

                for role in message_record['roles']:
                    if not role['sentiment'] and role['type'] == 'human':
                        role['sentiment'] = sentiment_human['sentiment'] 
            except:
                for role in message_record['roles']:
                    if not role['sentiment'] and role['type'] == 'human':
                        role['sentiment'] = 'Neutral'
                        
            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})   

        if configuration_record['bot_response']:                       
            data = {
                'text': response.content
            }

            try:
                sentiment = requests.post(sentiment_url, headers = sentiment_headers, data = data, timeout = 8, verify = False)
                sentiment_ai = json.loads(sentiment.text)

                message_record = await messages_collections.find_one({'session_id': session_id})

                for role in message_record['roles']:
                    if not role['sentiment'] and role['type'] == 'ai-agent':
                        role['sentiment'] = sentiment_ai['sentiment'] 
            except:
                for role in message_record['roles']:
                    if not role['sentiment'] and role['type'] == 'ai-agent':
                        role['sentiment'] = 'Neutral'                            

            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})  

        message_record = await messages_collections.find_one({'session_id': session_id})

        for role in message_record['roles']:
            if role['type'] == 'human' and not role['input_tokens']:
                role['input_tokens'] = input_tokens

        await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})   

        return response.content

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")
    
async def embedding_conversation_chain(
    bots_record, workspace_record, configuration_record, session_id, text
):
    
    try:
        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)

        messages_collections = db['messages']
        profiles_collections = db['profiles']
        
        message_record = await messages_collections.find_one({"workspace_id": workspace_record['workspace_id'], "session_id": session_id})
        profiles_record = await profiles_collections.find_one({"workspace_id": workspace_record['workspace_id'], "session_id": session_id})

        host = config("DATABASE_HOST")
        username = config("DATABASE_USERNAME")
        password = config("DATABASE_PASSWORD")

        username = urllib.parse.quote_plus(username)
        password = urllib.parse.quote_plus(password)

        _, vectorstore = await embeddings_and_vectordb_selection(workspace_record)

        retriever = vectorstore.as_retriever(search_kwargs={"k": int(workspace_record['k_retreive'])})

        rag_prompt = workspace_record['system_prompt'] + '\n\n{context}'

        qa_prompt = ChatPromptTemplate.from_messages(
            [
                ("system", rag_prompt),
                MessagesPlaceholder("chat_history"),
                ("human", "{input}"),
            ]
        )

        llm = await llm_selection(workspace_record)

        question_answer_chain = create_stuff_documents_chain(llm, qa_prompt)
        rag_chain = create_retrieval_chain(retriever, question_answer_chain)

        if username and password:
            chain_with_history = RunnableWithMessageHistory(
                rag_chain,
                lambda session_id: get_limited_message_history(
                    session_id=session_id,
                    connection_string = f"mongodb://{username}:{password}@{host}:27017",
                    database_name=f"{bots_record['bot_name'] + slug_db}",
                    collection_name="history"
                ),
                input_messages_key="input",
                history_messages_key="chat_history",    
                output_messages_key="answer"    
            )

        else:
            chain_with_history = RunnableWithMessageHistory(
                rag_chain,
                lambda session_id: get_limited_message_history(
                    session_id=session_id,
                    connection_string=f"mongodb://{host}:27017",
                    database_name=f"{bots_record['bot_name'] + slug_db}",
                    collection_name="history"
                ),
                input_messages_key="input",
                history_messages_key="chat_history", 
                output_messages_key="answer"       
            )

        human_time = current_time()

        message_record = await messages_collections.find_one({"session_id": session_id})
        if message_record: 
            message_record['roles'].append({
                "type": 'human', "text": text, "timestamp": human_time, "input_tokens": None, "sentiment": None, 'id': str(uuid.uuid4())
            })

            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})
            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"latest_timestamp": human_time}})

        else:
            document = {"session_id": session_id, "roles": [{
                "type": 'human', "text": text, "timestamp": human_time, "input_tokens": None, "sentiment": None, 'id': str(uuid.uuid4())
                }], 'timeout': bots_record['timeout'], 'language': None, 'sentiment': None, 'agent_sentiment': None, 'tags': [None], 
                'slug': None, 'workspace_id': workspace_record['workspace_id'], 'end_conversation': 0, 'transfer_conversation': 0, 
                'human_intervention': 0, 'agent_expiry': 0, 'latest_timestamp': human_time}
            
            await messages_collections.insert_one(document)

        profiles_record = await profiles_collections.find_one({'session_id': session_id})
        await profiles_collections.update_one({"_id": profiles_record["_id"]}, {"$set": {"latest_timestamp": human_time}})                      

        response = chain_with_history.invoke({'input': text}, config={
            "configurable": {"session_id": session_id}
        })

        prompt_tokens = len(enc.encode(workspace_record['system_prompt'])) + 3
        user_tokens = len(enc.encode(response['input'])) + 3

        if response['chat_history']:
            history_tokens = [len(enc.encode(message.content)) + 3 for message in response['chat_history']]
        else:
            history_tokens = [0]

        context_tokens = [len(enc.encode(message.page_content)) + 3 for message in response['context']]

        input_tokens, output_tokens = 0, 0
        input_tokens += prompt_tokens + user_tokens + sum(history_tokens) + sum(context_tokens)

        tokens = enc.encode(response['answer'])
        output_tokens += len(tokens)

        bot_time = current_time()

        message_record = await messages_collections.find_one({"session_id": session_id})

        message_record['roles'].append({
            "type": 'ai-agent', "text": response['answer'], "timestamp": bot_time, "output_tokens": output_tokens, 
            "sentiment": None, 'id': str(uuid.uuid4())
        })

        profiles_record = await profiles_collections.find_one({'session_id': session_id})
        await profiles_collections.update_one({"_id": profiles_record["_id"]}, {"$set": {"latest_timestamp": bot_time}})  

        await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})
        await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"latest_timestamp": bot_time}})

        if configuration_record['client_query']:
            data = {
                'text': text
            }

            try:
                sentiment = requests.post(sentiment_url, headers = sentiment_headers, data = data, timeout = 8, verify = False)
                sentiment_human = json.loads(sentiment.text)

                message_record = await messages_collections.find_one({'session_id': session_id})

                for role in message_record['roles']:
                    if not role['sentiment'] and role['type'] == 'human':
                        role['sentiment'] = sentiment_human['sentiment'] 
            except:
                for role in message_record['roles']:
                    if not role['sentiment'] and role['type'] == 'human':
                        role['sentiment'] = 'Neutral'
                        
            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})  

        if configuration_record['bot_response']:                    
            data = {
                'text': response['answer']
            }

            try:
                sentiment = requests.post(sentiment_url, headers = sentiment_headers, data = data, timeout = 8, verify = False)
                sentiment_ai = json.loads(sentiment.text)

                message_record = await messages_collections.find_one({'session_id': session_id})

                for role in message_record['roles']:
                    if not role['sentiment'] and role['type'] == 'ai-agent':
                        role['sentiment'] = sentiment_ai['sentiment'] 
            except:
                for role in message_record['roles']:
                    if not role['sentiment'] and role['type'] == 'ai-agent':
                        role['sentiment'] = 'Neutral'                            

        await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})

        message_record = await messages_collections.find_one({'session_id': session_id})

        for role in message_record['roles']:
            if role['type'] == 'human' and not role['input_tokens']:
                role['input_tokens'] = input_tokens

        await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})          

        return response['answer']

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")