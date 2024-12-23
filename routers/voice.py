import uuid, os, json, requests, tiktoken
from datetime import datetime
from fastapi import APIRouter, Request, UploadFile, File, Form, HTTPException
from fastapi.responses import FileResponse
from langchain_openai import ChatOpenAI
from langchain_groq import ChatGroq
from langchain_community.chat_models import ChatOllama
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.prompts import MessagesPlaceholder
from langchain_core.runnables.history import RunnableWithMessageHistory
from langchain_mongodb.chat_message_histories import MongoDBChatMessageHistory
from decouple import config

from utilities.database import connect
from decorators.jwt import jwt_token
from decorators.key import x_app_key
from decorators.teams import x_super_team

slug_db = config("SLUG_DATABASE")

language_english = config("LANGUAGE_ENGLISH") 
display_language_english = config("DISPLAY_LANGUAGE_ENGLISH") 

language_arabic = config("LANGUAGE_ARABIC") 
display_language_arabic = config("DISPLAY_LANGUAGE_ARABIC") 

enc = tiktoken.get_encoding("cl100k_base")

sentiment_url = 'https://sentiments.enteract.app/get_sentiment'
sentiment_headers = {
    'x-app-key': 'eyJhbGciOiJodHRwOi8vd3d3LnczLm9yZy8yMDAxLzA0L3htbGRzaWctbW9yZSNobWFjLXNoYTI1NiIsInR5cCI6IkpXVCJ9.eyJodHRwOi8vc2NoZW1hcy54bWxzb2FwLm9yZy93cy8yMDA1LzA1L2lkZW50aXR5L2NsYWltcy9uYW1laWRlbnRpZmllciI6IjEiLCJodHRwOi8vc2NoZW1hcy54bWxzb2FwLm9yZy93cy8yMDA1LzA1L2lkZW50aXR5L2NsYWltcy9uYW1lIjoibXVoYW1tYWQucml4dmFuLndhaGVlZEBnbWFpbC5jb20iLCJleHAiOjE2NzYyMzA4MjYsImlzcyI6Imh0dHBzOi8vbG9jYWxob3N0OjQ0MzY5LyIsImF1ZCI6Imh0dHBzOi8vbG9jYWxob3N0OjQyMDAifQ.NlSFdJSUQfDF0_hbXkfL_smZkfV8b9KFt4ToBFZDzO0',
    'x-super-team': '100'
}

voice_router = APIRouter()

def transcribe_audio(file_path: str, language: str, asr_model_api_url: str):
    data = {'language': language}

    with open(file_path, 'rb') as audio_file:
        files = {'audio': audio_file}
        response = requests.post(asr_model_api_url, data=data, files=files, verify=False)
    
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=f"ASR API failed: {response.text}")
    
    return response.json()

def convert_text_to_speech(company_id: str, bot_id: str, workspace_id: str, session_id: str, text: str, language: str, tts_model_api_url):

    data = {'language': language, 'text': text}
    response = requests.post(tts_model_api_url, data=data)

    if response.status_code != 200:
        raise HTTPException(status_code=500, detail=f"TTS API failed: {response.text}")
    
    # Create directory for saving audio
    output_directory = os.path.join(os.getcwd(), "app", "audio", company_id, bot_id, workspace_id, session_id, "ai-agent")
    os.makedirs(output_directory, exist_ok=True)
    
    audio_file_name = f"{uuid.uuid4()}.wav"
    audio_file_path = os.path.join(output_directory, audio_file_name)
    
    # Save audio content to file
    with open(audio_file_path, 'wb') as audio_file:
        audio_file.write(response.content)

    # audio_file_path = os.path.join(audio_file_path)

    return audio_file_path

def get_limited_message_history(session_id, connection_string, database_name, collection_name):
    history = MongoDBChatMessageHistory(
        session_id=session_id,
        connection_string=connection_string,
        database_name=database_name,
        collection_name=collection_name,
    )

    return history

async def llm_response(text, session_id, token):
    try:        
        db = await connect()

        bots_collections = db['bots']
        workspace_collections = db['workspace']
        tokens_collections = db['tokens']

        tokens_record = await tokens_collections.find_one({"token": token, "is_active": 1})

        if tokens_record:
            now = datetime.now()
            date_time = now.strftime("%d/%m/%Y %H:%M:%S")

            date_format = "%d/%m/%Y %H:%M:%S"
            created_date = datetime.strptime(date_time, date_format)
            expiry_date = datetime.strptime(tokens_record['expiry_date'], date_format)

            if expiry_date < created_date:
                raise HTTPException(status_code = 401, detail = "An error occurred: token has expired")
            
            bot_id = tokens_record['bot_id']
            workspace_id = tokens_record['workspace_id']
            company_id = tokens_record['company_id']
        else:
            raise HTTPException(status_code = 404, detail = "An error occurred: invalid 'token' parameter")

        bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 1})

        if bots_record:
            workspace_record = await workspace_collections.find_one({
                "company_id": company_id, "bot_id": bot_id, "workspace_id": workspace_id, "is_active": 1
            })
            
            if not workspace_record:
                raise HTTPException(status_code = 404, detail = "An error occurred: no workspace found for this bot or key doesn't exist")
            
            slug = bots_record['bot_name'] + slug_db
            db = await connect(slug)

            messages_collections = db['messages']

            if workspace_record['llm'] == 'anythingllm':
                headers = {
                    'accept': 'application/json',
                    'Authorization': f"Bearer {workspace_record['llm_api_key']}",
                    'Content-Type': 'application/json'
                }

                message_record = await messages_collections.find_one({'session_id': session_id})
                url = f"{workspace_record['llm_url']}/api/v1/workspace/{workspace_record['model']}/thread/{message_record['slug']}/chat"

                data = {
                    "message": text,
                    "mode": "chat"
                }

                response = requests.post(url, headers=headers, json=data)
                response = response.json()

                return response['textResponse']
        
            else:
                if workspace_record['llm'] == 'openai':
                    llm = ChatOpenAI(model_name = "gpt-3.5-turbo", temperature = 0, api_key = workspace_record['llm_api_key'], )
                elif workspace_record['llm'] == 'ollama':
                    if workspace_record['llm_url']:
                        llm = ChatOllama(model = workspace_record['model'], base_url = workspace_record['llm_url'])
                    else:
                        llm = ChatOllama(model = workspace_record['model'])
                elif workspace_record['llm'] == 'groq':
                    llm = ChatGroq(model = workspace_record['model'], api_key = workspace_record['llm_api_key'])
                
                non_rag_prompt = workspace_record['system_prompt']
                
                qa_prompt = ChatPromptTemplate.from_messages(
                    [
                        ("system", non_rag_prompt),
                        MessagesPlaceholder("chat_history"),
                        ("human", "{input}"),
                    ]
                )
                runnable = qa_prompt | llm

                host = config("DATABASE_HOST")
                username = config("DATABASE_USERNAME")
                password = config("DATABASE_PASSWORD")

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
                        history_messages_key="chat_history",
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
                        history_messages_key="chat_history",        
                    )

                now = datetime.now()
                
                response = chain_with_history.invoke({'input': text}, config={
                    "configurable": {"session_id": session_id}
                })

                return response.content
        else:
            raise HTTPException(status_code = 404, detail = "An error occurred: bot doesn\'t exist")

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")
    
@jwt_token
@x_app_key
@x_super_team
@voice_router.post("/batch")
async def batch(request: Request, token: str = Form(...), session_id: str = Form(...), file: UploadFile = File(...)):
    try:
        db = await connect()
        tokens_collections = db['tokens']
        bots_collections = db['bots']
        configuration_collections = db['configuration']
        classifiers_collection = db['classifiers']

        company_id = request.headers.get('x-super-team')
        
        tokens_record = await tokens_collections.find_one({"token": token, "is_active": 1})

        if tokens_record:
            now = datetime.now()
            date_time = now.strftime("%d/%m/%Y %H:%M:%S")

            date_format = "%d/%m/%Y %H:%M:%S"
            created_date = datetime.strptime(date_time, date_format)
            expiry_date = datetime.strptime(tokens_record['expiry_date'], date_format)

            if expiry_date < created_date:
                raise HTTPException(status_code = 404, detail = "An error occurred: token has expired")
            
            bot_id = tokens_record['bot_id']
            workspace_id = tokens_record['workspace_id']
        else:
            raise HTTPException(status_code = 401, detail = "An error occurred: invalid 'token' parameter")
        
        configuration_record = await configuration_collections.find_one({
            "company_id": company_id, "bot_id": bot_id, "workspace_id": workspace_id, "is_active": 1
        })

        if not configuration_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: configuration doesn\'t exist")

        client_query = configuration_record['client_query']
        bot_response = configuration_record['bot_response'] 

        request_directory = os.path.join(os.getcwd(), "app", "audio", company_id, bot_id, workspace_id, session_id, "human")
        os.makedirs(request_directory, exist_ok=True)

        query = {
            "company_id": company_id,
            "bot_id": bot_id,
            "workspace_id": workspace_id,
            "model_type": "ASR",  # Model type should be ASR
            "is_active": "1"  # Model should be active
        }

        # Retrieve the document from the collection that matches the query
        document = await classifiers_collection.find_one(query)
        if document:
            asr_model_api_url = document.get('model_api_url')
        else:
            raise HTTPException(status_code = 404, detail = "model_api_url is not found")

        current_time = datetime.now()

        uploaded_file_name = f"{uuid.uuid4()}.{file.filename.split('.')[-1]}"
        uploaded_file_path = os.path.join(request_directory, uploaded_file_name)

        with open(uploaded_file_path, "wb") as audio_buffer:
            audio_buffer.write(await file.read())

        bots_record = await bots_collections.find_one({'bot_id': bot_id})

        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)
        
        messages_collections = db['messages']
        profiles_collections = db['profiles']

        profiles_record = await profiles_collections.find_one({'session_id': session_id})
        if profiles_record['preference'] == language_english:
            language = language_english
        elif profiles_record['preference'] == language_arabic:
            language = language_arabic

        ASR_DB_file_path = uploaded_file_path.split("/")[-1]

        ASR_LANGUAGES = {language_english: "en", language_arabic: "ar"}

        transcription_response = transcribe_audio(uploaded_file_path, ASR_LANGUAGES.get(language), asr_model_api_url)

        if not transcription_response or 'transcription' not in transcription_response:
            raise HTTPException(status_code=500, detail="Transcription failed")
        
        transcription_text = transcription_response["transcription"]
        current_time_str = current_time.strftime("%d/%m/%Y %H:%M:%S")

        message_record = await messages_collections.find_one({'session_id': session_id})

        now = datetime.now()
        human_time = now.strftime("%d/%m/%Y %H:%M:%S")

        input_tokens = len(enc.encode(transcription_text))

        asr_attachments = {
        "file": ASR_DB_file_path,
        "type": "audio"
         }

        message_record['roles'].append({
            "type": 'human',
            "text": transcription_text,
            "timestamp": human_time,
            "input_tokens": input_tokens,
            "sentiment": None,
            "attachments": asr_attachments,
            'id': str(uuid.uuid4())
        })

        await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})
        await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"latest_timestamp": human_time}})

        profiles_record = await profiles_collections.find_one({'session_id': session_id})
        await profiles_collections.update_one({"_id": profiles_record["_id"]}, {"$set": {"latest_timestamp": human_time}})

        if client_query:
            data = {
                'text': transcription_text
            }

            try:
                sentiment = requests.post(sentiment_url, headers = sentiment_headers, data = data, timeout = 8)
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

        result = await llm_response(transcription_text, session_id, token)

        TTS_LANGUAGES = {language_english: "English", language_arabic: "Arabic"}
        
        query = {
        "company_id": company_id,
        "bot_id": bot_id,
        "workspace_id": workspace_id,
        "model_type": "TTS",  # Model type should be ASR
        "is_active": "1"  # Model should be active
        }

        document = await classifiers_collection.find_one(query)
        # Retrieve the document from the collection that matches the query
        if document:
            tts_model_api_url = document.get("model_api_url")
        else:
            raise HTTPException(status_code = 404, detail = "model_api_url is not found")
        
        tts_audio_path = convert_text_to_speech(company_id, bot_id, workspace_id, session_id, result, TTS_LANGUAGES.get(language), tts_model_api_url)

        db_tts_audio_path = tts_audio_path.split("/")[-1]
        
        tts_attachments = {
        "file": db_tts_audio_path,
        "type": db_tts_audio_path.split(".")[-1]
        }

        message_record = await messages_collections.find_one({'session_id': session_id})

        now = datetime.now()
        bot_time = now.strftime("%d/%m/%Y %H:%M:%S")

        output_tokens = len(enc.encode(result))

        message_record['roles'].append({
            "type": 'ai-agent',
            "text": result,
            "timestamp": bot_time,
            "output_tokens": output_tokens,
            "sentiment": None,
            "attchemnts": tts_attachments,
            'id': str(uuid.uuid4())
        })

        await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})
        await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"latest_timestamp": bot_time}})

        profiles_record = await profiles_collections.find_one({'session_id': session_id})
        await profiles_collections.update_one({"_id": profiles_record["_id"]}, {"$set": {"latest_timestamp": bot_time}})

        if bot_response:
            data = {
                'text': result
            }

            try:
                sentiment = requests.post(sentiment_url, headers = sentiment_headers, data = data, timeout = 8)
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

        return FileResponse(tts_audio_path, media_type="audio/wav", filename=os.path.basename(tts_audio_path))
    
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")

@jwt_token
@x_app_key
@x_super_team
@voice_router.get("/chat/audio")
async def get_audio(
    request: Request   
):
    params = request.query_params
    session_id = params['session_id']
    token = params['token']
    file_name = params['file_name']
    audio_type = params['audio_type']

    db = await connect()
    tokens_collection = db['tokens']
    # Validate token
    token_record = await tokens_collection.find_one({"token": token, "is_active": 1})
    if not token_record:
        raise HTTPException(status_code=401, detail="Invalid token")

    bot_id = token_record['bot_id']
    workspace_id = token_record['workspace_id']

    company_id = request.headers.get('x-super-team')

    get_audio_file_path = os.path.join(os.getcwd(), "app", "audio", company_id, bot_id, workspace_id, session_id, audio_type, file_name)
    return FileResponse(get_audio_file_path, media_type="audio/wav", filename=os.path.basename(get_audio_file_path))