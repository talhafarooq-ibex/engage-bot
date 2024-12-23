import pymongo, openai, os, string, secrets
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse
from datetime import datetime, timedelta

from decorators.jwt import jwt_token
from decorators.key import x_app_key
from decorators.teams import x_super_team
from utilities.database import connect
from utilities.validation import process_name, check_required_fields

SUPPORTED_LLMS = ['ollama', 'openai', 'groq', 'anythingllm']
SUPPORTED_EMBEDDINGS = ['ollama', 'openai', 'huggingface']
SUPPORTED_VDB = ['chroma', 'faiss', 'lancedb']

workspaces_router = APIRouter()

@workspaces_router.get('/get/all')
@x_super_team
@x_app_key
@jwt_token
async def get_all(request: Request):
    try:
        data = request.query_params

        required_fields = ['bot_id']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")
        
        bot_id = data.get('bot_id')

        company_id = request.headers.get('x-super-team')

        db = await connect()

        bots_collections = db['bots']
        workspace_collections = db['workspace']
            
        bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 1})
        workspace_record = await workspace_collections.find({"company_id": company_id, "bot_id": bot_id, "is_active": 1}).to_list(length=None)

        if not bots_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: bot doesn\'t exist")
        
        if not workspace_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: no workspace found for this bot or key doesn't exist")
            
        result = []

        for record in workspace_record:
            record.pop('is_active')
            record.pop('created_date')
            record.pop('modified_date')
            record.pop('created_by')
            record.pop('modified_by')
            record.pop('_id')

            result.append(record)

        return JSONResponse(result, status_code = 200)

    except HTTPException as e:
            raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")

@workspaces_router.get('/get')
@x_super_team
@x_app_key
@jwt_token
async def get(request: Request):
    try:
        data = request.query_params

        required_fields = ['bot_id', 'workspace_id']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")
        
        bot_id, workspace_id = data.get('bot_id'), data.get('workspace_id')

        company_id = request.headers.get('x-super-team')

        db = await connect()

        bots_collections = db['bots']
        workspace_collections = db['workspace']
            
        bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 1})
        workspace_record = await workspace_collections.find_one({"company_id": company_id, "bot_id": bot_id, "workspace_id": workspace_id, "is_active": 1})

        if not bots_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: bot doesn\'t exist")
        
        if not workspace_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: no workspace found for this bot or key doesn't exist")

        workspace_record.pop('is_active')
        workspace_record.pop('created_date')
        workspace_record.pop('modified_date')
        workspace_record.pop('created_by')
        workspace_record.pop('modified_by')
        workspace_record.pop('_id')

        return JSONResponse(workspace_record, status_code = 200)

    except HTTPException as e:
            raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")
    
@workspaces_router.post('/create')
@x_super_team
@x_app_key
@jwt_token
async def create(request: Request):
    try:
        data = await request.form()

        required_fields = ['bot_id', 'llm', 'workspace_name', 'chat_limit', 'sessions_limit']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")
                
        bot_id, llm, embeddings, vectordb = data.get('bot_id'), data.get('llm'), data.get('embeddings'), data.get('vectordb')
        workspace_name, chat_limit, sessions_limit = data.get('workspace_name'), data.get('chat_limit'), data.get('sessions_limit')
        llm_api_key, model, llm_url  = data.get('llm_api_key'), data.get('model'), data.get('llm_url')
        embeddings, embeddings_api_key, embeddings_url = data.get('embeddings'), data.get('embeddings_api_key'), data.get('embeddings_url')
        embeddings_model, vector_db_url, vector_db_api_key = data.get('embeddings_model'), data.get('vector_db_url'), data.get('vector_db_api_key')
        vectordb, system_prompt, chat_limit = data.get('vectordb'), data.get('system_prompt'), data.get('chat_limit')
        k_retreive, llm_temperature = data.get('k_retreive'), data.get('llm_temperature')

        company_id = request.headers.get('x-super-team')
        user = request.state.current_user

        workspace_name = process_name(workspace_name, 1) 

        db = await connect()

        bots_collections = db['bots']
        workspace_collections = db['workspace']
        tokens_collections = db['tokens']
            
        bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 1})

        if not bots_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: bot doesn\'t exist")
            
        if llm not in SUPPORTED_LLMS:
            raise HTTPException(status_code = 400, detail = "An error occurred: invalid 'llm' parameter")
        if embeddings and embeddings not in SUPPORTED_EMBEDDINGS:
            raise HTTPException(status_code = 400, detail = "An error occurred: invalid 'embeddings' parameter")
        if vectordb and vectordb not in SUPPORTED_VDB:
            raise HTTPException(status_code = 400, detail = "An error occurred: invalid 'vectordb' parameter")
        
        workspace_record = await workspace_collections.find_one({"company_id": company_id, "bot_id": bot_id, 'workspace_name': workspace_name})
        if workspace_record:
            return JSONResponse(content={"detail": f"Workspace name already exists."}, status_code = 400)

        if llm == 'openai':
            required_fields = ['model', 'llm_api_key']
            if not check_required_fields(data, required_fields):
                raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")   

            client = openai.OpenAI(api_key = llm_api_key)
            try:
                client.models.list()
            except openai.AuthenticationError:
                raise HTTPException(status_code = 400, detail = "An error occurred: invalid openai llm key")
            
        elif llm == 'ollama':
            llm_api_key = None
            
            required_fields = ['model']
            if not check_required_fields(data, required_fields):
                raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")   

        elif llm == 'groq':
            required_fields = ['model', 'llm_api_key']
            if not check_required_fields(data, required_fields):
                raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")                    

        elif llm == 'anythingllm':
            required_fields = ['model', 'llm_api_key', 'llm_url']
            if not check_required_fields(data, required_fields):
                raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)") 

            now = datetime.now()
            date_time = now.strftime("%d/%m/%Y %H:%M:%S")

            workspace_record = await workspace_collections.find_one({'bot_id': bot_id}, sort=[("_id", pymongo.DESCENDING)])
            if workspace_record:
                workspace_id = f"{int(workspace_record['workspace_id']) + 1}"
            else:
                workspace_id = '1'

            if os.path.exists(f'library/{company_id}/{bot_id}/{workspace_id}'):
                os.rmdir(f'library/{company_id}/{bot_id}/{workspace_id}/documents')
                os.rmdir(f'library/{company_id}/{bot_id}/{workspace_id}/embeddings')

            os.makedirs(f'library/{company_id}/{bot_id}/{workspace_id}/documents')  
            os.makedirs(f'library/{company_id}/{bot_id}/{workspace_id}/embeddings')

            document = {
                'company_id': company_id, 'bot_id': bot_id, 'workspace_id': workspace_id, 'workspace_name': workspace_name, 'llm': llm, 
                'model': model, 'llm_api_key': llm_api_key, 'llm_url': llm_url, 'embeddings': embeddings, 'embeddings_api_key': embeddings_api_key, 
                'embeddings_url': embeddings_url, 'vectordb': vectordb, 'vector_db_url': vector_db_url, 'vector_db_api_key': vector_db_api_key, 
                'system_prompt': system_prompt, 'k_retreive': k_retreive, 'llm_temperature': llm_temperature, 'chat_limit': chat_limit, 'sessions_limit': sessions_limit, 'is_active': 1, 
                'created_date': date_time, 'modified_date': date_time, 'created_by': user, 'modified_by': user, 'embeddings_model': embeddings_model
            }

            await workspace_collections.insert_one(document)

            now = datetime.now()
            date_time = now.strftime("%d/%m/%Y %H:%M:%S")

            date_format = "%d/%m/%Y %H:%M:%S"
            created_date = datetime.strptime(date_time, date_format)
            expiry_date = created_date + timedelta(days=7)
            expiry_date = expiry_date.strftime(date_format)

            alphabet = string.ascii_letters + string.digits
            token = ''.join(secrets.choice(alphabet) for _ in range(64))

            document = {
                "company_id": company_id, 'bot_id': bot_id, "workspace_id": workspace_id, 'token': token, 'expiry_date': expiry_date, 
                'is_active': 1, 'created_date': date_time, 'modified_date': date_time, 'created_by': user, 'modified_by': user
                }
            
            await tokens_collections.insert_one(document) 
            
            await bots_collections.update_one({"_id": bots_record["_id"]}, {"$set": {"modified_date": date_time}})
            await bots_collections.update_one({"_id": bots_record["_id"]}, {"$set": {"modified_by": user}})
            
            return JSONResponse(content={"detail": f"Workspace has been created."}, status_code = 200)

        if embeddings:
            if embeddings_model == 'text-embedding-3-small' or embeddings_model == 'text-embedding-3-large':
                required_fields = ['embeddings_api_key']
                if not check_required_fields(data, required_fields):
                    raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")   

                client = openai.OpenAI(api_key = embeddings_api_key)
                try:
                    client.models.list()
                except openai.AuthenticationError:
                    raise HTTPException(status_code = 401, detail = "An error occurred: invalid openai embeddings key")

        # if vectordb:
        #     required_fields = ['vector_db_url', 'vector_db_api_key']
        #     if not check_required_fields(data, required_fields):
        #         raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")

        if not data.get('system_prompt'):
            system_prompt = (
                "Given the following conversation, relevant context, and a follow up question, "
                "reply with an answer to the current question the user is asking. "
                "Return only your response to the question given the above information following "
                "the users instructions as needed. If you do not find the answer in retrieved context, "
                "or the previous conversation, use your own knowledge to answer the question."
            )

        required_fields = ['k_retreive', 'llm_temperature']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")  

        now = datetime.now()
        date_time = now.strftime("%d/%m/%Y %H:%M:%S")

        workspace_record = await workspace_collections.find_one({'bot_id': bot_id}, sort=[("_id", pymongo.DESCENDING)])
        if workspace_record:
            workspace_id = f"{int(workspace_record['workspace_id']) + 1}"
        else:
            workspace_id = '1'

        if os.path.exists(f'library/{company_id}/{bot_id}/{workspace_id}'):
            os.rmdir(f'library/{company_id}/{bot_id}/{workspace_id}/documents')
            os.rmdir(f'library/{company_id}/{bot_id}/{workspace_id}/embeddings')

        os.makedirs(f'library/{company_id}/{bot_id}/{workspace_id}/documents')  
        os.makedirs(f'library/{company_id}/{bot_id}/{workspace_id}/embeddings')

        document = {
            'company_id': company_id, 'bot_id': bot_id, 'workspace_id': workspace_id, 'workspace_name': workspace_name, 'llm': llm, 
            'model': model, 'llm_api_key': llm_api_key, 'llm_url': llm_url, 'embeddings': embeddings, 'embeddings_model': embeddings_model,
            'embeddings_api_key': embeddings_api_key, 'embeddings_url': embeddings_url, 'vectordb': vectordb, 'vector_db_url': vector_db_url, 
            'vector_db_api_key': vector_db_api_key, 'system_prompt': system_prompt, 'k_retreive': k_retreive, 'llm_temperature': llm_temperature, 
            'chat_limit': chat_limit, 'sessions_limit': sessions_limit, 'is_active': 1, 'created_date': date_time, 'modified_date': date_time, 
            'created_by': user, 'modified_by': user
        }
        
        await workspace_collections.insert_one(document)

        now = datetime.now()
        date_time = now.strftime("%d/%m/%Y %H:%M:%S")

        date_format = "%d/%m/%Y %H:%M:%S"
        created_date = datetime.strptime(date_time, date_format)
        expiry_date = created_date + timedelta(days=7)
        expiry_date = expiry_date.strftime(date_format)

        alphabet = string.ascii_letters + string.digits
        token = ''.join(secrets.choice(alphabet) for _ in range(64))

        document = {"company_id": company_id, 'bot_id': bot_id, "workspace_id": workspace_id, 'token': token, 'expiry_date': expiry_date, 'is_active': 1, 'created_date': date_time, 'modified_date': date_time, 'created_by': user, 'modified_by': user}
        await tokens_collections.insert_one(document) 
        
        await bots_collections.update_one({"_id": bots_record["_id"]}, {"$set": {"modified_date": date_time}})
        await bots_collections.update_one({"_id": bots_record["_id"]}, {"$set": {"modified_by": user}})
        
        return JSONResponse(content={"detail": f"Workspace has been created."}, status_code = 200)
            
    except HTTPException as e:
            raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")
    
@workspaces_router.post('/update')
@x_super_team
@x_app_key
@jwt_token
async def update(request: Request):
    try:
        data = await request.form()
        company_id = request.headers.get('x-super-team')
        user = request.state.current_user

        required_fields = ['bot_id', 'workspace_id']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = "An error occurred: missing parameter(s)")

        bot_id = data.get('bot_id')
        workspace_id = data.get('workspace_id')

        db = await connect()
        workspace_collections = db['workspace']
        bots_collections = db['bots']

        workspace_record = await workspace_collections.find_one({
            "company_id": company_id,
            "bot_id": bot_id,
            "workspace_id": workspace_id
        })

        if not workspace_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: invalid parameter(s)")

        updatable_fields = [
            'llm', 'model', 'llm_api_key', 'llm_url', 'embeddings', 'embeddings_api_key', 'embeddings_model',
            'embeddings_url', 'vectordb', 'vector_db_url', 'vector_db_api_key', 'k_retreive',
            'system_prompt', 'chat_limit', 'sessions_limit', 'llm_temperature'
        ]

        update_data = {
            key: data.get(key, workspace_record.get(key))
            for key in updatable_fields
        }

        if 'llm' in data and data['llm'] not in SUPPORTED_LLMS:
            raise HTTPException(status_code = 400, detail = "An error occurred: invalid 'llm' parameter.")

        if 'embeddings' in data and data['embeddings'] not in SUPPORTED_EMBEDDINGS:
            raise HTTPException(status_code = 400, detail = "An error occurred: invalid 'embeddings' parameter.")

        if 'vectordb' in data and data['vectordb'] not in SUPPORTED_VDB:
            raise HTTPException(status_code = 400, detail = "An error occurred: invalid 'vectordb' parameter.")

        update_data['modified_date'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        update_data['modified_by'] = user

        await workspace_collections.update_one(
            {"_id": workspace_record["_id"]},
            {"$set": update_data}
        )

        await bots_collections.update_one(
            {"company_id": company_id, "bot_id": bot_id},
            {"$set": {"modified_date": update_data['modified_date'], "modified_by": user}}
        )

        return JSONResponse(content={"detail": "Workspace has been updated."}, status_code=200)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@workspaces_router.post('/disable')
@x_super_team
@x_app_key
@jwt_token
async def disable(request: Request):
    try:
        data = await request.form()

        required_fields = ['bot_id', 'workspace_id']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")

        bot_id, workspace_id = data.get('bot_id'), data.get('workspace_id')

        company_id = request.headers.get('x-super-team')
        user = request.state.current_user

        db = await connect()
    
        bots_collections = db['bots']
        workspace_collections = db['workspace']

        bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 1})
        workspace_record = await workspace_collections.find({"company_id": company_id, "bot_id": bot_id, "is_active": 1}).to_list(length=None)

        if not bots_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: bot doesn\'t exist")
        
        if not workspace_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: no workspace found for this bot or key doesn't exist")

        now = datetime.now()
        date_time = now.strftime("%d/%m/%Y %H:%M:%S")

        await workspace_collections.update_one({"_id": workspace_record["_id"]}, {"$set": {"is_active": 0}})
        await workspace_collections.update_one({"_id": workspace_record["_id"]}, {"$set": {"modified_date": date_time}})
        await workspace_collections.update_one({"_id": workspace_record["_id"]}, {"$set": {"modified_by": user}})

        await bots_collections.update_one({"_id": bots_record["_id"]}, {"$set": {"modified_date": date_time}})
        await bots_collections.update_one({"_id": bots_record["_id"]}, {"$set": {"modified_by": user}})

        return JSONResponse(content={"detail": f"workspace has been disabled."}, status_code = 200)

    except HTTPException as e:
            raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")