from datetime import datetime

from decorators.jwt import jwt_token
from decorators.key import x_app_key
from decorators.teams import x_super_team
from decouple import config
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from utilities.database import connect
from utilities.time import current_time
from utilities.validation import check_required_fields, validate_token

configuration_router = APIRouter()

slug_db = config("SLUG_DATABASE")

@configuration_router.post('/create')
@x_super_team
@x_app_key
@jwt_token
async def create(request: Request):
    try:
        data = await request.form()

        required_fields = ['suggestion', 'summary', 'token', 'client_query', 'bot_response', 'agent', 'auto_assignment', 'conversation']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = "An error occurred: missing parameter(s)")
        
        token, summary, suggestion  = data.get('token'), int(data.get('summary')), int(data.get('suggestion')), 
        client_query, bot_response, agent = int(data.get('client_query')), int(data.get('bot_response')), int(data.get('agent')) 
        auto_assignment, conversation = int(data.get('auto_assignment')), int(data.get('conversation'))

        company_id = request.headers.get('x-super-team')
        user = request.state.current_user

        result = await validate_token(token)
        if not result:
            raise HTTPException(status_code = 400, detail = "An error occurred: invalid parameter(s)")
        else:
            bots_record, workspace_record, _ = result

        db = await connect()
        configuration_collections = db['configuration']
        bots_collections = db['bots']

        date_time = current_time()

        document = {
            'company_id': company_id, 'bot_id': workspace_record['bot_id'], "workspace_id": workspace_record['workspace_id'], "summary": summary, "suggestion": suggestion, 
            "auto_assignment": auto_assignment, "client_query": client_query, "bot_response": bot_response, "agent": agent, 
            "conversation": conversation, 'is_active': 1, 'created_date': date_time, 'modified_date': date_time, 'created_by': user, 'modified_by': user
        }  

        await configuration_collections.insert_one(document)

        await bots_collections.update_one({"_id": bots_record["_id"]}, {"$set": {"modified_date": date_time}})
        await bots_collections.update_one({"_id": bots_record["_id"]}, {"$set": {"modified_by": user}})

        return JSONResponse(content={"detail": "Configuration has been set."}, status_code = 200)

    except Exception as e:
        raise HTTPException(status_code = 500, detail=f"An error occurred: {str(e)}") from e
    
@configuration_router.get('/get')
@x_super_team
@x_app_key
@jwt_token
async def get(request: Request):
    try:
        data = await request.form()

        required_fields = ['token']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = "An error occurred: missing parameter(s)")
        
        token = data.get('token')
        
        result = await validate_token(token)
        if not result:
            raise HTTPException(status_code = 400, detail = "An error occurred: invalid parameter(s)")
        else:
            _, _, configuration_record = result
            
        configuration_record.pop('company_id')
        configuration_record.pop('bot_id')
        configuration_record.pop('workspace_id')
        configuration_record.pop('is_active')
        configuration_record.pop('created_date')
        configuration_record.pop('modified_date')
        configuration_record.pop('created_by')
        configuration_record.pop('modified_by')
        configuration_record.pop('_id')
        
        return JSONResponse(configuration_record, status_code = 200)

    except Exception as e:
        raise HTTPException(status_code = 500, detail=f"An error occurred: {str(e)}") from e
    
@configuration_router.post('/update')
@x_super_team
@x_app_key
@jwt_token
async def update(request: Request):
    try:
        data = await request.form()

        required_fields = ['suggestion', 'summary', 'token', 'client_query', 'bot_response', 'agent', 'auto_assignment', 'conversation']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = "An error occurred: missing parameter(s)")
        
        token, summary, suggestion  = data.get('token'), int(data.get('summary')), int(data.get('suggestion')), 
        client_query, bot_response, agent = int(data.get('client_query')), int(data.get('bot_response')), int(data.get('agent')) 
        auto_assignment, conversation = int(data.get('auto_assignment')), int(data.get('conversation'))

        user = request.state.current_user

        result = await validate_token(token)
        if not result:
            raise HTTPException(status_code = 400, detail = "An error occurred: invalid parameter(s)")
        else:
            bots_record, _, configuration_record = result

        db = await connect()
        configuration_collections = db['configuration']
        bots_collections = db['bots']

        date_time = current_time()
        
        await configuration_collections.update_one({"_id": configuration_record["_id"]}, {"$set": {"summary": summary}})
        await configuration_collections.update_one({"_id": configuration_record["_id"]}, {"$set": {"suggestion": suggestion}})
        await configuration_collections.update_one({"_id": configuration_record["_id"]}, {"$set": {"auto_assignment": auto_assignment}})
        await configuration_collections.update_one({"_id": configuration_record["_id"]}, {"$set": {"client_query": client_query}})
        await configuration_collections.update_one({"_id": configuration_record["_id"]}, {"$set": {"bot_response": bot_response}})
        await configuration_collections.update_one({"_id": configuration_record["_id"]}, {"$set": {"agent": agent}})
        await configuration_collections.update_one({"_id": configuration_record["_id"]}, {"$set": {"conversation": conversation}})
        await configuration_collections.update_one({"_id": configuration_record["_id"]}, {"$set": {"modified_date": date_time}})
        await configuration_collections.update_one({"_id": configuration_record["_id"]}, {"$set": {"modified_by": user}})

        await bots_collections.update_one({"_id": bots_record["_id"]}, {"$set": {"modified_date": date_time}})
        await bots_collections.update_one({"_id": bots_record["_id"]}, {"$set": {"modified_by": user}})

        return JSONResponse(content={"detail": "Configuration has been updated."}, status_code = 200)

    except Exception as e:
        raise HTTPException(status_code = 500, detail=f"An error occurred: {str(e)}") from e