from datetime import datetime

from decorators.jwt import jwt_token
from decorators.key import x_app_key
from decorators.teams import x_super_team
from decouple import config
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from utilities.database import connect
from utilities.redis import delete_redis, enqueue, get_redis, view_queue
from utilities.time import current_time
from utilities.validation import check_required_fields, validate_token

agents_router = APIRouter()

slug_db = config("SLUG_DATABASE")
transfer_queue = config("TRANSFER_QUEUE")

@agents_router.post('/login')
@x_super_team
@x_app_key
@jwt_token
async def login(request: Request):
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
            bots_record, workspace_record, _ = result
        
        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)

        agents_collections = db['agents']        

        id = request.state.current_user
        email = request.state.current_email
        name = request.state.current_name

        agent_record = await agents_collections.find_one({'agent_id': id, 'agent_name': name, 'agent_email': email, 'is_active': 1})
        if agent_record:
            raise HTTPException(status_code = 400, detail = "An error occurred: agent is already active")

        date_time = current_time()

        document = {'agent_id': id, 'agent_email': email, "agent_name": name, "workspace_id": workspace_record['workspace_id'], 'is_active': 1, 'created_date': date_time, 'modified_date': date_time}  
        await agents_collections.insert_one(document)

        return JSONResponse(content={"detail": "Agent has been logged in."}, status_code = 200)

    except Exception as e:
        raise HTTPException(status_code = 500, detail=f"An error occurred: {str(e)}") from e
    
@agents_router.post('/logout')
@x_super_team
@x_app_key
@jwt_token
async def logout(request: Request):
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
            bots_record, workspace_record, configuration_record = result
        
        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)

        agents_collections = db['agents']        

        id = request.state.current_user
        email = request.state.current_email
        name = request.state.current_name

        date_time = current_time()

        agent_record = await agents_collections.find_one({'agent_id': id, 'agent_name': name, 'agent_email': email, "workspace_id": workspace_record['workspace_id'], 'is_active': 1})
        if not agent_record:
            raise HTTPException(status_code = 400, detail = "An error occurred: agent is already logged out")
        
        await agents_collections.update_one({"_id": agent_record["_id"]}, {"$set": {"is_active": 0}})
        await agents_collections.update_one({"_id": agent_record["_id"]}, {"$set": {"modified_date": date_time}})

        if configuration_record['auto_assignment']:
            redis = await get_redis()
            session_ids = await view_queue(f'{id}:{name}:{email}')
            await delete_redis(redis, f'{id}:{name}:{email}')
            
            for session_id in session_ids:
                transfer_queue_temp = transfer_queue + f":{workspace_record['bot_id']}:{workspace_record['workspace_id']}"
                await enqueue(session_id, transfer_queue_temp)

        return JSONResponse(content={"detail": "Agent has been logged out."}, status_code = 200)

    except Exception as e:
        raise HTTPException(status_code = 500, detail=f"An error occurred: {str(e)}") from e