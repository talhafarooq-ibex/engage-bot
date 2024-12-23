from decouple import config
from datetime import datetime
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse

from decorators.jwt import jwt_token
from decorators.key import x_app_key
from decorators.teams import x_super_team
from utilities.database import connect
from utilities.redis import delete_redis, get_redis, view_queue, enqueue
from utilities.validation import check_required_fields

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
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")
        
        token = data.get('token')

        company_id = request.headers.get('x-super-team')

        db = await connect()

        tokens_collections = db['tokens']
        bots_collections = db['bots']
        workspace_collections = db['workspace']

        tokens_record = await tokens_collections.find_one({"token": token, "is_active": 1})

        if not tokens_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: invalid 'token' parameter")
        
        now = datetime.now()
        date_time = now.strftime("%d/%m/%Y %H:%M:%S")

        date_format = "%d/%m/%Y %H:%M:%S"
        created_date = datetime.strptime(date_time, date_format)
        expiry_date = datetime.strptime(tokens_record['expiry_date'], date_format)

        if expiry_date < created_date:
            raise HTTPException(status_code = 401, detail = "An error occurred: token has expired")
        
        bot_id = tokens_record['bot_id']
        workspace_id = tokens_record['workspace_id']

        bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 1})
        if not bots_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: bot doesn\'t exist")
        
        workspace_record = await workspace_collections.find_one({
            "company_id": company_id, "bot_id": bot_id, "workspace_id": workspace_id, "is_active": 1
        })
        
        if not workspace_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: no workspace found for this bot or key doesn't exist")
        
        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)

        agents_collections = db['agents']        

        id = request.state.current_user
        email = request.state.current_email
        name = request.state.current_name

        agent_record = await agents_collections.find_one({'agent_id': id, 'agent_name': name, 'agent_email': email, 'is_active': 1})
        if agent_record:
            raise HTTPException(status_code = 400, detail = "An error occurred: agent is already active")

        now = datetime.now()
        date_time = now.strftime("%d/%m/%Y %H:%M:%S")

        document = {'agent_id': id, 'agent_email': email, "agent_name": name, "workspace_id": workspace_id, 'is_active': 1, 'created_date': date_time, 'modified_date': date_time}  
        await agents_collections.insert_one(document)

        return JSONResponse(content={"detail": f"Agent has been logged in."}, status_code = 200)

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")
    
@agents_router.post('/logout')
@x_super_team
@x_app_key
@jwt_token
async def logout(request: Request):
    try:        
        data = await request.form()

        required_fields = ['token']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")
        
        token = data.get('token')
        
        company_id = request.headers.get('x-super-team')

        db = await connect()

        tokens_collections = db['tokens']
        bots_collections = db['bots']
        workspace_collections = db['workspace']
        configurations_collections = db['configuration']

        tokens_record = await tokens_collections.find_one({"token": token, "is_active": 1})

        if not tokens_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: invalid 'token' parameter")
        
        now = datetime.now()
        date_time = now.strftime("%d/%m/%Y %H:%M:%S")

        date_format = "%d/%m/%Y %H:%M:%S"
        created_date = datetime.strptime(date_time, date_format)
        expiry_date = datetime.strptime(tokens_record['expiry_date'], date_format)

        if expiry_date < created_date:
            raise HTTPException(status_code = 401, detail = "An error occurred: token has expired")
        
        bot_id = tokens_record['bot_id']
        workspace_id = tokens_record['workspace_id']

        bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 1})
        if not bots_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: bot doesn\'t exist")
        
        workspace_record = await workspace_collections.find_one({
            "company_id": company_id, "bot_id": bot_id, "workspace_id": workspace_id, "is_active": 1
        })
        
        if not workspace_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: no workspace found for this bot or key doesn't exist")
        
        configuration_record = await configurations_collections.find_one({'bot_id': bot_id, 'workspace_id': workspace_id})
        
        if not configuration_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: bot configurations doesn\'t exist")
        
        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)

        agents_collections = db['agents']        

        id = request.state.current_user
        email = request.state.current_email
        name = request.state.current_name

        now = datetime.now()
        date_time = now.strftime("%d/%m/%Y %H:%M:%S")

        agent_record = await agents_collections.find_one({'agent_id': id, 'agent_name': name, 'agent_email': email, "workspace_id": workspace_id, 'is_active': 1})
        if not agent_record:
            raise HTTPException(status_code = 400, detail = "An error occurred: agent is already logged out")
        
        await agents_collections.update_one({"_id": agent_record["_id"]}, {"$set": {"is_active": 0}})
        await agents_collections.update_one({"_id": agent_record["_id"]}, {"$set": {"modified_date": date_time}})

        if configuration_record['auto_assignment']:
            redis = await get_redis()
            session_ids = await view_queue(f'{id}:{name}:{email}')
            await delete_redis(redis, f'{id}:{name}:{email}')
            
            for session_id in session_ids:
                transfer_queue = transfer_queue + f':{bot_id}:{workspace_id}'
                await enqueue(session_id, transfer_queue)

        return JSONResponse(content={"detail": f"Agent has been logged out."}, status_code = 200)

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")