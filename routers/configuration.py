from decouple import config
from datetime import datetime
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse

from decorators.jwt import jwt_token
from decorators.key import x_app_key
from decorators.teams import x_super_team
from utilities.database import connect
from utilities.validation import check_required_fields

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
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")
        
        token, summary, suggestion  = data.get('token'), int(data.get('summary')), int(data.get('suggestion')), 
        client_query, bot_response, agent = int(data.get('client_query')), int(data.get('bot_response')), int(data.get('agent')) 
        auto_assignment, conversation = int(data.get('auto_assignment')), int(data.get('conversation'))

        company_id = request.headers.get('x-super-team')
        user = request.state.current_user

        db = await connect()

        bots_collections = db['bots']
        workspace_collections = db['workspace']
        tokens_collections = db['tokens']
        configuration_collections = db['configuration']

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

        document = {
            'company_id': company_id, 'bot_id': bot_id, "workspace_id": workspace_id, "summary": summary, "suggestion": suggestion, 
            "auto_assignment": auto_assignment, "client_query": client_query, "bot_response": bot_response, "agent": agent, 
            "conversation": conversation, 'is_active': 1, 'created_date': date_time, 'modified_date': date_time, 'created_by': user, 'modified_by': user
        }  

        await configuration_collections.insert_one(document)

        return JSONResponse(content={"detail": f"Configuration has been set."}, status_code = 200)

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")
    
@configuration_router.get('/get')
@x_super_team
@x_app_key
@jwt_token
async def get(request: Request):
    try:
        data = await request.form()

        required_fields = ['token']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")
        
        token = data.get('token')
        
        company_id = request.headers.get('x-super-team')

        db = await connect()

        bots_collections = db['bots']
        workspace_collections = db['workspace']
        tokens_collections = db['tokens']
        configuration_collections = db['configuration']

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

        configuration_record = await configuration_collections.find_one({
            "company_id": company_id, "bot_id": bot_id, "workspace_id": workspace_id, "is_active": 1
        })
        
        if not configuration_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: summary configuration doesn\'t exist")
            
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

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")
    
@configuration_router.post('/update')
@x_super_team
@x_app_key
@jwt_token
async def update(request: Request):
    try:
        data = await request.form()

        required_fields = ['suggestion', 'summary', 'token', 'client_query', 'bot_response', 'agent', 'auto_assignment', 'conversation']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")
        
        token, summary, suggestion  = data.get('token'), int(data.get('summary')), int(data.get('suggestion')), 
        client_query, bot_response, agent = int(data.get('client_query')), int(data.get('bot_response')), int(data.get('agent')) 
        auto_assignment, conversation = int(data.get('auto_assignment')), int(data.get('conversation'))

        company_id = request.headers.get('x-super-team')
        user = request.state.current_user

        db = await connect()

        bots_collections = db['bots']
        workspace_collections = db['workspace']
        tokens_collections = db['tokens']
        configuration_collections = db['configuration']

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
        
        configuration_record = await configuration_collections.find_one({
            "company_id": company_id, "bot_id": bot_id, "workspace_id": workspace_id, "is_active": 1
        })
        
        if not configuration_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: summary configuration doesn\'t exist")
            
        now = datetime.now()
        date_time = now.strftime("%d/%m/%Y %H:%M:%S")
        
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

        return JSONResponse(content={"detail": f"Configuration has been updated."}, status_code = 200)

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")