import string, secrets
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse
from datetime import datetime

from decorators.jwt import jwt_token
from decorators.key import x_app_key
from decorators.teams import x_super_team
from utilities.database import connect
from utilities.validation import check_required_fields

tokens_router = APIRouter()

@tokens_router.get('/get')
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
        tokens_collections = db['tokens']
            
        bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 1})
        if not bots_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: bot doesn\'t exist")
        
        workspace_record = await workspace_collections.find_one({"company_id": company_id, "bot_id": bot_id, 'workspace_id': workspace_id})
        
        if not workspace_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: workspace doesn\'t exist for this bot")
        
        tokens_record = await tokens_collections.find_one({"company_id": company_id, "bot_id": bot_id, "workspace_id": workspace_id, "is_active": 1})

        token_details = {'token': tokens_record['token'], 'expiry': str(tokens_record['expiry_date'])}

        if tokens_record:     
            return JSONResponse(token_details, status_code = 200)
        else:
            raise HTTPException(status_code = 404, detail = "An error occurred: token doesn\'t exist for this workspace")
            
    except HTTPException as e:
            raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")

@tokens_router.post("/regenerate")
@x_super_team
@x_app_key
@jwt_token
async def regenerate(request: Request):
    try:
        data = await request.form()

        required_fields = ['bot_id', 'workspace_id', 'expiry_date']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")
        
        bot_id, workspace_id, expiry_date = data.get('bot_id'), data.get('workspace_id'), data.get('expiry_date')

        company_id = request.headers.get('x-super-team')
        user = request.state.current_user

        db = await connect()

        bots_collections = db['bots']
        tokens_collections = db['tokens']
        workspace_collections = db['workspace']

        bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 1})

        if not bots_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: bot doesn\'t exist")

        workspace_record = await workspace_collections.find_one({"company_id": company_id, "bot_id": bot_id, 'workspace_id': workspace_id})
        
        if not workspace_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: workspace doesn\'t exist for this bot")
        
        now = datetime.now()
        date_time = now.strftime("%d/%m/%Y %H:%M:%S")

        date_format = "%d/%m/%Y %H:%M:%S"
        created_date = datetime.strptime(date_time, date_format)
        expiry_date = datetime.strptime(expiry_date, date_format)

        if expiry_date < created_date:
            raise HTTPException(status_code = 401, detail = "An error occurred: expiry date cannot be less than current time")
        
        expiry_date = expiry_date.strftime(date_format)
        
        tokens_record = await tokens_collections.find_one({"company_id": company_id, "bot_id": bot_id, "workspace_id": workspace_id, "is_active": 1})

        alphabet = string.ascii_letters + string.digits
        token = ''.join(secrets.choice(alphabet) for _ in range(64))

        if tokens_record:
            await tokens_collections.update_one({"_id": tokens_record["_id"]}, {"$set": {"is_active": 0}})
            await tokens_collections.update_one({"_id": tokens_record["_id"]}, {"$set": {"modified_date": date_time}})

        document = {
            "company_id": company_id, 'bot_id': bot_id, "workspace_id": workspace_id, 'token': token, 'expiry_date': expiry_date, 
            'is_active': 1, 'created_date': date_time, 'modified_date': date_time, 'created_by': user, 'modified_by': user
        }
        
        await tokens_collections.insert_one(document) 
        
        await bots_collections.update_one({"_id": bots_record["_id"]}, {"$set": {"modified_date": date_time}})
        await bots_collections.update_one({"_id": bots_record["_id"]}, {"$set": {"modified_by": user}})
                
        return JSONResponse(content={"detail": f"Token has been created."}, status_code = 200)

    except HTTPException as e:
            raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")