from decouple import config
from datetime import datetime
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse

from decorators.jwt import jwt_token
from decorators.key import x_app_key
from decorators.teams import x_super_team
from utilities.database import connect
from utilities.validation import check_required_fields

csat_router = APIRouter()

slug_db = config("SLUG_DATABASE")

@csat_router.post('/create')
@x_super_team
@x_app_key
@jwt_token
async def create(request: Request):
    try:
        data = await request.form()

        required_fields = ['email', 'session_id', 'score', 'token']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")

        email, session_id, score, token = data.get('email'), data.get('session_id'), data.get('score'), data.get('token')
        
        company_id = request.headers.get('x-super-team')
        user = request.state.current_user

        db = await connect()

        bots_collections = db['bots']
        workspace_collections = db['workspace']
        tokens_collections = db['tokens']

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

        csat_collections = db['csat']

        document = {
            'company_id': company_id, 'bot_id': bot_id, "workspace_id": workspace_id, "email": email, "session_id": session_id, "score": score, 
            'is_active': 1, 'created_date': date_time, 'modified_date': date_time, 'created_by': user, 'modified_by': user
        }  
        
        await csat_collections.insert_one(document)

        return JSONResponse(content={"detail": f"CSAT has been created."}, status_code = 200)
            
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")