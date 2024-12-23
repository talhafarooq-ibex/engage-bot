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
            raise HTTPException(status_code = 400, detail = "An error occurred: missing parameter(s)")

        email, session_id, score, token = data.get('email'), data.get('session_id'), data.get('score'), data.get('token')
        
        company_id = request.headers.get('x-super-team')
        user = request.state.current_user

        result = await validate_token(token)
        if not result:
            raise HTTPException(status_code = 400, detail = "An error occurred: invalid parameter(s)")
        else:
            bots_record, workspace_record, _ = result

        date_time = current_time()
        
        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)

        csat_collections = db['csat']

        document = {
            'company_id': company_id, 'bot_id': workspace_record['bot_id'], "workspace_id": workspace_record['workspace_id'], "email": email, "session_id": session_id, 
            "score": score, 'is_active': 1, 'created_date': date_time, 'modified_date': date_time, 'created_by': user, 'modified_by': user
        }  
        
        await csat_collections.insert_one(document)

        return JSONResponse(content={"detail": "CSAT has been created."}, status_code = 200)
            
    except Exception as e:
        raise HTTPException(status_code = 500, detail=f"An error occurred: {str(e)}") from e