import pymongo
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse
from datetime import datetime

from decorators.jwt import jwt_token
from decorators.key import x_app_key
from decorators.teams import x_super_team
from utilities.database import connect
from utilities.validation import check_required_fields, process_name

bots_router = APIRouter()

@bots_router.get('/get')
@x_super_team
@x_app_key
@jwt_token
async def get(request: Request):
        try:
            data = request.query_params

            required_fields = ['bot_id']
            if not check_required_fields(data, required_fields):
                raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")
            
            bot_id = data.get('bot_id')

            company_id = request.headers.get('x-super-team')

            db = await connect()

            bots_collections = db['bots']
                
            bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 1})
            if not bots_record:
                raise HTTPException(status_code = 404, detail = "An error occurred: invalid parameter(s)")
            
            bots_record.pop('is_active')
            bots_record.pop('created_date')
            bots_record.pop('modified_date')
            bots_record.pop('created_by')
            bots_record.pop('modified_by')
            bots_record.pop('_id')
                
            return JSONResponse(bots_record, status_code = 200)

        except HTTPException as e:
                raise e
        except Exception as e:
            raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")
        
@bots_router.get('/get/all')
@x_super_team
@x_app_key
@jwt_token
async def get_all(request: Request):
        try:
            company_id = request.headers.get('x-super-team')

            db = await connect()

            bots_collections = db['bots']
                
            bots_records = await bots_collections.find({"company_id": company_id, "is_active": 1}).to_list(length=None)
            if not bots_records:
                raise HTTPException(status_code = 404, detail = "An error occurred: invalid parameter(s)")
            
            result = []
            for record in bots_records:
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

@bots_router.post("/create")
@x_super_team
@x_app_key
@jwt_token
async def create(request: Request):
        try:
            data = await request.form()

            required_fields = ['bot_name', 'timeout']
            if not check_required_fields(data, required_fields):
                raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")
            
            name, timeout = data.get('bot_name'), data.get('timeout')

            name = process_name(name, 1)            

            company_id = request.headers.get('x-super-team')
            user = request.state.current_user

            db = await connect()

            bots_collections = db['bots']

            bots_record = await bots_collections.find_one({"bot_name": name})
            if bots_record:
                raise HTTPException(status_code = 400, detail = "An error occurred: invalid parameter(s)")

            bots_record = await bots_collections.find_one(sort=[("_id", pymongo.DESCENDING)])
            if bots_record:
                bot_id = str(int(bots_record['bot_id'])+1)
            else:
                bot_id = str(1) 

            now = datetime.now()
            date_time = now.strftime("%d/%m/%Y %H:%M:%S")

            document = {
                'company_id': company_id, 'bot_id': bot_id, "bot_name": name, 'timeout': timeout, 'is_active': 1, 
                'created_date': date_time, 'modified_date': date_time, 'created_by': user, 'modified_by': user
            }  
            
            await bots_collections.insert_one(document)
            return JSONResponse(content={"detail": f"Bot has been created."}, status_code = 200)

        except HTTPException as e:
                raise e
        except Exception as e:
            raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")
        
@bots_router.post('/disable')
@x_super_team
@x_app_key
@jwt_token
async def disable(request: Request):
    try:
        data = await request.form()

        required_fields = ['bot_id']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")

        bot_id = data.get('bot_id')

        company_id = request.headers.get('x-super-team')
        user = request.state.current_user

        db = await connect()
    
        bots_collections = db['bots']

        bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 1})
        if not bots_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: bot doesn't exist")

        now = datetime.now()
        date_time = now.strftime("%d/%m/%Y %H:%M:%S")

        await bots_collections.update_one({"_id": bots_record["_id"]}, {"$set": {"is_active": 0}})
        await bots_collections.update_one({"_id": bots_record["_id"]}, {"$set": {"modified_date": date_time}})
        await bots_collections.update_one({"_id": bots_record["_id"]}, {"$set": {"modified_by": user}})

        return JSONResponse(content={"detail": f"Bot has been disabled."}, status_code = 200)

    except HTTPException as e:
            raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")
    
@bots_router.post('/enable')
@x_super_team
@x_app_key
@jwt_token
async def enable(request: Request):
    try:
        data = await request.form()

        required_fields = ['bot_id']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")

        bot_id = data.get('bot_id')

        company_id = request.headers.get('x-super-team')
        user = request.state.current_user

        db = await connect()
    
        bots_collections = db['bots']

        bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 0})
        if not bots_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: bot doesn't exist")

        now = datetime.now()
        date_time = now.strftime("%d/%m/%Y %H:%M:%S")

        await bots_collections.update_one({"_id": bots_record["_id"]}, {"$set": {"is_active": 1}})
        await bots_collections.update_one({"_id": bots_record["_id"]}, {"$set": {"modified_date": date_time}})
        await bots_collections.update_one({"_id": bots_record["_id"]}, {"$set": {"modified_by": user}})

        return JSONResponse(content={"detail": f"Bot has been disabled."}, status_code = 200)

    except HTTPException as e:
            raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")
    
@bots_router.post('/update')
@x_super_team
@x_app_key
@jwt_token
async def enable(request: Request):
    try:
        data = await request.form()

        required_fields = ['bot_id', 'timeout']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")

        bot_id, timeout = data.get('bot_id'), data.get('timeout')

        company_id = request.headers.get('x-super-team')
        user = request.state.current_user

        db = await connect()
    
        bots_collections = db['bots']

        bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id})
        if not bots_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: bot doesn't exist")

        now = datetime.now()
        date_time = now.strftime("%d/%m/%Y %H:%M:%S")

        await bots_collections.update_one({"_id": bots_record["_id"]}, {"$set": {"timeout": timeout}})
        await bots_collections.update_one({"_id": bots_record["_id"]}, {"$set": {"modified_date": date_time}})
        await bots_collections.update_one({"_id": bots_record["_id"]}, {"$set": {"modified_by": user}})

        return JSONResponse(content={"detail": f"Bot has been updated."}, status_code = 200)

    except HTTPException as e:
            raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")