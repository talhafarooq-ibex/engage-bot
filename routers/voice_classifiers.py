from datetime import datetime

from decorators.jwt import jwt_token
from decorators.key import x_app_key
from decorators.teams import x_super_team
from decouple import config
from fastapi import APIRouter, Form, HTTPException, Request
from utilities.database import connect
from utilities.time import current_time
from utilities.validation import check_required_fields

salt = config("TOKEN_SALT")
slug_db = config("SLUG_DATABASE")

classifier_router = APIRouter()

def convert_objectid(document):
    """
    Convert ObjectId to string in MongoDB documents.
    """
    document["_id"] = str(document["_id"])
    return document

@classifier_router.post("/create")
@jwt_token
@x_app_key
@x_super_team
async def create(
    request: Request,
):
    try:
        data = await request.form()

        required_fields = ['model_type', 'model_name', 'token']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = "An error occurred: missing parameter(s)")

        model_type, model_name, model_api_url,  = data.get('model_type'), data.get('model_name'), data.get('model_api_url')
        token, user, password = data.get('token'), data.get('user'), data.get('password')

        db = await connect()
        tokens_collection = db['tokens']
        classifiers_collection = db['classifiers']

        # Validate token
        token_record = await tokens_collection.find_one({"token": token, "is_active": 1})
        if not token_record:
            raise HTTPException(status_code=401, detail="Invalid token")

        bot_id = token_record['bot_id']
        workspace_id = token_record['workspace_id']
        company_id = request.headers.get('x-super-team')

        user_id = request.state.current_user

        # Freeze the currently active model of the same model_type if it exists
        existing_model = await classifiers_collection.find_one({"model_type": model_type, "is_active": "1"})
        if existing_model:
            await classifiers_collection.update_one(
                {"_id": existing_model["_id"]},
                {"$set": {"is_active": "0", "modified_by": user_id, "modified_date": current_time()}}
            )

        date_time = current_time()

        # Insert the new model with is_active=1
        document = {
            "company_id": company_id,
            "bot_id": bot_id,
            "workspace_id": workspace_id,
            "model_type": model_type,
            "model_name": model_name,
            "model_api_url": model_api_url,
            "user": user,
            "password": password,
            "is_active": "1",  # Set the new model to active
            "is_block": "0",
            "created_date": date_time,
            "created_by": user_id,
            "modified_date": date_time,
            "modified_by": user_id
        }

        # Insert the new document into the collection
        await classifiers_collection.insert_one(document)

        return {"message": "New model created and active"}
    
    except Exception as e:
        raise HTTPException(status_code = 500, detail=f"An error occurred: {str(e)}") from e        

# Read documents
@classifier_router.get("/get")
@jwt_token
@x_app_key
@x_super_team
async def get(request: Request):
    try:
        data = request.query_params

        required_fields = ['token']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = "An error occurred: missing parameter(s)")

        token = data.get('token')
        
        db = await connect()
        tokens_collection = db['tokens']
        classifiers_collection = db['classifiers']

        # Validate token
        token_record = await tokens_collection.find_one({"token": token, "is_active": 1})
        if not token_record:
            raise HTTPException(status_code=401, detail="Invalid token")
        
        bot_id = token_record['bot_id']
        workspace_id = token_record['workspace_id']
        company_id = request.headers.get('x-super-team')        

        query = {"bot_id": bot_id, "workspace_id": workspace_id, "company_id": company_id}

        documents = await classifiers_collection.find(query).to_list(None)
        documents = [convert_objectid(doc) for doc in documents]
            
        return documents
        
    except Exception as e:
        raise HTTPException(status_code = 500, detail=f"An error occurred: {str(e)}") from e   

# Update a document
@jwt_token
@x_app_key
@x_super_team
@classifier_router.post("/update")
async def update_document(filter_query: dict, update_data: dict):
    try:
        db = await connect()
        classifiers_collection = db['classifiers']
        update_query = {"$set": update_data}
        result = await classifiers_collection.update_one(filter_query, update_query)
        if result.modified_count > 0:
            return {"message": "Document updated successfully"}
        else:
            raise HTTPException(status_code=404, detail="No document found with the given filter")
    except Exception as e:
        raise HTTPException(status_code = 500, detail=f"An error occurred: {str(e)}") from e   

# Delete a document
@jwt_token
@x_app_key
@x_super_team
@classifier_router.post("/delete")
async def delete_document(company_id: str = Form(...)):
    try:
        db = await connect()
        classifiers_collection = db['classifiers']
        
        filter_query = {"company_id": company_id}
        
        # Update the document by setting is_active = 0 and is_block = 1
        update_data = {
            "$set": {
                "is_active": "0", 
                "is_block": "1", 
                "modified_date": current_time()  # Optional: update modified_date
            }
        }
        
        # Perform the update operation
        result = await classifiers_collection.update_one(filter_query, update_data)
        
        if result.matched_count > 0:
            return {"message": "Document updated successfully"}
        else:
            raise HTTPException(status_code=404, detail="No document found with the given company id")
    except Exception as e:
        raise HTTPException(status_code = 500, detail=f"An error occurred: {str(e)}") from e   

@jwt_token
@x_app_key
@x_super_team
@classifier_router.post("/disable")
async def disable(company_id: str = Form(...)):
    try:
        db = await connect()
        classifiers_collection = db['classifiers']
        
        filter_query = {"company_id": company_id}
        
        # Update the document by setting is_active = 0 and is_block = 1
        update_data = {
            "$set": {
                "is_active": "0", 
                "is_block": "0", 
                "modified_date": current_time()  # Optional: update modified_date
            }
        }
        
        # Perform the update operation
        result = await classifiers_collection.update_one(filter_query, update_data)
        
        if result.matched_count > 0:
            return {"message": "Document updated successfully"}
        else:
            raise HTTPException(status_code=404, detail="No document found with the given company id")
    except Exception as e:
        raise HTTPException(status_code = 500, detail=f"An error occurred: {str(e)}") from e   


