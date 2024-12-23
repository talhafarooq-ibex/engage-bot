import string, secrets
from fastapi import HTTPException
from decouple import config
from datetime import datetime
from utilities.database import connect

slug_db = config("SLUG_DATABASE")

sentiment_url = config("SENTIMENT_URL")
x_app_key = config("X_APP_KEY")

sentiment_headers = {
    'x-app-key': x_app_key,
    'x-super-team': '100'
}

async def create(bots_record, workspace_record, phone, username, email, queue):
    try:
        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)

        profiles_collections = db['profiles'] 

        alphabet = string.ascii_letters + string.digits
        session_id = ''.join(secrets.choice(alphabet) for _ in range(16))

        now = datetime.now()
        date_time = now.strftime("%d/%m/%Y %H:%M:%S")

        document = {
            'company_id': workspace_record["company_id"], 'bot_id': workspace_record["bot_id"], "email": email, "phone": phone, 
            'username': username, 'queue': queue, 'preference': None, 'timeout': bots_record['timeout'], 
            'workspace_id': workspace_record['workspace_id'], 'session_id': session_id, 'created_date': date_time, 'latest_timestamp': None
        }
        
        await profiles_collections.insert_one(document)
        
        return session_id  

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")