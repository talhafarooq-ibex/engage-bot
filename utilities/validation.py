import string
from datetime import datetime
from decouple import config
from validators import url as is_valid_url
from utilities.database import connect

punc = string.punctuation
punc = punc.replace('_', '').replace('[', '').replace(']', '')

host = config("DATABASE_HOST")
username = config("DATABASE_USERNAME")
password = config("DATABASE_PASSWORD")
database = config("DATABASE_NAME")
slug_db = config("SLUG_DATABASE")

async def validate_inputs(company_id, bot_id, workspace_id):
    db = await connect()

    bots_collections = db['bots']
    workspace_collections = db['workspace']
    configurations_collections = db['configuration']

    bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 1})
    if not bots_record:
        return False
    
    workspace_record = await workspace_collections.find_one({
        "company_id": company_id, "bot_id": bot_id, "workspace_id": workspace_id, "is_active": 1
    })
    if not workspace_record:
        return False
    
    configuration_record = await configurations_collections.find_one({'bot_id': bot_id, 'workspace_id': workspace_id})
    if not configuration_record:
        return False

    return bots_record, workspace_record, configuration_record

async def validate_token(token):
    db = await connect()

    tokens_collections = db['tokens']

    tokens_record = await tokens_collections.find_one({"token": token, "is_active": 1})
    if not tokens_record:
        return False
    
    now = datetime.now()
    date_time = now.strftime("%d/%m/%Y %H:%M:%S")

    date_format = "%d/%m/%Y %H:%M:%S"
    created_date = datetime.strptime(date_time, date_format)
    expiry_date = datetime.strptime(tokens_record['expiry_date'], date_format)

    if expiry_date < created_date:
        return False
    
    bot_id = tokens_record['bot_id']
    workspace_id = tokens_record['workspace_id']
    company_id = tokens_record['company_id']

    return await validate_inputs(company_id, bot_id, workspace_id)

def process_name(name, underscore = 0):
    if underscore:
        if len(name.split(' ')) >= 2:
            name = name.replace(' ', '_')

    name = name.translate(str.maketrans('', '', punc)).strip()
    name = ' '.join(name.split())

    return name

def check_required_fields(data, required_fields):
    for field in required_fields:
        if not data.get(field):
            return False
    return True

def check_link_validity(link):
    if not is_valid_url(link):
        return False
    return True
