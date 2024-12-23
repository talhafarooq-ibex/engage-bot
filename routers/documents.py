import pymongo, time, random, string
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse
from datetime import datetime

from decorators.jwt import jwt_token
from decorators.key import x_app_key
from decorators.teams import x_super_team
from utilities.database import connect
from utilities.validation import check_required_fields, check_link_validity

documents_router = APIRouter()

ALLOWED_EXTENSIONS = ['.pdf', '.json', '.html', '.md', '.csv', '.xlsx', '.xls']

@documents_router.get('/get/all')
@x_super_team
@x_app_key
@jwt_token
async def get_all(request: Request):
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
        library_collections = db['library']

        bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 1})

        if not bots_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: bot doesn\'t exist")

        workspace_record = await workspace_collections.find_one({
            "company_id": company_id, "bot_id": bot_id, "workspace_id": workspace_id, "is_active": 1
        })
            
        if not workspace_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: no workspace found for this bot or key doesn't exist")

        library_records = await library_collections.find({
            "company_id": company_id, "bot_id": bot_id, "workspace_id": workspace_id
        }).to_list(length=None)

        if not library_records:
            raise HTTPException(status_code = 404, detail = "An error occurred: no documents available for the bot")
            
        results = []

        for record in library_records:
            try:
                temp = {
                    'bot_id': record['bot_id'], 'workspace_id': record['workspace_id'], 'document_id': record['document_id'], 
                    'file_name': record['file_name'], 'url': record['url'], 'is_active': record['is_active']
                }
            except:
                temp = {
                    'bot_id': record['bot_id'], 'workspace_id': record['workspace_id'], 'document_id': record['document_id'], 
                    'file_name': record['file_name'], 'url': None, 'is_active': record['is_active']
                }

            results.append(temp)
                    
        return JSONResponse(results, status_code = 200)   
        
    except HTTPException as e:
            raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")

@documents_router.get('/get')
@x_super_team
@x_app_key
@jwt_token
async def get(request: Request):
    try:
        data = request.query_params

        required_fields = ['bot_id', 'workspace_id', 'document_id']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")

        bot_id, workspace_id, document_id = data.get('bot_id'), data.get('workspace_id'), data.get('document_id')

        company_id = request.headers.get('x-super-team')

        db = await connect()

        bots_collections = db['bots']
        workspace_collections = db['workspace']
        library_collections = db['library']

        bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 1})

        if not bots_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: bot doesn\'t exist")
        
        workspace_record = await workspace_collections.find_one({
            "company_id": company_id, "bot_id": bot_id, "workspace_id": workspace_id, "is_active": 1
        })
            
        if not workspace_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: no workspace found for this bot or key doesn't exist")
        
        library_record = await library_collections.find_one({
            "company_id": company_id, "bot_id": bot_id, "workspace_id": workspace_id, "document_id": document_id
        })

        if not library_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: no documents available for the bot")
        
        try:
            temp = {
                'bot_id': library_record['bot_id'], 'workspace_id': library_record['workspace_id'], 'document_id': library_record['document_id'], 
                'file_name': library_record['file_name'], 'url': library_record['url'], 'is_active': library_record['is_active']
            }
        except:
            temp = {
                'bot_id': library_record['bot_id'], 'workspace_id': library_record['workspace_id'], 'document_id': library_record['document_id'], 
                'file_name': library_record['file_name'], 'url': None, 'is_active': library_record['is_active']
            }            

        return JSONResponse(temp, status_code = 200)   
        
    except HTTPException as e:
            raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")
    
@documents_router.post('/upload')
@x_super_team
@x_app_key
@jwt_token
async def upload(request: Request):
    try:
        data = await request.form()

        required_fields = ['bot_id', 'workspace_id', 'documents']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")

        bot_id, files, workspace_id = data.get('bot_id'), data.getlist('documents'), data.get('workspace_id')
        
        for file in files:
            if not any(file.filename.endswith(ext) for ext in ALLOWED_EXTENSIONS):
                raise HTTPException(status_code = 400, detail=f"An error occurred: invalid document format for file '{file.filename}'") 
            
        company_id = request.headers.get('x-super-team')
        user = request.state.current_user

        db = await connect()

        bots_collections = db['bots']
        library_collections = db['library']
        workspace_collections = db['workspace']

        bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 1})

        if not bots_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: bot doesn\'t exist")
        
        for file in files:                 
            library_record = await library_collections.find_one({
                "company_id": company_id, "bot_id": bot_id, "workspace_id": workspace_id, "is_active": 1, "file_name": file.filename
            })

            workspace_record = await workspace_collections.find_one({
                "company_id": company_id, "bot_id": bot_id, "workspace_id": workspace_id, "is_active": 1
            })
        
            if not workspace_record:
                raise HTTPException(status_code = 404, detail = "An error occurred: no workspace found for this bot or key doesn't exist")

            if library_record and library_record['file_name'] == file.filename:
                raise HTTPException(status_code = 400, detail = "An error occurred: document already exists")
            
            latest_document = await library_collections.find_one({
                "company_id": company_id, "bot_id": bot_id, "workspace_id": workspace_id}, sort=[("created_date", pymongo.DESCENDING)])
            
            if latest_document:
                document_id = f"{int(latest_document['document_id']) + 1}"
            else: 
                document_id = '1'

            now = datetime.now()
            date_time = now.strftime("%d/%m/%Y %H:%M:%S")

            document = {
                'company_id': company_id, 'bot_id': bot_id, "workspace_id": workspace_id, 'document_id': document_id, 'file_name': file.filename, 'url': None,
                'is_active': 1, 'created_date': date_time, 'modified_date': date_time, 'created_by': user, 'modified_by': user
            }

            await library_collections.insert_one(document)

            await bots_collections.update_one({"_id": bots_record["_id"]}, {"$set": {"modified_date": date_time}})
            await bots_collections.update_one({"_id": bots_record["_id"]}, {"$set": {"modified_by": user}})

            file_path = f"library/{company_id}/{bot_id}/{workspace_id}/documents/{file.filename}"
            with open(file_path, "wb") as buffer:
                buffer.write(await file.read())  

            time.sleep(1)

        return JSONResponse(content={"detail": f"Documents have been uploaded."}, status_code = 200)

    except HTTPException as e:
            raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")
    
@documents_router.post('/create')
@x_super_team
@x_app_key
@jwt_token
async def create(request: Request):
    try:
        data = await request.form()

        required_fields = ['bot_id', 'workspace_id', 'text']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")

        bot_id, text, workspace_id = data.get('bot_id'), data.get('text'), data.get('workspace_id')

        company_id = request.headers.get('x-super-team')
        user = request.state.current_user

        db = await connect()

        bots_collections = db['bots']
        library_collections = db['library']
        workspace_collections = db['workspace']

        bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 1})

        if not bots_record:   
            raise HTTPException(status_code = 404, detail = "An error occurred: bot doesn\'t exist")
        
        workspace_record = await workspace_collections.find_one({
            "company_id": company_id, "bot_id": bot_id, "workspace_id": workspace_id, "is_active": 1
        })
        
        if not workspace_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: no workspace found for this bot or key doesn't exist")

        latest_document = await library_collections.find_one({
            "company_id": company_id, "bot_id": bot_id, "workspace_id": workspace_id}, sort=[("created_date", pymongo.DESCENDING)]
        )
        
        if latest_document:
            document_id = f"{int(latest_document['document_id']) + 1}"
        else: 
            document_id = '1'
            
        letters = string.ascii_letters + string.digits
        filename = ''.join(random.choice(letters) for i in range(12))

        file_path = f"library/{company_id}/{bot_id}/{workspace_id}/documents/{filename}.pdf"

        doc = SimpleDocTemplate(file_path, pagesize=letter)

        styles = getSampleStyleSheet()
        style = styles['Normal']

        elements = []

        for paragraph in text.split('\n'):
            elements.append(Paragraph(paragraph, style))
            elements.append(Spacer(1, 12))  

        doc.build(elements)

        now = datetime.now()
        date_time = now.strftime("%d/%m/%Y %H:%M:%S")

        document = {
            'company_id': company_id, 'bot_id': bot_id, 'document_id': document_id, "workspace_id": workspace_id, 'file_name': f'{filename}.pdf', 'url': None,  
            'is_active': 1, 'created_date': date_time, 'modified_date': date_time, 'created_by': user, 'modified_by': user
        }
        
        await library_collections.insert_one(document)

        await bots_collections.update_one({"_id": bots_record["_id"]}, {"$set": {"modified_date": date_time}})
        await bots_collections.update_one({"_id": bots_record["_id"]}, {"$set": {"modified_by": user}})

        return JSONResponse(content={"detail": f"Documents have been created."}, status_code = 200)

    except HTTPException as e:
            raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")

@documents_router.post('/url')
@x_super_team
@x_app_key
@jwt_token
async def url(request: Request):
    try:
        data = await request.form()

        required_fields = ['bot_id', 'workspace_id', 'urls']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")

        bot_id, urls, workspace_id = data.get('bot_id'), data.getlist('urls'), data.get('workspace_id')

        company_id = request.headers.get('x-super-team')
        user = request.state.current_user

        db = await connect()

        bots_collections = db['bots']
        library_collections = db['library']
        workspace_collections = db['workspace']

        bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 1})

        if not bots_record:   
            raise HTTPException(status_code = 404, detail = "An error occurred: bot doesn\'t exist")
        
        workspace_record = await workspace_collections.find_one({
            "company_id": company_id, "bot_id": bot_id, "workspace_id": workspace_id, "is_active": 1
        })
        
        if not workspace_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: no workspace found for this bot or key doesn't exist")

        for url in urls:
            if not check_link_validity(url):
                raise HTTPException(status_code = 400, detail = f"An error occurred: invalid url: {url}")

        for url in urls:
            now = datetime.now()
            date_time = now.strftime("%d/%m/%Y %H:%M:%S")

            latest_document = await library_collections.find_one({
                "company_id": company_id, "bot_id": bot_id, "workspace_id": workspace_id}, sort=[("created_date", pymongo.DESCENDING)]
            )
            
            if latest_document:
                document_id = f"{int(latest_document['document_id']) + 1}"
            else: 
                document_id = '1'

            document = {
                'company_id': company_id, 'bot_id': bot_id, 'document_id': document_id, "workspace_id": workspace_id, 'file_name': None, 'url': url,
                'is_active': 1, 'created_date': date_time, 'modified_date': date_time, 'created_by': user, 'modified_by': user
            }
            
            await library_collections.insert_one(document)

        await bots_collections.update_one({"_id": bots_record["_id"]}, {"$set": {"modified_date": date_time}})
        await bots_collections.update_one({"_id": bots_record["_id"]}, {"$set": {"modified_by": user}})

        return JSONResponse(content={"detail": f"URLs have been created."}, status_code = 200)

    except HTTPException as e:
            raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")
    
@documents_router.post('/disable')
@x_super_team
@x_app_key
@jwt_token
async def disable(request: Request):
    try:
        data = await request.form()

        required_fields = ['bot_id', 'workspace_id', 'document_id']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")

        bot_id, document_id, workspace_id = data.get('bot_id'), data.get('document_id'), data.get('workspace_id')

        company_id = request.headers.get('x-super-team')
        user = request.state.current_user

        db = await connect()
    
        bots_collections = db['bots']
        library_collections = db['library']
        workspace_collections = db['workspace']

        bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 1})

        if not bots_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: bot doesn\'t exist")
    
        workspace_record = await workspace_collections.find_one({
            "company_id": company_id, "bot_id": bot_id, "workspace_id": workspace_id, "is_active": 1
        })
        
        if not workspace_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: no workspace found for this bot or key doesn't exist")

        library_record = await library_collections.find_one({
            "company_id": company_id, "bot_id": bot_id, "workspace_id": workspace_id, "document_id": document_id, "is_active": 1
        })
        
        if not library_record:
            raise HTTPException(status_code = 404, detail = f"An error occurred: document is disabled or doesn\'t exist")

        now = datetime.now()
        date_time = now.strftime("%d/%m/%Y %H:%M:%S")

        await library_collections.update_one({"_id": library_record["_id"]}, {"$set": {"is_active": 0}})
        await library_collections.update_one({"_id": library_record["_id"]}, {"$set": {"modified_date": date_time}})
        await library_collections.update_one({"_id": library_record["_id"]}, {"$set": {"modified_by": user}})

        await bots_collections.update_one({"_id": bots_record["_id"]}, {"$set": {"modified_date": date_time}})
        await bots_collections.update_one({"_id": bots_record["_id"]}, {"$set": {"modified_by": user}})

        return JSONResponse(content={"detail": f"Document has been disabled."}, status_code = 200)

    except HTTPException as e:
            raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")
    
@documents_router.post('/enable')
@x_super_team
@x_app_key
@jwt_token
async def enable(request: Request):
    try:
        data = await request.form()

        required_fields = ['bot_id', 'workspace_id', 'document_id']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = f"An error occurred: missing parameter(s)")

        bot_id, document_id, workspace_id = data.get('bot_id'), data.get('document_id'), data.get('workspace_id')

        company_id = request.headers.get('x-super-team')
        user = request.state.current_user

        db = await connect()
    
        bots_collections = db['bots']
        library_collections = db['library']
        workspace_collections = db['workspace']

        bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 1})

        if not bots_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: bot doesn\'t exist")
        
        workspace_record = await workspace_collections.find_one({
            "company_id": company_id, "bot_id": bot_id, "workspace_id": workspace_id, "is_active": 1
        })
        
        if not workspace_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: no workspace found for this bot or key doesn't exist")
        
        library_record = await library_collections.find_one({
            "company_id": company_id, "bot_id": bot_id, "workspace_id": workspace_id, "document_id": document_id, "is_active": 0
        })
        
        if not library_record:
            raise HTTPException(status_code = 404, detail = f"An error occurred: document is enabled or doesn\'t exist")

        now = datetime.now()
        date_time = now.strftime("%d/%m/%Y %H:%M:%S")

        await library_collections.update_one({"_id": library_record["_id"]}, {"$set": {"is_active": 1}})
        await library_collections.update_one({"_id": library_record["_id"]}, {"$set": {"modified_date": date_time}})
        await library_collections.update_one({"_id": library_record["_id"]}, {"$set": {"modified_by": user}})

        await bots_collections.update_one({"_id": bots_record["_id"]}, {"$set": {"modified_date": date_time}})
        await bots_collections.update_one({"_id": bots_record["_id"]}, {"$set": {"modified_by": user}})

        return JSONResponse(content={"detail": f"Document has been enabled."}, status_code = 200)
        
    except HTTPException as e:
            raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")