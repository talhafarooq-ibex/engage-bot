import os
import shutil
from datetime import datetime

import torch
from decorators.jwt import jwt_token
from decorators.key import x_app_key
from decorators.teams import x_super_team
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from lancedb.rerankers import LinearCombinationReranker
from langchain.docstore.document import Document
from langchain_chroma import Chroma
from langchain_community.document_loaders import (JSONLoader, PyMuPDFLoader,
                                                  UnstructuredExcelLoader,
                                                  UnstructuredHTMLLoader,
                                                  UnstructuredMarkdownLoader)
from langchain_community.document_loaders.csv_loader import CSVLoader
from langchain_community.embeddings import OllamaEmbeddings
from langchain_community.vectorstores import FAISS, LanceDB
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_openai import OpenAIEmbeddings
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_unstructured import UnstructuredLoader
from utilities.database import connect, format_docs
from utilities.time import current_time
from utilities.validation import check_required_fields, validate_inputs

device = "cuda" if torch.cuda.is_available() else "cpu"

embeddings_router = APIRouter()

@embeddings_router.get('/get')
@x_super_team
@x_app_key
@jwt_token
async def get(request: Request):
    try:
        data = request.query_params

        required_fields = ['bot_id', 'workspace_id']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = "An error occurred: missing parameter(s)")

        bot_id, workspace_id = data.get('bot_id'), data.get('workspace_id')

        company_id = request.headers.get('x-super-team')

        result = await validate_inputs(company_id, bot_id, workspace_id)
        if not result:
            raise HTTPException(status_code = 400, detail = "An error occurred: invalid parameter(s)")
        else:
            _, _, _ = result

        db = await connect()
        library_collections = db['library']
        embeddings_collections = db['embeddings']

        library_records = await library_collections.find({
            "company_id": company_id, "bot_id": bot_id, "workspace_id": workspace_id
        }).to_list(length=None)

        if not library_records:
            raise HTTPException(status_code = 404, detail = "An error occurred: no documents available for the bot")
        
        embeddings_record = await embeddings_collections.find_one({
            "company_id": company_id, "bot_id": bot_id, "workspace_id": workspace_id, "is_active": 1
        })

        if not embeddings_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: no embeddings available for the bot")
            
        embeddings_record.pop('_id')
        embeddings_record.pop('created_date')
        embeddings_record.pop('modified_date')
        embeddings_record.pop('created_by')
        embeddings_record.pop('modified_by')

        return JSONResponse(embeddings_record, status_code = 200)   
        
    except Exception as e:
        raise HTTPException(status_code = 500, detail=f"An error occurred: {str(e)}") from e
    
@embeddings_router.post('/create')
@x_super_team
@x_app_key
@jwt_token
async def create(request: Request):
    try:
        data = await request.form()

        required_fields = ['bot_id', 'workspace_id']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = "An error occurred: missing parameter(s)")
        
        bot_id, workspace_id = data.get('bot_id'), data.get('workspace_id')
        
        company_id = request.headers.get('x-super-team')
        user = request.state.current_user

        result = await validate_inputs(company_id, bot_id, workspace_id)
        if not result:
            raise HTTPException(status_code = 400, detail = "An error occurred: invalid parameter(s)")
        else:
            _, workspace_record, _ = result

        db = await connect()
        library_collections = db['library']
        embeddings_collections = db['embeddings']
        bots_collections = db['bots']
        
        library_records = await library_collections.find({
            "company_id": company_id, "bot_id": bot_id, "workspace_id": workspace_id, "is_active": 1
        }).to_list(length=None)

        if not library_records:
            raise HTTPException(status_code = 404, detail = "An error occurred: no documents available for the bot")
        
        embeddings_record = await embeddings_collections.find_one({
            "company_id": company_id, "bot_id": bot_id, "workspace_id": workspace_id, "is_active": 1
        })

        path = f"library/{company_id}/{bot_id}/{workspace_id}/embeddings"
        if embeddings_record:
            for item_name in os.listdir(path):
                item_path = os.path.join(path, item_name)

                if os.path.isfile(item_path):
                    os.remove(item_path)
                elif os.path.isdir(item_path):
                    shutil.rmtree(item_path)
        
        data = []
        for record in library_records:
            try:
                if record['url']:
                    loader = UnstructuredLoader(web_url = record['url'])

                    data_temp = loader.load()

                    temp = []
                    for i in data_temp:
                        temp.append(i.page_content)

                    content = '\n'.join(temp)
                    doc =  Document(page_content=content, metadata={"source": record['url']})

                    data.extend([doc])

                else:
                    file_path = f"library/{company_id}/{bot_id}/{workspace_id}/documents/{record['file_name']}"
                    if record['file_name'].endswith('.pdf'):
                        loader = PyMuPDFLoader(file_path, extract_images = 'enable')
                    elif record['file_name'].endswith('.html'):
                        loader = UnstructuredHTMLLoader(file_path)
                    elif record['file_name'].endswith('.json'):
                        loader = JSONLoader(file_path = file_path, jq_schema='.', text_content=False)
                    elif record['file_name'].endswith('.md'):
                        loader = UnstructuredMarkdownLoader(file_path)
                    elif record['file_name'].endswith('.csv'):
                        loader = CSVLoader(file_path = file_path)
                    elif record['file_name'].endswith('.xlsx') or record['file_name'].endswith('.xls'):
                        loader = UnstructuredExcelLoader(file_path)     
                    
                    data.extend(loader.load())
            
            except Exception:
                if record['url']:
                    raise HTTPException(status_code = 404, detail = f"An error occurred: existing connection was forcibly closed by the remote host for {record['url']}.")
                
                file_path = f"library/{company_id}/{bot_id}/{workspace_id}/documents/{record['file_name']}"
                if record['file_name'].endswith('.pdf'):
                    loader = PyMuPDFLoader(file_path, extract_images = 'enable')
                elif record['file_name'].endswith('.html'):
                    loader = UnstructuredHTMLLoader(file_path)
                elif record['file_name'].endswith('.json'):
                    loader = JSONLoader(file_path = file_path, jq_schema='.', text_content=False)
                elif record['file_name'].endswith('.md'):
                    loader = UnstructuredMarkdownLoader(file_path)
                elif record['file_name'].endswith('.csv'):
                    loader = CSVLoader(file_path = file_path)
                elif record['file_name'].endswith('.xlsx') or record['file_name'].endswith('.xls'):
                    loader = UnstructuredExcelLoader(file_path)
                
                data.extend(loader.load())

        text_splitter = RecursiveCharacterTextSplitter(
        separators=["\n\n", "\n", " ", ".", ",", ""],
            chunk_size = 1000,
            chunk_overlap = 50,
            length_function = len,
            is_separator_regex = False,
        )

        chunks = text_splitter.split_documents(data)

        if workspace_record['embeddings'] == 'openai':
            embeddings = OpenAIEmbeddings(model = workspace_record['embeddings_model'], openai_api_key = workspace_record['embeddings_api_key'])
        elif workspace_record['embeddings'] == 'huggingface':
            embeddings = HuggingFaceEmbeddings(
                model_name = workspace_record['embeddings_model'], model_kwargs = {'device': device}, encode_kwargs = {'normalize_embeddings': False}
            )
        elif workspace_record['embeddings'] == 'ollama':
            if workspace_record['embeddings_url']:
                embeddings = OllamaEmbeddings(model = workspace_record['embeddings_model'], base_url = workspace_record['embeddings_url'])
            else: 
                embeddings = OllamaEmbeddings(model = workspace_record['embeddings_model'])

        path = f"library/{company_id}/{bot_id}/{workspace_id}/embeddings"
        if workspace_record['vectordb'] == 'faiss':
            vector_store = FAISS.from_documents(chunks, embeddings)
            vector_store.save_local(path)
        elif workspace_record['vectordb'] == 'chroma':
            vector_store = Chroma.from_documents(chunks, embeddings, persist_directory = path)
        elif workspace_record['vectordb'] == 'lancedb':
            reranker = LinearCombinationReranker(weight = 0.3)
            vector_store = LanceDB.from_documents(chunks, embeddings, reranker = reranker, uri = path)

        embeddings_record = await embeddings_collections.find_one({
            "company_id": company_id, "bot_id": bot_id, "workspace_id": workspace_id, "is_active": 1
        })

        date_time = current_time()

        if embeddings_record:
            await embeddings_collections.update_one({"_id": embeddings_record["_id"]}, {"$set": {"is_active": 0}})
            await embeddings_collections.update_one({"_id": embeddings_record["_id"]}, {"$set": {"modified_date": date_time}})
            await embeddings_collections.update_one({"_id": embeddings_record["_id"]}, {"$set": {"modified_by": user}})

        document = {
            'company_id': company_id, 'bot_id': bot_id, 'workspace_id': workspace_id, 'is_active': 1, 'created_date': date_time, 
            'modified_date': date_time, 'created_by': user, 'modified_by': user
        }

        await embeddings_collections.insert_one(document)   

        bots_record = await bots_collections.find_one({"company_id": company_id, "bot_id": bot_id, "is_active": 1})

        await bots_collections.update_one({"_id": bots_record["_id"]}, {"$set": {"modified_date": date_time}})
        await bots_collections.update_one({"_id": bots_record["_id"]}, {"$set": {"modified_by": user}})
                        
        return JSONResponse(content={"detail": "Embeddings have been created."}, status_code = 200)

    except Exception as e:
        raise HTTPException(status_code = 500, detail=f"An error occurred: {str(e)}") from e
    
@embeddings_router.post('/response')
@x_super_team
@x_app_key
@jwt_token
async def response(request: Request):
    try:
        data = await request.form()

        required_fields = ['bot_id', 'workspace_id', 'text', 'k']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = "An error occurred: missing parameter(s)")

        bot_id, workspace_id, text, k = data.get('bot_id'), data.get('workspace_id'), data.get('text'), int(data.get('k'))

        company_id = request.headers.get('x-super-team')

        result = await validate_inputs(company_id, bot_id, workspace_id)
        if not result:
            raise HTTPException(status_code = 400, detail = "An error occurred: invalid parameter(s)")
        else:
            _, workspace_record, _ = result

        db = await connect()
        embeddings_collections = db['embeddings']
        
        embeddings_record = await embeddings_collections.find_one({
            "company_id": company_id, "bot_id": bot_id, "workspace_id": workspace_id, "is_active": 1
        })

        if not embeddings_record:
            raise HTTPException(status_code = 404, detail = "An error occurred: no embeddings available for the bot")

        if workspace_record['embeddings'] == 'openai':
            embeddings = OpenAIEmbeddings(model = workspace_record['embeddings_model'], openai_api_key = workspace_record['embeddings_api_key'])
        elif workspace_record['embeddings'] == 'huggingface':
            embeddings = HuggingFaceEmbeddings(
                model_name = workspace_record['embeddings_model'], model_kwargs = {'device': device}, encode_kwargs = {'normalize_embeddings': False}
            )
        elif workspace_record['embeddings'] == 'ollama':
            if workspace_record['embeddings_url']:
                embeddings = OllamaEmbeddings(model = workspace_record['embeddings_model'], base_url = workspace_record['embeddings_url'])
            else: 
                embeddings = OllamaEmbeddings(model = workspace_record['embeddings_model'])

        path = f"library/{company_id}/{bot_id}/{workspace_id}/embeddings"
        if workspace_record['vectordb'] == 'faiss':
            vectorstore = FAISS.load_local(path, embeddings, allow_dangerous_deserialization = True)
        elif workspace_record['vectordb'] == 'chroma':
            vectorstore = Chroma(persist_directory = path, embedding_function = embeddings)
        elif workspace_record['vectordb'] == 'lancedb':
            reranker = LinearCombinationReranker(weight=0.3)
            vectorstore = LanceDB(embedding = embeddings, uri = path, reranker = reranker)

        retriever = vectorstore.as_retriever(search_kwargs={"k": k})

        documents = retriever.invoke(text)
        documents = format_docs(documents)

        return JSONResponse(documents, status_code = 200)                                               

    except Exception as e:
        raise HTTPException(status_code = 500, detail=f"An error occurred: {str(e)}") from e