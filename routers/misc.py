from datetime import datetime

from decorators.jwt import jwt_token
from decorators.key import x_app_key
from decorators.teams import x_super_team
from decouple import config
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from langchain_ollama import ChatOllama
from utilities.validation import check_required_fields

prompt_summary_english = config("PROMPT_SUMMARY_ENGLISH")

misc_router = APIRouter()

@misc_router.post('/summary')
@x_super_team
@x_app_key
@jwt_token
async def create(request: Request):
    try:
        data = await request.form()

        required_fields = ['messages']
        if not check_required_fields(data, required_fields):
            raise HTTPException(status_code = 400, detail = "An error occurred: missing parameter(s)")
        
        messages = data.get('messages')  

        llm = ChatOllama(model = 'llama3.1')

        response = await llm.ainvoke(prompt_summary_english.format(messages=messages))

        return JSONResponse(content={"detail": response.content}, status_code = 200)  

    except Exception as e:
        raise HTTPException(status_code = 500, detail=f"An error occurred: {str(e)}") from e