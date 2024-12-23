from functools import wraps

from decouple import config
from fastapi import HTTPException, Request, WebSocket

key = config("X_APP_KEY")

def x_app_key(fn):
    @wraps(fn)
    async def apidecorator(request: Request):
        x_app_key = request.headers.get('x-app-key')

        if not x_app_key:
            raise HTTPException(status_code = 401, detail = "An error occurred: missing 'x-app-key' header")
        if x_app_key != key:
            raise HTTPException(status_code = 401, detail = "An error occurred: invalid 'x-app-key' header")
        
        return await fn(request)
    return apidecorator

async def x_app_key_wb(websocket: WebSocket):
    x_app_key = websocket.headers.get('x-app-key')

    if not x_app_key:
        await websocket.send_json({"error": "An error occurred: missing 'x-app-key' header"})
        await websocket.close() 
    if x_app_key != key:
        await websocket.send_json({"error": "An error occurred: invalid 'x-app-key' header"})
        await websocket.close() 
    return x_app_key