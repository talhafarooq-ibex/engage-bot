import jwt
from fastapi import HTTPException, Request, WebSocket
from functools import wraps
from decouple import config

salt = config("TOKEN_SALT")

def jwt_token(fn):
    @wraps(fn)
    async def decorated(request: Request):

        headers = request.headers

        try:
            if "Authorization" in headers:
                token_from_request = headers.get('Authorization').split(" ")[1]
            else:
                raise HTTPException(status_code = 401, detail = "An error occurred: missing authorization token")
        except:
            raise HTTPException(status_code = 401, detail = "An error occurred: missing authorization token")
      
        try:
            options = {
            'verify_exp': False, 
            'verify_aud': False 
            }

            payload = jwt.decode(token_from_request, salt, 'HS256', options)

            request.state.current_user = payload['UserId']
            request.state.current_name = payload['http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name']
            request.state.current_email = payload['http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress']

        except:
            raise HTTPException(status_code = 401, detail = "An error occurred: invalid authorization token")

        return await fn(request)

    return decorated

async def jwt_token_wb(websocket: WebSocket):
    authorization = websocket.headers.get('Authorization')

    if not authorization:
        await websocket.send_json({"error": "An error occurred: missing 'Authorization' header"})
        await websocket.close() 

    try:
        options = {
        'verify_exp': False, 
        'verify_aud': False 
        }

        payload = jwt.decode(authorization.split(' ')[1], salt, 'HS256', options)

    except:
        await websocket.send_json({"error": "An error occurred: invalid 'x-app-key' header"})
        await websocket.close() 

    return payload['UserId'], payload['http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name'], payload['http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress']