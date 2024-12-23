from fastapi import HTTPException, Request, WebSocket
from functools import wraps

def x_super_team(fn):
    @wraps(fn)
    async def teamdecorator(request: Request):
        x_super_team = request.headers.get('x-super-team')

        if x_super_team and x_super_team != 0:
            return await fn(request)
        else:
            return HTTPException(status_code = 401, detail = "An error occurred: missing 'x-super-team' header")
    return teamdecorator

async def x_super_team_wb(websocket: WebSocket):
    x_super_team = websocket.headers.get('x-super-team')

    if not x_super_team:
        await websocket.send_json({"error": "An error occurred: missing 'x-super-team' header"})
        await websocket.close() 
    elif x_super_team == 0:
        await websocket.send_json({"error": "An error occurred: invalid 'x-super-team' header"})
        await websocket.close() 
    return x_super_team