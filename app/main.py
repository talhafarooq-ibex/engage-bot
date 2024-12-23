import nltk
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from routers.bots import bots_router
from routers.workspaces import workspaces_router
from routers.documents import documents_router
from routers.embeddings import embeddings_router
from routers.tokens import tokens_router
from routers.agents import agents_router
from routers.chats.apis import chats_router
from routers.analytics import analytics_router
from routers.dashboard import dashboard_router
from routers.csat import csat_router
from routers.configuration import configuration_router
from routers.voice import voice_router
from routers.voice_classifiers import classifier_router
from routers.misc import misc_router

nltk.download('punkt')

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials = True,
    allow_methods=["*"], 
    allow_headers=["*"],  
)

app.include_router(bots_router, prefix="/bots")
app.include_router(workspaces_router, prefix="/workspaces")
app.include_router(documents_router, prefix="/documents")
app.include_router(embeddings_router, prefix="/embeddings")
app.include_router(tokens_router, prefix="/tokens")
app.include_router(agents_router, prefix="/agents")
app.include_router(chats_router, prefix="/chats")
app.include_router(analytics_router, prefix="/analytics")
app.include_router(dashboard_router, prefix="/dashboard")
app.include_router(csat_router, prefix="/csat")
app.include_router(configuration_router, prefix="/configuration")
app.include_router(voice_router, prefix="/voice")
app.include_router(classifier_router, prefix="/classifier")
app.include_router(misc_router, prefix="/misc")

app.mount("/", StaticFiles(directory="static", html = True), name="static")

if __name__ == "__main__":
    app.run()