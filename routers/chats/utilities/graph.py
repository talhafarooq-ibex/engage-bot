import json
import sqlite3
import uuid
from datetime import datetime
from typing import Annotated, Optional

import requests
import torch
from decouple import config
from fastapi import HTTPException
from lancedb.rerankers import LinearCombinationReranker
from langchain_chroma import Chroma
from langchain_community.embeddings import OllamaEmbeddings
from langchain_community.vectorstores import FAISS, LanceDB
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import Runnable, RunnableConfig, RunnableLambda
from langchain_core.tools import tool
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_openai import OpenAIEmbeddings
from langgraph.graph import START, StateGraph
from langgraph.graph.message import AnyMessage, add_messages
from langgraph.prebuilt import ToolNode, tools_condition
from pydantic import BaseModel, Field
from typing_extensions import Annotated, TypedDict
from utilities.database import connect
from utilities.redis import enqueue
from utilities.time import current_time

from routers.chats.utilities.client import (agent_involved_chat, llm_selection,
                                            max_allowed_chats)
from routers.chats.utilities.suggestions import client_suggestions_otherllms
from routers.chats.utilities.summary import client_summary_otherllms

from .mongo import AsyncMongoDBSaver

host = config("DATABASE_HOST")
username = config("DATABASE_USERNAME")
password = config("DATABASE_PASSWORD")

slug_db = config("SLUG_DATABASE")

language_english = config("LANGUAGE_ENGLISH") 
display_language_english = config("DISPLAY_LANGUAGE_ENGLISH") 

language_arabic = config("LANGUAGE_ARABIC") 
display_language_arabic = config("DISPLAY_LANGUAGE_ARABIC") 

human_end_message = config("HUMAN_END_MESSAGE")
display_human_end_message_english = config("DISPLAY_HUMAN_END_MESSAGE_ENGLISH")
display_human_end_message_arabic = config("DISPLAY_HUMAN_END_MESSAGE_ARABIC")

transfer_message = config("TRANSFER_MESSAGE")
display_transfer_message_english = config("DISPLAY_TRANSFER_MESSAGE_ENGLISH")
display_transfer_message_arabic = config("DISPLAY_TRANSFER_MESSAGE_ARABIC")

human_takeover_message = config("HUMAN_TAKEOVER_MESSAGE")
display_human_takeover_message_english = config("DISPLAY_HUMAN_TAKEOVER_MESSAGE_ENGLISH")
display_human_takeover_message_arabic = config("DISPLAY_HUMAN_TAKEOVER_MESSAGE_ARABIC")

prompt_goodbye_english = config("PROMPT_GOODBYE_ENGLISH")
prompt_goodbye_arabic = config("PROMPT_GOODBYE_ARABIC")

prompt_transfer_english = config("PROMPT_TRANSFER_ENGLISH")
prompt_transfer_arabic = config("PROMPT_TRANSFER_ARABIC")

transfer_queue = config("TRANSFER_QUEUE")

sentiment_url = config("SENTIMENT_URL")
x_app_key_var = config("X_APP_KEY")

sentiment_headers = {
    'x-app-key': x_app_key_var,
    'x-super-team': '100'
}

class State(TypedDict):
    messages: Annotated[list[AnyMessage], add_messages]

db_booking = 'massage_booking.db'

ALLOWED_MASSAGE_TYPES = ['Foot', 'Swedish', 'Deep Tissue', 'Sports']

device = "cuda" if torch.cuda.is_available() else "cpu"

def handle_tool_error(state) -> dict:
    error = state.get("error")
    tool_calls = state["messages"][-1].tool_calls
    return {
        "messages": [
            ToolMessage(
                content=f"Error: {repr(error)}\n please fix your mistakes.",
                tool_call_id=tc["id"],
            )
            for tc in tool_calls
        ]
    }

def create_tool_node_with_fallback(tools: list) -> dict:
    return ToolNode(tools).with_fallbacks(
        [RunnableLambda(handle_tool_error)], exception_key="error"
    )

@tool
async def massage_information_retrieval(query: str, config: RunnableConfig):
    """
    Retrieve detailed information about massages, including their types, benefits, pricing, and other related details.

    This tool is designed to answer customer questions about massages by providing relevant and accurate information. 
    Examples of questions it can handle include:
    - What types of massages are available?
    - How much does a deep tissue massage cost?
    - What are the benefits of a Swedish massage?
    - Do you offer package deals or discounts on massages?

    Args:
        query (str): The customer's specific question or inquiry about massages.
        config (RunnableConfig): Configuration settings required for executing the tool's functionality, such as language preferences or query processing options.

    Returns:
        str: A response containing the requested information about massages, tailored to the provided query.
    """

    configuration = config.get("configurable", {})
    company_id = configuration.get("company_id", None)
    bot_id = configuration.get("bot_id", None)
    workspace_id = configuration.get("workspace_id", None)

    if not company_id or not bot_id or not workspace_id:
        return "Sorry, there was a problem with the configuration. Can you please try again"    

    path = f"library/{company_id}/{bot_id}/{workspace_id}/embeddings"

    db = await connect()
    workspace_collections = db['workspace']
    
    workspace_record = await workspace_collections.find_one({"bot_id": bot_id, "workspace_id": workspace_id, "is_active": 1})
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

    if workspace_record['vectordb'] == 'faiss':
        vectorstore = FAISS.load_local(path, embeddings, allow_dangerous_deserialization = True)
    elif workspace_record['vectordb'] == 'chroma':
        vectorstore = Chroma(persist_directory = path, embedding_function = embeddings)
    elif workspace_record['vectordb'] == 'lancedb':
        reranker = LinearCombinationReranker(weight = 0.3)
        vectorstore = LanceDB(embedding = embeddings, uri = path, reranker = reranker)

    retrieved_docs = vectorstore.similarity_search(query, k = int(workspace_record['k_retreive']))

    sources = [
        doc.metadata['source']
        for doc in retrieved_docs]

    sources = list(set(sources))
    return retrieved_docs, sources

@tool
async def search_bookings(
    massage_type: Optional[str] = None,
    booking_datetime: Optional[str] = None
):
    """Search for bookings based on massage type and booking datetime.

      Args:
        massage_type: Customer's preferred massage type from only this list ['Foot', 'Swedish', 'Deep Tissue', 'Sports']
        booking_datetime: Customer's preferred booking time (Format: YYYY-MM-DD HH:MM:SS)
    """
    conn = sqlite3.connect(db_booking)
    cursor = conn.cursor()

    query = "SELECT * FROM bookings WHERE 1 = 1"
    params = []

    if massage_type:
        if massage_type not in ALLOWED_MASSAGE_TYPES:
            return f"Invalid massage type. Please choose from the following options: {', '.join(ALLOWED_MASSAGE_TYPES)}."
    
        query += " AND massage_type = ?"
        params.append(massage_type)

    if booking_datetime:
        try:
            booking_dt = datetime.strptime(booking_datetime, "%Y-%m-%d %H:%M:%S")
            if booking_dt < datetime.now():
                return "The provided booking time has already passed. Please choose a future date and time."
            
        except ValueError:
            return "Invalid date time format. Please provide the date and time again."
        
        query += " AND booking_datetime = ?"
        params.append(booking_datetime)

    cursor.execute(query, params)
    row = cursor.fetchone()

    try:
        if row:
            _, massage_type, booking_datetime, booked, _ = row
            if booked:
                conn.close()
                return "Massage is already booked by another user. Can you please adjust your preferences?"
            else:
                column_names = [column[0] for column in cursor.description]
                result = [dict(zip(column_names, row))]

                conn.close()
                return f"I have found this bookings result for your email:\n{result}"
        else:
            conn.close()
            return "Sorry, no massages found for either the given massage type or booking date. Can you please adjust your preferences?"
    except:
        conn.close()
        return "Sorry, there was a problem with my understanding. Can you please try again"      

@tool
async def book_bookings(
    config: RunnableConfig,
    query: Optional[str] = None,
    booking_id: Optional[int] = None, 
    massage_type: Optional[str] = None,
    booking_datetime: Optional[str] = None
):
    """
    User explicitly asks to book a massage, place it using the massage ID, massage type and booking date time.

    Args:
        query: Customer's question related to booking
        booking_id: The ID of the massage to book.
        massage_type: Customer's preferred massage type from only this list ['Foot', 'Swedish', 'Deep Tissue', 'Sports']
        booking_datetime: Customer's preferred booking time (Format: YYYY-MM-DD HH:MM:SS)

    Returns:
        str: A message indicating whether the massage was successfully booked or not.
    """

    configuration = config.get("configurable", {})
    company_id = configuration.get("company_id", None)
    bot_id = configuration.get("bot_id", None)
    workspace_id = configuration.get("workspace_id", None)
    email = configuration.get("email", None)

    if not email or not company_id or not bot_id or not workspace_id:
        return "Sorry, there was a problem with the configuration. Can you please try again" 

    db = await connect()
    workspace_collections = db['workspace']
    
    workspace_record = await workspace_collections.find_one({"bot_id": bot_id, "workspace_id": workspace_id, "is_active": 1}) 
    llm = await llm_selection(workspace_record)

    if query:
        class grade(BaseModel):
            """Determine if the user intends to book a massage."""
            binary_score: str = Field(
                description=(
                    "Evaluate the user query and respond with 'yes' or 'no'. "
                    "Answer 'yes' **only** if the user clearly expresses an intention to book a massage, such as 'I want to book' or 'Please book a massage'. "
                    "Answer 'no' if the user is unsure, asking about availability, or does not explicitly confirm a booking."
                )
            )
    
        structured_llm = llm.with_structured_output(grade)
        scored_result = structured_llm.invoke(query) 

        try:
            if not scored_result or scored_result.binary_score == "no":
                return "Sorry, there was a problem with my understanding. Can you please try again."  
        except:
            return "Sorry, there was a problem with my understanding. Can you please try again."  

    conn = sqlite3.connect(db_booking)
    cursor = conn.cursor()

    if not (booking_id or (booking_datetime and massage_type)):
        return "Sorry, there was a problem with the booking. Can you please try again."
    
    params = []
    query = "SELECT * FROM bookings WHERE 1 = 1"

    if booking_id:
        query += " AND booking_id = ?"
        params.append(booking_id)

    if massage_type:
        if massage_type not in ALLOWED_MASSAGE_TYPES:
            return f"Invalid massage type. Please choose from the following options: {', '.join(ALLOWED_MASSAGE_TYPES)}."
    
        query += " AND massage_type = ?"
        params.append(massage_type)

    if booking_datetime:
        try:
            booking_dt = datetime.strptime(booking_datetime, "%Y-%m-%d %H:%M:%S")
            if booking_dt < datetime.now():
                return "The provided booking time has already passed. Please choose a future date and time."
        except ValueError:
            return "Invalid date time format. Please provide the date and time again."
        
        query += " AND booking_datetime = ?"
        params.append(booking_datetime)

    cursor.execute(query, params)
    conn.commit()

    row = cursor.fetchone()

    try:
        if row:
            booking_id, massage_type, booking_datetime, booked, booked_by = row
            if booked:
                return "Massage is already booked by another user. Can you please adjust your preferences?"
            else:
                cursor.execute("UPDATE bookings SET booked = 1, booked_by = ? WHERE booking_id = ?", (email, booking_id,))
                conn.commit()
                conn.close()
                return f"Massage is successfully booked for {email}. Thank you for your booking and make sure to be on time for the appointment."
        else:
            conn.close()
            if massage_type and booking_datetime:
                conn.close()
                return "Sorry, no massages found for either the given massage type or booking date. Can you adjust your preferences?" 
    except:
        conn.close()
        return "Sorry, there was a problem with the booking. Can you please try again."  

@tool
async def cancel_bookings(
    config: RunnableConfig,
    query: Optional[str] = None,
    massage_type: Optional[str] = None,
    booking_datetime: Optional[str] = None,
    booking_id: Optional[int] = None
) -> str:
    """
    User explicitly asks to cancel a massage, cancel it using the massage ID.

    Args:
        query (str): Customer's question related to booking
        booking_id (int): The ID of the massage to cancel.
        massage_type (str): Customer's preferred massage type from only this list ['Foot', 'Swedish', 'Deep Tissue', 'Sports']
        booking_datetime (str): Customer's preferred booking time (Format: YYYY-MM-DD HH:MM:SS)

    Returns:
        str: A message indicating whether the massage was successfully cancelled or not.
    """

    configuration = config.get("configurable", {})
    company_id = configuration.get("company_id", None)
    bot_id = configuration.get("bot_id", None)
    workspace_id = configuration.get("workspace_id", None)
    email = configuration.get("email", None)

    if not email or not company_id or not bot_id or not workspace_id:
        return "Sorry, there was a problem with the configuration. Can you please try again" 

    db = await connect()
    workspace_collections = db['workspace']
    
    workspace_record = await workspace_collections.find_one({"bot_id": bot_id, "workspace_id": workspace_id, "is_active": 1}) 
    llm = await llm_selection(workspace_record)
    
    if query:
        class grade(BaseModel):
            """Determine if the user intends to book a massage."""
            binary_score: str = Field(
                description = (
                    "Evaluate the user query and respond with 'yes' or 'no'. "
                    "Answer 'yes' **only** if the user clearly expresses an intention to book a massage, such as 'I want to book' or 'Please book a massage'. "
                    "Answer 'no' if the user is unsure, asking about availability, or does not explicitly confirm a booking."
                )
            )

        structured_llm = llm.with_structured_output(grade)
        scored_result = structured_llm.invoke(query)

        try:
            if not scored_result or scored_result.binary_score == "no":
                return "Sorry, there was a problem with my understanding. Can you please try again."  
        except:
            return "Sorry, there was a problem with my understanding. Can you please try again."  

    conn = sqlite3.connect(db_booking)
    cursor = conn.cursor()

    params = []
    query = "SELECT * FROM bookings WHERE booked_by = ?"
    params.append(email)

    if booking_id:
        query += " AND booking_id = ?"
        params.append(booking_id)

    if massage_type:
        if massage_type not in ALLOWED_MASSAGE_TYPES:
            return f"Invalid massage type. Please choose from the following options: {', '.join(ALLOWED_MASSAGE_TYPES)}."
    
        query += " AND massage_type = ?"
        params.append(massage_type)

    if booking_datetime:
        try:
            booking_dt = datetime.strptime(booking_datetime, "%Y-%m-%d %H:%M:%S")
            if booking_dt < datetime.now():
                return "The provided booking time has already passed. Please choose a future date and time."
        except ValueError:
            return "Invalid date time format. Please provide the date and time again."
        
        query += " AND booking_datetime = ?"
        params.append(booking_datetime)

    cursor.execute(query, params)
    rows = cursor.fetchall()

    try:
        if len(rows) > 1:
            return "The provided criteria cancels more than two bookings. Please try cancelling one by one."
        elif len(rows) < 1:
            return f"No bookings found for the provided criteria for {email}."
        else:
            booking_id, massage_type, booking_datetime, booked, booked_by = rows[0]

            cursor.execute("UPDATE bookings SET booked = 0, booked_by = ? WHERE booking_id = ?", ('', booking_id,))
            conn.commit()
            conn.close()

            return f"Massage is successfully cancelled for {email}. Sorry for any inconvenience and let me know if you have any complaints."
    except:
        conn.close()
        return "Sorry, there was a problem with the cancellation. Can you please try again"  

@tool
def get_bookings(
    config: RunnableConfig,
    massage_type: Optional[str] = None,
    booking_datetime: Optional[str] = None,
):
    """Search for personal bookings made by the user. It can be on a specified criteria or just in general.
    
    massage_type: Customer's preferred massage type from only this list ['Foot', 'Swedish', 'Deep Tissue', 'Sports']
    booking_datetime: Customer's preferred booking time (Format: YYYY-MM-DD HH:MM:SS)
    """

    configuration = config.get("configurable", {})
    email = configuration.get("email", None)
    if not email:
        raise ValueError("No email provided is configured. Try again.")

    conn = sqlite3.connect(db_booking)
    cursor = conn.cursor()

    params = []
    query = "SELECT * FROM bookings WHERE 1 = 1"

    if massage_type:
        if massage_type not in ALLOWED_MASSAGE_TYPES:
            return f"Invalid massage type. Please choose from the following options: {', '.join(ALLOWED_MASSAGE_TYPES)}."
    
        query += " AND massage_type = ?"
        params.append(massage_type)

    if booking_datetime:
        try:
            booking_dt = datetime.strptime(booking_datetime, "%Y-%m-%d %H:%M:%S")
            if booking_dt < datetime.now():
                return "The provided booking time has already passed. Please choose a future date and time."
        except ValueError:
            return "Invalid date time format. Please provide the date and time again."
        
        query += " AND booking_datetime = ?"
        params.append(booking_datetime)

    query += " AND booked_by = ?"
    params.append(email)    

    cursor.execute(query, params)
    rows = cursor.fetchall()

    if not rows:
        conn.close()
        return "Sorry, no massages found for either the given massage type or booking date. Are you sure that you have booked any massage?"
    
    column_names = [column[0] for column in cursor.description]
    results = [dict(zip(column_names, row)) for row in rows]

    cursor.close()
    conn.close()

    return f"I have found these bookings results for your email:\n{results}"

class Assistant:
    def __init__(self, runnable: Runnable):
        self.runnable = runnable

    def __call__(self, state: State, config: RunnableConfig):
        while True:
            
            configuration = config.get("configurable", {})
            company_id = configuration.get("company_id", None)
            bot_id = configuration.get("bot_id", None)
            workspace_id = configuration.get("workspace_id", None)
            email = configuration.get("email", None)

            state = {**state, "company_id": company_id, 'bot_id': bot_id, 'workspace_id': workspace_id, 'email': email}
            result = self.runnable.invoke(state)

            if not result.tool_calls and (
                not result.content
                or isinstance(result.content, list)
                and not result.content[0].get("text")
            ):
                messages = state["messages"] + [("user", "Respond with a real output.")]
                state = {**state, "messages": messages}
            else:
                break
        return {"messages": result}
    
async def client_graph(
    bots_record, workspace_record, embeddings_record, configuration_record, text, session_id
):
    try:
        max_sessions = workspace_record['sessions_limit']
        
        if await agent_involved_chat(
            bots_record, workspace_record, configuration_record, session_id, text
        ):
            return "Response has been created"
        
        if await max_allowed_chats(workspace_record['workspace_id'], session_id, bots_record['bot_name'], max_sessions):
            return "No agent is available at the moment. Try again later!"
        
        if text == language_arabic or text == language_english:
            response = await client_language_graph(text, bots_record, workspace_record, configuration_record, session_id)

        elif text == human_end_message:
            response = await client_goodbye_graph(bots_record, workspace_record, configuration_record, session_id)
        
        elif text == transfer_message:
            response = await client_transfer_graph(bots_record, workspace_record, configuration_record, session_id)

        else:
            response = await client_conversation_graph(text, bots_record, workspace_record, embeddings_record, configuration_record, session_id)

        return response
        
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")
    
async def graph_create(
    workspace_record
):
    try:
        model = await llm_selection(workspace_record)

        tools = [search_bookings, book_bookings, cancel_bookings, massage_information_retrieval, get_bookings]
        model_with_tools = model.bind_tools(tools)

        assistant_prompt = ChatPromptTemplate.from_messages(
            [
                (
                    "system",
                    workspace_record['system_prompt'] + "\nCurrent time: {time}",
                ),
                ("placeholder", "{messages}"),
            ]
        ).partial(time=datetime.now)

        assistant_runnable = assistant_prompt | model_with_tools

        graph = StateGraph(State)

        graph.add_node("assistant", Assistant(assistant_runnable))
        graph.add_node("tools", create_tool_node_with_fallback(tools))
        graph.add_edge(START, "assistant")
        graph.add_conditional_edges(
            "assistant",
            tools_condition,
        )
        graph.add_edge("tools", "assistant")

        return graph
    
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")
    
async def client_language_graph(
    text, bots_record, workspace_record, configuration_record, session_id
):
    try:
        if text == language_arabic:
            display_message = display_language_arabic
        elif text == language_english:
            display_message = display_language_english

        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)

        messages_collections = db['messages']
        profiles_collections = db['profiles']
        
        message_record = await messages_collections.find_one({"workspace_id": workspace_record['workspace_id'], "session_id": session_id})
        profiles_record = await profiles_collections.find_one({"workspace_id": workspace_record['workspace_id'], "session_id": session_id})

        human_time = current_time()

        message_record = await messages_collections.find_one({"session_id": session_id})
        if message_record: 
            message_record['roles'].append({
                "type": 'human', "text": display_message, "timestamp": human_time, "input_tokens": None, 
                "sentiment": None, 'id': str(uuid.uuid4())
            })

            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})
            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"latest_timestamp": human_time}})

        else:
            document = {"session_id": session_id, "roles": [{
                "type": 'human', "text": display_message, "timestamp": human_time, "input_tokens": None, "sentiment": None, 
                "id": str(str(uuid.uuid4()))}], 'timeout': bots_record['timeout'], 'language': None, 'sentiment': None, 
                'agent_sentiment': None, 'tags': [None], 'slug': None, 'workspace_id': workspace_record['workspace_id'], 
                'end_conversation': 0, 'transfer_conversation': 0, 'human_intervention': 0, 'agent_expiry': 0, 
                'latest_timestamp': human_time
            }
            
            await messages_collections.insert_one(document)

            profiles_record = await profiles_collections.find_one({'session_id': session_id})
            await profiles_collections.update_one({"_id": profiles_record["_id"]}, {"$set": {"preference": text}}) 

        profiles_record = await profiles_collections.find_one({'session_id': session_id})
        await profiles_collections.update_one({"_id": profiles_record["_id"]}, {"$set": {"latest_timestamp": human_time}})   

        graph = await graph_create(workspace_record)
                      
        async with AsyncMongoDBSaver.from_conn_info(host = host, user = username, password = password, port = 27017, db_name = "checkpoints") as checkpointer:
            agent = graph.compile(checkpointer = checkpointer)
            input = {"messages": [HumanMessage(display_message)]}
            config = {
                "configurable": {
                    "thread_id": session_id, "company_id": workspace_record['company_id'], "bot_id": workspace_record['bot_id'], 
                    "workspace_id": workspace_record['workspace_id'], "email": profiles_record['email']
                }
            }

            messages = await agent.ainvoke(input, config = config)
            for response in messages['messages'][::-1]:
                if isinstance(response, AIMessage) and response.content: 
                    input_tokens, output_tokens = 0, 0
                    if workspace_record['llm'] == 'ollama':
                        input_tokens = response.response_metadata['prompt_eval_count']
                        output_tokens = response.response_metadata['eval_count']
                    else:
                        input_tokens = response.response_metadata['token_usage']['prompt_tokens']
                        output_tokens = response.response_metadata['token_usage']['completion_tokens']

                    bot_time = current_time()

                    message_record = await messages_collections.find_one({"session_id": session_id})

                    message_record['roles'].append({
                        "type": 'ai-agent', "text": response.content, "timestamp": bot_time,
                        "output_tokens": output_tokens, "sentiment": None, 'id': str(uuid.uuid4())
                    })

                    profiles_record = await profiles_collections.find_one({'session_id': session_id})
                    await profiles_collections.update_one({"_id": profiles_record["_id"]}, {"$set": {"latest_timestamp": bot_time}})  

                    await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})
                    await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"latest_timestamp": bot_time}})

                    if configuration_record["client_query"]:
                        data = {
                            'text': display_message
                        }

                        try:
                            sentiment = requests.post(sentiment_url, headers = sentiment_headers, data = data, timeout = 8, verify = False)
                            sentiment_human = json.loads(sentiment.text)

                            message_record = await messages_collections.find_one({'session_id': session_id})

                            for role in message_record['roles']:
                                if not role['sentiment'] and role['type'] == 'human':
                                    role['sentiment'] = sentiment_human['sentiment'] 
                        except:
                            for role in message_record['roles']:
                                if not role['sentiment'] and role['type'] == 'human':
                                    role['sentiment'] = 'Neutral'
                        
                        await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})   

                    if configuration_record["bot_response"]:      
                        try:
                            sentiment = requests.post(sentiment_url, headers = sentiment_headers, data = data, timeout = 8, verify = False)
                            sentiment_ai = json.loads(sentiment.text)

                            message_record = await messages_collections.find_one({'session_id': session_id})

                            for role in message_record['roles']:
                                if not role['sentiment'] and role['type'] == 'ai-agent':
                                    role['sentiment'] = sentiment_ai['sentiment'] 
                        except:
                            for role in message_record['roles']:
                                if not role['sentiment'] and role['type'] == 'ai-agent':
                                    role['sentiment'] = 'Neutral'                            

                        await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})

                    message_record = await messages_collections.find_one({'session_id': session_id})

                    for role in message_record['roles']:
                        if role['type'] == 'human' and not role['input_tokens']:
                            role['input_tokens'] = input_tokens

                    await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})

                    profiles_record = await profiles_collections.find_one({'session_id': session_id})
                    await profiles_collections.update_one({"_id": profiles_record["_id"]}, {"$set": {"latest_timestamp": bot_time}})  

                    return response.content

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")
    
async def client_goodbye_graph(
    bots_record, workspace_record, configuration_record, session_id
):
    
    try:
        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)

        messages_collections = db['messages']
        profiles_collections = db['profiles']
        
        message_record = await messages_collections.find_one({"workspace_id": workspace_record['workspace_id'], "session_id": session_id})
        profiles_record = await profiles_collections.find_one({"workspace_id": workspace_record['workspace_id'], "session_id": session_id})

        db_temp = await connect('checkpoints')
        checkpoints_collections = db_temp['checkpoints']
        checkpoint_writes_collections = db_temp['checkpoint_writes']
        await checkpoints_collections.delete_many({'thread_id': session_id})
        await checkpoint_writes_collections.delete_many({'thread_id': session_id})

        human_time = current_time()

        if profiles_record['queue'] == 'web':
            if profiles_record['preference'] == language_english:
                input_tokens = None
                display_message = display_human_end_message_english

            elif profiles_record['preference'] == language_arabic:
                input_tokens = None
                display_message = display_human_end_message_arabic

        if message_record: 
            if configuration_record["client_query"]:
                message_record['roles'].append({
                    "type": 'human', "text": display_message, "timestamp": human_time, "input_tokens": input_tokens, 
                    "sentiment": "Neutral", 'id': str(uuid.uuid4())
                })
            else:
                message_record['roles'].append({
                    "type": 'human', "text": display_message, "timestamp": human_time, "input_tokens": input_tokens, 
                    "sentiment": None, 'id': str(uuid.uuid4())
                })

            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})
            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"latest_timestamp": human_time}})

        else:
            if configuration_record["client_query"]:
                temp = {
                    "type": 'human', "text": display_message, "timestamp": human_time, "input_tokens": input_tokens, 
                    "sentiment": "Neutral", 'id': str(uuid.uuid4())
                }
            else:
                temp = {
                    "type": 'human', "text": display_message, "timestamp": human_time, "input_tokens": input_tokens, 
                    "sentiment": None, 'id': str(uuid.uuid4())
                }

            new_slug = None
        
            document = {
                "session_id": session_id, "roles": [temp], 'timeout': bots_record['timeout'], 'language': None, 'sentiment': None, 
                'agent_sentiment': None, 'tags': [None], 'slug': new_slug, 'workspace_id': workspace_record['workspace_id'], 
                'end_conversation': 0, 'transfer_conversation': 0, 'human_intervention': 0, 'agent_expiry': 0, 
                'latest_timestamp': human_time
            }

            await messages_collections.insert_one(document)

        profiles_record = await profiles_collections.find_one({'session_id': session_id})
        await profiles_collections.update_one({"_id": profiles_record["_id"]}, {"$set": {"latest_timestamp": human_time}})

        if profiles_record['queue'] == 'web':
            response = await client_goodbye_web_response(bots_record, workspace_record, configuration_record, session_id)
        elif profiles_record['queue'] == 'whatsapp':
            message_record = await messages_collections.find_one({"session_id": session_id})
            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"end_conversation": 1}})
            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"latest_timestamp": human_time}})

            response = "Response has been created"

        return response

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")
    
async def client_goodbye_web_response(
    bots_record, workspace_record, configuration_record, session_id
):
    
    try:
        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)

        messages_collections = db['messages']
        profiles_collections = db['profiles']
        
        message_record = await messages_collections.find_one({"workspace_id": workspace_record['workspace_id'], "session_id": session_id})
        profiles_record = await profiles_collections.find_one({"workspace_id": workspace_record['workspace_id'], "session_id": session_id})

        graph = await graph_create(workspace_record)
                      
        response = None
        async with AsyncMongoDBSaver.from_conn_info(host = host, user = username, password = password, port = 27017, db_name = "checkpoints") as checkpointer:
            agent = graph.compile(checkpointer = checkpointer)

            if profiles_record['preference'] == language_english:
                input = {"messages": [HumanMessage(prompt_goodbye_english)]}

            elif profiles_record['preference'] == language_arabic:
                input = {"messages": [HumanMessage(prompt_goodbye_arabic)]}

            config = {
                "configurable": {
                    "thread_id": session_id, "company_id": workspace_record['company_id'], "bot_id": workspace_record['bot_id'], 
                    "workspace_id": workspace_record['workspace_id'], "email": profiles_record['email']
                }
            }            
            
            messages = await agent.ainvoke(input, config = config)
            for response in messages['messages'][::-1]:
                if isinstance(response, AIMessage) and response.content: 

                    bot_time = current_time()

                    message_record = await messages_collections.find_one({"session_id": session_id})
                    await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"end_conversation": 1}})

                    if workspace_record['llm'] == 'ollama':
                        message_record['roles'].append({
                            "type": 'ai-agent', "text": response.content, "timestamp": bot_time, 
                            "output_tokens": response.response_metadata['eval_count'], "sentiment": None, 'id': str(uuid.uuid4())
                        })
                    else:
                        message_record['roles'].append({
                            "type": 'ai-agent', "text": response.content, "timestamp": bot_time, 
                            "output_tokens": response.response_metadata['token_usage']['completion_tokens'], "sentiment": None, 'id': str(uuid.uuid4())
                        })
                        
                    await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})
                    await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"latest_timestamp": bot_time}})

                    profiles_record = await profiles_collections.find_one({'session_id': session_id})
                    await profiles_collections.update_one({"_id": profiles_record["_id"]}, {"$set": {"latest_timestamp": bot_time}})    

                    if configuration_record["bot_response"]:              
                        data = {
                            'text': response.content
                        }

                        try:
                            sentiment = requests.post(sentiment_url, headers = sentiment_headers, data = data, timeout = 8, verify = False)
                            sentiment_ai = json.loads(sentiment.text)

                            message_record = await messages_collections.find_one({'session_id': session_id})

                            for role in message_record['roles']:
                                if not role['sentiment'] and role['type'] == 'ai-agent':
                                    role['sentiment'] = sentiment_ai['sentiment'] 
                        except:
                            for role in message_record['roles']:
                                if not role['sentiment'] and role['type'] == 'ai-agent':
                                    role['sentiment'] = 'Neutral'                            

                        await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})

                    message_record = await messages_collections.find_one({'session_id': session_id})

                    for role in message_record['roles']:
                        if role['type'] == 'human' and not role['input_tokens']:
                            if workspace_record['llm'] == 'ollama':
                                role['input_tokens'] = response.response_metadata['prompt_eval_count']
                            else:
                                role['input_tokens'] = response.response_metadata['token_usage']['prompt_tokens']

                    await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})
                
                    await client_summary_otherllms(
                        workspace_record["company_id"], workspace_record['bot_id'], workspace_record['workspace_id'], session_id
                    )
                
                    return response.content

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")
    
async def client_transfer_graph(
    bots_record, workspace_record, configuration_record, session_id
):
    
    try:
        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)

        messages_collections = db['messages']
        profiles_collections = db['profiles']

        db_temp = await connect('checkpoints')
        checkpoints_collections = db_temp['checkpoints']
        checkpoint_writes_collections = db_temp['checkpoint_writes']
        await checkpoints_collections.delete_many({'thread_id': session_id})
        await checkpoint_writes_collections.delete_many({'thread_id': session_id})

        message_record = await messages_collections.find_one({"workspace_id": workspace_record['workspace_id'], "session_id": session_id})
        profiles_record = await profiles_collections.find_one({"workspace_id": workspace_record['workspace_id'], "session_id": session_id})

        human_time = current_time()

        if profiles_record['preference'] == language_english:
            input_tokens = None
            display_message = display_transfer_message_english

        elif profiles_record['preference'] == language_arabic:
            input_tokens = None
            display_message = display_transfer_message_arabic

        if message_record: 
            if configuration_record['client_query']:
                message_record['roles'].append({
                    "type": 'human', "text": display_message, "timestamp": human_time, "input_tokens": input_tokens, 
                    "sentiment": "Neutral", 'id': str(uuid.uuid4())
                })
            else:
                message_record['roles'].append({
                    "type": 'human', "text": display_message, "timestamp": human_time, "input_tokens": input_tokens, 
                    "sentiment": None, 'id': str(uuid.uuid4())
                })

            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})
            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"latest_timestamp": human_time}})
        else:
            if configuration_record['client_query']:
                temp = {
                    "type": 'human', "text": display_message, "timestamp": human_time, "input_tokens": input_tokens, 
                    "sentiment": "Neutral", 'id': str(uuid.uuid4())
                }
            else:
                temp = {
                    "type": 'human', "text": display_message, "timestamp": human_time, "input_tokens": input_tokens,
                    "sentiment": None, 'id': str(uuid.uuid4())
                }

            new_slug = None
        
            document = {
                "session_id": session_id, "roles": [temp], 'timeout': bots_record['timeout'], 'language': None, 'sentiment': None,
                'agent_sentiment': None, 'tags': [None], 'slug': new_slug, 'workspace_id': workspace_record['workspace_id'], 'end_conversation': 0, 
                'transfer_conversation': 0, 'human_intervention': 0, 'agent_expiry': 0, 'latest_timestamp': human_time
            }

            await messages_collections.insert_one(document)

        profiles_record = await profiles_collections.find_one({'session_id': session_id})
        await profiles_collections.update_one({"_id": profiles_record["_id"]}, {"$set": {"latest_timestamp": human_time}})

        if profiles_record['queue'] == 'web':
            response = await client_transfer_web_response(bots_record, workspace_record, configuration_record, session_id)
        
        elif profiles_record['queue'] == 'whatsapp':
            message_record = await messages_collections.find_one({"session_id": session_id})
            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"transfer_conversation": 1}})
            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"end_conversation": 1}})
            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"latest_timestamp": human_time}})

            response = "Response has been created"
        
        return response

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")
    
async def client_transfer_web_response(
    bots_record, workspace_record, configuration_record, session_id
):
    
    try:
        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)

        messages_collections = db['messages']
        profiles_collections = db['profiles']
        
        message_record = await messages_collections.find_one({"workspace_id": workspace_record['workspace_id'], "session_id": session_id})
        profiles_record = await profiles_collections.find_one({"workspace_id": workspace_record['workspace_id'], "session_id": session_id})

        graph = await graph_create(workspace_record)
                      
        response = None
        async with AsyncMongoDBSaver.from_conn_info(host = host, user = username, password = password, port = 27017, db_name = "checkpoints") as checkpointer:
            agent = graph.compile(checkpointer = checkpointer)

            if profiles_record['preference'] == language_english:
                input = {
                    "messages": [HumanMessage(prompt_transfer_english)]
                }

            elif profiles_record['preference'] == language_arabic:
                input = {
                    "messages": [HumanMessage(prompt_transfer_arabic)]
                }

            config = {
                "configurable": {
                    "thread_id": session_id, "company_id": workspace_record['company_id'], "bot_id": workspace_record['bot_id'], 
                    "workspace_id": workspace_record['workspace_id'], "email": profiles_record['email']
                }
            }
                
            messages = await agent.ainvoke(input, config = config)
            for response in messages['messages'][::-1]:
                if isinstance(response, AIMessage) and response.content: 

                    bot_time = current_time()

                    message_record = await messages_collections.find_one({"session_id": session_id})
                    await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"transfer_conversation": 1}})

                    if workspace_record['llm'] == 'ollama':
                        message_record['roles'].append({
                            "type": 'ai-agent', "text": response.content.replace('"', ''), "timestamp": bot_time, 
                            "output_tokens": response.response_metadata['eval_count'], "sentiment": None, 'id': str(uuid.uuid4())
                        })
                    else:
                        message_record['roles'].append({
                            "type": 'ai-agent', "text": response.content.replace('"', ''), "timestamp": bot_time, 
                            "output_tokens": response.response_metadata['token_usage']['completion_tokens'], "sentiment": None, 
                            'id': str(uuid.uuid4())
                        })

                    if configuration_record['auto_assignment']:
                        queue = transfer_queue + f":{workspace_record['bot_id']}:{workspace_record['workspace_id']}"
                        await enqueue(session_id, queue)

                    await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})
                    await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"latest_timestamp": bot_time}})

                    profiles_record = await profiles_collections.find_one({'session_id': session_id})
                    await profiles_collections.update_one({"_id": profiles_record["_id"]}, {"$set": {"latest_timestamp": bot_time}})

                    if configuration_record["bot_response"]:           
                        data = {
                            'text': response.content
                        }

                        try:
                            sentiment = requests.post(sentiment_url, headers = sentiment_headers, data = data, timeout = 8, verify = False)
                            sentiment_ai = json.loads(sentiment.text)

                            message_record = await messages_collections.find_one({'session_id': session_id})

                            for role in message_record['roles']:
                                if not role['sentiment'] and role['type'] == 'ai-agent':
                                    role['sentiment'] = sentiment_ai['sentiment'] 
                        except:
                            for role in message_record['roles']:
                                if not role['sentiment'] and role['type'] == 'ai-agent':
                                    role['sentiment'] = 'Neutral'                            

                        await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})

                    message_record = await messages_collections.find_one({'session_id': session_id})

                    for role in message_record['roles']:
                        if role['type'] == 'human' and not role['input_tokens']:
                            if workspace_record['llm'] == 'ollama':
                                role['input_tokens'] = response.response_metadata['prompt_eval_count']
                            else:
                                role['input_tokens'] = response.response_metadata['token_usage']['prompt_tokens']

                    await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})

                    await client_summary_otherllms( 
                        workspace_record["company_id"], workspace_record['bot_id'], workspace_record['workspace_id'], session_id
                    )

                    await client_suggestions_otherllms( 
                        workspace_record["company_id"], workspace_record['bot_id'], workspace_record['workspace_id'], session_id
                    )

                    return response.content
        
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")

async def client_conversation_graph(
    text, bots_record, workspace_record, embeddings_record, configuration_record, session_id
):
    
    try:
        slug = bots_record['bot_name'] + slug_db
        db = await connect(slug)

        messages_collections = db['messages']
        profiles_collections = db['profiles']
        
        message_record = await messages_collections.find_one({"workspace_id": workspace_record['workspace_id'], "session_id": session_id})
        profiles_record = await profiles_collections.find_one({"workspace_id": workspace_record['workspace_id'], "session_id": session_id})

        human_time = current_time()

        message_record = await messages_collections.find_one({"session_id": session_id})
        if message_record: 
            message_record['roles'].append({
                "type": 'human', "text": text, "timestamp": human_time, "input_tokens": None, "sentiment": None, 'id': str(uuid.uuid4())
            })

            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})
            await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"latest_timestamp": human_time}})

        else:
            document = {"session_id": session_id, "roles": [{
                "type": 'human', "text": text, "timestamp": human_time, "input_tokens": None, "sentiment": None, 'id': str(uuid.uuid4())
                }], 'timeout': bots_record['timeout'], 'language': None, 'sentiment': None, 'agent_sentiment': None, 'tags': [None], 
                'slug': None, 'workspace_id': workspace_record['workspace_id'], 'end_conversation': 0, 'transfer_conversation': 0, 
                'human_intervention': 0, 'agent_expiry': 0, 'latest_timestamp': human_time}
            
            await messages_collections.insert_one(document)

        profiles_record = await profiles_collections.find_one({'session_id': session_id})
        await profiles_collections.update_one({"_id": profiles_record["_id"]}, {"$set": {"latest_timestamp": human_time}})  

        graph = await graph_create(workspace_record)
                      
        response = None
        async with AsyncMongoDBSaver.from_conn_info(host = host, user = username, password = password, port = 27017, db_name = "checkpoints") as checkpointer:
            agent = graph.compile(checkpointer = checkpointer)

            input = {"messages": [HumanMessage(text)]}

            config = {
                "configurable": {
                    "thread_id": session_id, "company_id": workspace_record['company_id'], "bot_id": workspace_record['bot_id'], 
                    "workspace_id": workspace_record['workspace_id'], "email": profiles_record['email']
                }
            }            

            messages = await agent.ainvoke(input, config = config)
            for response in messages['messages'][::-1]:
                if isinstance(response, AIMessage) and response.content:                   
        
                    input_tokens, output_tokens = 0, 0
                    if workspace_record['llm'] == 'ollama':
                        input_tokens = response.response_metadata['prompt_eval_count']
                        output_tokens = response.response_metadata['eval_count']
                    else:
                        input_tokens = response.response_metadata['token_usage']['prompt_tokens']
                        output_tokens = response.response_metadata['token_usage']['completion_tokens']
                
                    bot_time = current_time()

                    message_record = await messages_collections.find_one({"session_id": session_id})

                    message_record['roles'].append({
                        "type": 'ai-agent', "text": response.content, "timestamp": bot_time, "output_tokens": output_tokens, 
                        "sentiment": None, 'id': str(uuid.uuid4())
                    })

                    profiles_record = await profiles_collections.find_one({'session_id': session_id})
                    await profiles_collections.update_one({"_id": profiles_record["_id"]}, {"$set": {"latest_timestamp": bot_time}})  

                    await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})
                    await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"latest_timestamp": bot_time}})

                    if configuration_record['client_query']:
                        data = {
                            'text': text
                        }

                        try:
                            sentiment = requests.post(sentiment_url, headers = sentiment_headers, data = data, timeout = 8, verify = False)
                            sentiment_human = json.loads(sentiment.text)

                            message_record = await messages_collections.find_one({'session_id': session_id})

                            for role in message_record['roles']:
                                if not role['sentiment'] and role['type'] == 'human':
                                    role['sentiment'] = sentiment_human['sentiment'] 
                        except:
                            for role in message_record['roles']:
                                if not role['sentiment'] and role['type'] == 'human':
                                    role['sentiment'] = 'Neutral'
                                    
                        await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})   

                    if configuration_record['bot_response']:                       
                        data = {
                            'text': response.content
                        }

                        try:
                            sentiment = requests.post(sentiment_url, headers = sentiment_headers, data = data, timeout = 8, verify = False)
                            sentiment_ai = json.loads(sentiment.text)

                            message_record = await messages_collections.find_one({'session_id': session_id})

                            for role in message_record['roles']:
                                if not role['sentiment'] and role['type'] == 'ai-agent':
                                    role['sentiment'] = sentiment_ai['sentiment'] 
                        except:
                            for role in message_record['roles']:
                                if not role['sentiment'] and role['type'] == 'ai-agent':
                                    role['sentiment'] = 'Neutral'                            

                        await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})  

                    message_record = await messages_collections.find_one({'session_id': session_id})

                    for role in message_record['roles']:
                        if role['type'] == 'human' and not role['input_tokens']:
                            role['input_tokens'] = input_tokens

                    await messages_collections.update_one({"_id": message_record["_id"]}, {"$set": {"roles": message_record['roles']}})   

                    return response.content

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code = 500, detail = f"An error occurred: {str(e)}")