import os
import time
import random
import requests
from google import genai
from google.genai import types
from google.genai import errors as genai_errors

# 1. Setup API Keys & URLs
GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
DATABRICKS_PAT = os.environ["DATABRICKS_PAT"]
CONNECTOR_URL = "http://localhost:8000"

client = genai.Client(api_key=GEMINI_API_KEY)

# 2. Declare Tools to Gemini
get_schema_tool = types.FunctionDeclaration(
    name="get_databricks_schema",
    description="Get tables and columns from Databricks Unity Catalog.",
    parameters=types.Schema(
        type="OBJECT",
        properties={
            "catalog": types.Schema(type="STRING", description="Catalog name, e.g., 'hackathon_db'"),
            "schema_name": types.Schema(type="STRING", description="Schema name, e.g., 'inventory_system'")
        }
    )
)

execute_sql_tool = types.FunctionDeclaration(
    name="execute_databricks_sql",
    description="Run a read-only SQL query on Databricks.",
    parameters=types.Schema(
        type="OBJECT",
        properties={
            "sql_query": types.Schema(type="STRING", description="SQL SELECT query to execute")
        },
        required=["sql_query"]
    )
)

databricks_tools = types.Tool(function_declarations=[get_schema_tool, execute_sql_tool])


# Gemini returns 503/429 when the shared model pool is saturated; these are transient.
RETRYABLE_STATUS = {429, 500, 502, 503, 504}


def send_with_retry(chat_session, message, max_attempts=5, base_delay=2.0):
    for attempt in range(1, max_attempts + 1):
        try:
            return chat_session.send_message(message)
        except genai_errors.APIError as err:
            if err.code not in RETRYABLE_STATUS or attempt == max_attempts:
                raise
            delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 1)
            print(f"[Retry {attempt}/{max_attempts - 1}] Gemini returned {err.code}; waiting {delay:.1f}s...")
            time.sleep(delay)

# 3. Create Chat Session with System Guidance
system_instruction = (
    "You are an enterprise data assistant with access to Databricks. "
    "Unless specified otherwise, look in catalog 'hackathon_db' and schema 'inventory_system'. "
    "Always discover schema tables before querying data."
)

chat = client.chats.create(
    model="gemini-flash-latest",
    config=types.GenerateContentConfig(
        system_instruction=system_instruction,
        tools=[databricks_tools],
        temperature=0.0
    )
)

# 4. Target Query
prompt = "Check the inventory system schema and show me all products with a stock count less than 10."
print(f"User Question: {prompt}\n")

response = send_with_retry(chat, prompt)

# 5. Execute Function Call Loop
headers = {"Authorization": f"Bearer {DATABRICKS_PAT}"}

while response.function_calls:
    tool_parts = []

    for call in response.function_calls:
        print(f"[Gemini Invoking Tool]: {call.name} with args {call.args}")

        try:
            if call.name == "get_databricks_schema":
                api_res = requests.post(f"{CONNECTOR_URL}/tools/get_schema", json=call.args, headers=headers, timeout=120)
            elif call.name == "execute_databricks_sql":
                api_res = requests.post(f"{CONNECTOR_URL}/tools/execute_sql", json=call.args, headers=headers, timeout=120)
            else:
                continue

            try:
                tool_output = api_res.json()
            except Exception:
                tool_output = {"error": f"HTTP {api_res.status_code}", "raw": api_res.text}

        except Exception as err:
            tool_output = {"error": str(err)}

        tool_parts.append(
            types.Part.from_function_response(
                name=call.name,
                response={"result": tool_output}
            )
        )

    if not tool_parts:
        break

    response = send_with_retry(chat, tool_parts)

print("\n--- Final Gemini Response ---")
print(response.text)