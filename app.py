from typing import Any, Dict, List
import os
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
import snowflake.connector
import requests
import pandas as pd
from snowflake.core import Root
from dotenv import dotenv_values
import matplotlib
import time
import matplotlib.pyplot as plt 
from cortex_chat import CortexChat

matplotlib.use('Agg')

# # Environment Variables
# Read the .env file using dotenv_values()
env_vars = dotenv_values(".env")

# Extract the variables from the loaded dictionary
USER = env_vars.get("USER")
ACCOUNT = env_vars.get("ACCOUNT")
PASSWORD = env_vars.get("PASSWORD")
ROLE = env_vars.get("ROLE")
WAREHOUSE = env_vars.get("WAREHOUSE")

ANALYST_ENDPOINT = env_vars.get("ANALYST_ENDPOINT")
RSA_PRIVATE_KEY_PATH = env_vars.get("RSA_PRIVATE_KEY_PATH")

SUPPORT_TICKETS_SEMANTIC_MODEL = env_vars.get("SUPPORT_TICKETS_SEMANTIC_MODEL")
SUPPLY_CHAIN_SEMANTIC_MODEL = env_vars.get("SUPPLY_CHAIN_SEMANTIC_MODEL")

SLACK_APP_TOKEN = env_vars.get("SLACK_APP_TOKEN")
SLACK_BOT_TOKEN = env_vars.get("SLACK_BOT_TOKEN")

ENABLE_CHARTS = True
DEBUG = True
DASHES = '-'*50

# Optionally, you can print them to verify
if DEBUG:
    print(f"\n{DASHES}\n")
    print("Setting the following environment variables:")
    for key, value in env_vars.items():
        if key != "PASSWORD":  # Exclude printing the password
            print(f"{key}={value}")
    print(f"\n{DASHES}\n")


# Initialize Slack App
app = App(token=SLACK_BOT_TOKEN)

# Initialize Snowflake and CortexChat
conn = snowflake.connector.connect(
    user=USER,
    authenticator="SNOWFLAKE_JWT",
    private_key_file=RSA_PRIVATE_KEY_PATH,
    account=ACCOUNT,
    role=ROLE,
    warehouse=WAREHOUSE
)
cortex_chat = CortexChat(ACCOUNT, USER, RSA_PRIVATE_KEY_PATH, ANALYST_ENDPOINT, SUPPORT_TICKETS_SEMANTIC_MODEL, SUPPLY_CHAIN_SEMANTIC_MODEL)

if DEBUG:
    cortex_chat.print_vars()


Root = Root(conn)

@app.message("hello")
def message_hello(message, say):
    say(f"Hey there <@{message['user']}>!")
    say(text="Let's BUILD", blocks=[
        {"type": "header", "text": {"type": "plain_text", "text": ":snowflake: Let's BUILD!"}}
    ])

@app.event("message")
def handle_message_events(ack, body, say):
    ack()
    process_analyst_message(body['event']['text'], say)

@app.command("/asksnowflake")
def ask_cortex(ack, body, say):
    ack()
    process_analyst_message(body['text'], say)

def process_analyst_message(prompt, say) -> Any:
    say_question(prompt, say)
    response = cortex_chat.query_cortex_analyst(prompt)
    display_analyst_content(response["message"]["content"], say)

def say_question(prompt, say):
    say(text=f"Question: {prompt}", blocks=[
        {"type": "header", "text": {"type": "plain_text", "text": f"Question: {prompt}"}},
        {"type": "divider"},
        {"type": "section", "text": {"type": "plain_text", "text": "Snowflake Cortex Analyst is generating a response. Please wait..."}},
        {"type": "divider"}
    ])

def display_analyst_content(content: List[Dict[str, str]], say):
    for item in content:
        if item["type"] == "sql":
            say(text="Generated SQL", blocks=[
                {"type": "rich_text", "elements": [{"type": "rich_text_preformatted", "elements": [{"type": "text", "text": item['statement']}]}]}
            ])
            df = pd.read_sql(item["statement"], conn)
            say(text="Answer:", blocks=[
                {"type": "rich_text", "elements": [{"type": "rich_text_preformatted", "elements": [{"type": "text", "text": df.to_string()}]}]}
            ])
            if ENABLE_CHARTS and len(df.columns) > 1:
                try:
                    chart_img_url = plot_chart(df)
                    if chart_img_url:
                        say(text="Chart", blocks=[
                            {"type": "image", "title": {"type": "plain_text", "text": "Chart"}, "block_id": "image", "slack_file": {"url": chart_img_url}, "alt_text": "Chart"}
                        ])
                except Exception as e:
                    print(f"Warning: Unable to generate chart - {e}")
        elif item["type"] == "suggestions":
            suggestions = "\n- ".join(item['suggestions'])
            say(text=f"You may try these suggested questions:\n- {suggestions}")

def plot_chart(df):
    plt.figure(figsize=(10, 6))
    plt.pie(df[df.columns[1]], labels=df[df.columns[0]], autopct='%1.1f%%', startangle=90)
    plt.axis('equal')
    file_path = 'chart.jpg'
    plt.savefig(file_path, format='jpg')
    file_upload_url = app.client.files_getUploadURLExternal(filename=file_path, length=os.path.getsize(file_path))
    file_id = file_upload_url['file_id']
    with open(file_path, 'rb') as f:
        requests.post(file_upload_url['upload_url'], files={'file': f})
    response = app.client.files_completeUploadExternal(files=[{"id": file_id, "title": "chart"}])
    time.sleep(2)
    return response['files'][0]['permalink'] if response.get('files') else None

if __name__ == "__main__":
    SocketModeHandler(app, SLACK_APP_TOKEN).start()
