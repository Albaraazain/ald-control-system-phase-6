import os
import asyncio
from datetime import datetime, timezone
from supabase import create_client, Client
from realtime import RealtimeClient
from realtime import RealtimeChannel

# Get environment variables
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
REALTIME_URL = f"wss://{SUPABASE_URL.split('://')[1]}/realtime/v1"

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Initialize Realtime client
client = RealtimeClient(REALTIME_URL, SUPABASE_KEY)

async def main():
    # Connect to Realtime
    await client.connect()

    # Create channel
    channel = client.channel('db-changes')

    # Set up callback for insert events on recipe_commands
    def on_insert(payload):
        if payload['eventType'] == 'INSERT' and payload['table'] == 'recipe_commands':
            record = payload['new']
            if record['status'] == 'pending':
                command_id = record['id']
                print(f"New pending command: {command_id}, type: {record['type']}, parameters: {record['parameters']}")
                # Try to update status to 'processing' if it's still 'pending'
                result = supabase.table('recipe_commands').update({'status': 'processing'}).eq('id', command_id).eq('status', 'pending').execute()
                if result.count > 0:
                    # Successfully claimed the command
                    print(f"Processing command {command_id}")
                    # Simulate processing
                    # For now, just set to 'completed'
                    supabase.table('recipe_commands').update({'status': 'completed', 'executed_at': datetime.now(timezone.utc).isoformat()}).eq('id', command_id).execute()
                    print(f"Command {command_id} completed")
                else:
                    print(f"Command {command_id} already being processed or status changed")

    # Subscribe to insert events
    channel.on_postgres_changes(event="INSERT", schema="public", table="recipe_commands", callback=on_insert)

    # Subscribe to the channel
    await channel.subscribe()

    # Keep the script running
    while True:
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())