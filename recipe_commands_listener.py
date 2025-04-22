import os
import asyncio
from datetime import datetime, timezone
from dotenv import load_dotenv
from supabase import create_client, Client
from supabase import create_async_client

# Load environment variables
load_dotenv()

# Get environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in .env file")

# Initialize sync client for database operations
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

async def main():
    try:
        print("Initializing async Supabase client...")
        # Initialize async client for realtime
        async_supabase = await create_async_client(SUPABASE_URL, SUPABASE_KEY)

        print("Creating realtime channel...")
        # Create channel for recipe commands
        channel = async_supabase.channel('recipe-commands')

        # Set up callback for insert events on recipe_commands
        def on_insert(payload):
            try:
                print(f"Received payload: {payload}")  # Debug log
                # Database change payloads are nested under 'data'
                record = payload['data']['record']
                if record['status'] == 'pending':
                    command_id = record['id']
                    print(f"New pending command: {command_id}, type: {record['type']}, parameters: {record['parameters']}")
                    # Try to update status to 'processing' if it's still 'pending'
                    result = supabase.table('recipe_commands').update({
                        'status': 'processing'
                    }).eq('id', command_id).eq('status', 'pending').execute()
                    
                    # Check if any rows were updated
                    if result.data and len(result.data) > 0:
                        # Successfully claimed the command
                        print(f"Processing command {command_id}")
                        # Simulate processing
                        # For now, just set to 'completed'
                        supabase.table('recipe_commands').update({
                            'status': 'completed',
                            'executed_at': datetime.now(timezone.utc).isoformat()
                        }).eq('id', command_id).execute()
                        print(f"Command {command_id} completed")
                    else:
                        print(f"Command {command_id} already being processed or status changed")
            except Exception as e:
                print(f"Error processing insert event: {str(e)}")
                print(f"Payload structure: {payload.keys()}")  # More debug info

        print("Setting up database change listener...")
        # Subscribe to database changes
        channel = (channel
            .on_postgres_changes(
                event='INSERT',
                schema='public',
                table='recipe_commands',
                callback=on_insert
            )
        )

        print("Subscribing to channel...")
        await channel.subscribe()

        print("Successfully subscribed to recipe_commands table!")
        print("Listening for recipe commands...")

        # Keep the script running
        while True:
            await asyncio.sleep(1)

    except Exception as e:
        print(f"Error in main loop: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main())