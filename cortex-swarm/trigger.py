import asyncio
import grpc
import uuid
import broker_pb2
import broker_pb2_grpc

async def trigger_task():
    print("🚀 Connecting to AetherOS Broker...")
    channel = grpc.aio.insecure_channel('127.0.0.1:50051')
    stub = broker_pb2_grpc.BrokerServiceStub(channel)
    
    payload = "Build a Python script that uses the requests library to fetch the current price of Bitcoin from the CoinGecko API. Ensure it has error handling for network failures."
    task_uuid = f"task-{uuid.uuid4().hex[:8]}"
    
    try:
        req = broker_pb2.TaskSubmission(
            task_id=task_uuid,
            task_type="langgraph_agent",
            payload=payload,
            priority=1,
            submitted_by="user_trigger_script",
            max_retries=3,
            lease_seconds=300 
        )
        
        print(f"📡 Submitting Task: {task_uuid}")
        response = await stub.SubmitTask(req)
        
        print(f"✅ Task successfully injected into the Hash Ring!")
        print(f"Response Task ID: {response.task_id}")
        print("Switch to your Worker terminal to watch the AI nodes process it!")
        
    except grpc.RpcError as e:
        print(f"❌ Failed to submit task: {e.details()}")
    finally:
        await channel.close()

if __name__ == "__main__":
    asyncio.run(trigger_task())