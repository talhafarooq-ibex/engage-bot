import json, aioredis

async def get_redis():
    return await aioredis.from_url("redis://localhost", decode_responses=True)

async def set_redis(redis, key, value):
    await redis.set(key, value)

async def delete_redis(redis, key):
    await redis.delete(key)

async def check_and_update_redis(redis, key, new_data):
    previous_data = await redis.get(key)

    if previous_data is None or previous_data != json.dumps(new_data):
        await redis.set(key, json.dumps(new_data))
        return True
    return False

async def delete_redis_keys(redis, websocket_id):
    async for key in redis.scan_iter(f"websocket:*:{websocket_id}*"):
        await redis.delete(key)

async def enqueue(session_id, queue_name):
    redis = await get_redis()
    await redis.lpush(queue_name, session_id)
    await redis.close()

async def dequeue(queue_name):
    redis = await get_redis()
    item = await redis.rpop(queue_name)
    await redis.close()
    return item

async def view_queue(queue_name):
    redis = await get_redis()
    items = await redis.lrange(queue_name, 0, -1) 
    await redis.close()
    return items

async def delete_from_queue(session_id, queue_name):
    redis = await get_redis()
    await redis.lrem(queue_name, 1, session_id)
    await redis.close()