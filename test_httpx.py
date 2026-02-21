import asyncio
import httpx

async def main():
    try:
        async with httpx.AsyncClient() as client:
            await client.delete('http://example.com', json={"test": 1})
    except Exception as e:
        print(f"Error client.delete: {type(e).__name__} - {e}")

    try:
        async with httpx.AsyncClient() as client:
            await client.request('DELETE', 'http://example.com', json={"test": 1})
    except Exception as e:
        print(f"Error client.request: {type(e).__name__} - {e}")

if __name__ == "__main__":
    asyncio.run(main())
