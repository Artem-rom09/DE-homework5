import os
import asyncio
import json
import time
from typing import List, Dict, Any, Optional, AsyncIterator
from contextlib import asynccontextmanager 

import redis.asyncio as redis
import aiomysql
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

# --- Redis Configuration ---
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_DB = int(os.environ.get("REDIS_DB", 0))

redis_client: Optional[redis.Redis] = None

# --- MySQL Configuration ---
MYSQL_HOST = os.environ.get("MYSQL_HOST", "localhost")
MYSQL_USER = os.environ.get("MYSQL_USER", "me")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "artem228")
MYSQL_DATABASE = os.environ.get("MYSQL_DATABASE", "my_ad_data")

db_pool = None 

async def get_db_connection():
    """Retrieves a connection from the MySQL connection pool."""
    global db_pool
    if not db_pool:
        raise RuntimeError("MySQL connection pool not initialized.")
    async with db_pool.acquire() as conn:
        yield conn 

# --- Lifespan Event Handler ---
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """
    Handles startup and shutdown events for the application.
    Initializes Redis and MySQL connection pools on startup,
    and closes them on shutdown.
    """
    global redis_client, db_pool

    # --- Startup Logic ---
    # Initialize Redis client
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    try:
        await redis_client.ping()
        print(f"Успішно підключено до Redis за адресою {REDIS_HOST}:{REDIS_PORT}")
    except redis.exceptions.ConnectionError as e:
        print(f"Не вдалося підключитися до Redis: {e}")
        redis_client = None 

    # Initialize MySQL connection pool using aiomysql
    try:
        db_pool = await aiomysql.create_pool(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            db=MYSQL_DATABASE, 
            autocommit=True, 
            port=3306, 
            charset='utf8mb4', 
            cursorclass=aiomysql.cursors.DictCursor, 
            maxsize=5 
        )
        # Test connection
        async with db_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT 1")
        print(f"Успішно підключено до MySQL за адресою {MYSQL_HOST}/{MYSQL_DATABASE}")
    except Exception as e:
        print(f"Не вдалося підключитися до MySQL: {e}")
        db_pool = None

    yield 

    # --- Shutdown Logic ---
    if redis_client:
        await redis_client.close()
        print("З’єднання з Redis закрито.")
    if db_pool:
        db_pool.close() 
        await db_pool.wait_closed()
        print("Пул з’єднань MySQL закрито.")

# FastAPI app initialization with lifespan
app = FastAPI(
    title="Analytics API with Redis Caching and MySQL",
    lifespan=lifespan # Assign the lifespan context manager
)

# --- Pydantic Models for API Responses ---

class CampaignPerformance(BaseModel):
    impressions: int = Field(..., description="Загальна кількість показів реклами")
    clicks: int = Field(..., description="Загальна кількість кліків по рекламі")
    ad_spend: float = Field(..., description="Загальні витрати на рекламу у валюті")
    ctr: float = Field(..., description="Коефіцієнт клікабельності (CTR) у відсотках")

class AdvertiserSpending(BaseModel):
    total_ad_spend: float = Field(..., description="Загальні витрати рекламодавця на рекламу")

class UserEngagementItem(BaseModel):
    ad_id: str = Field(..., description="ID взаємодіючої реклами (ImpressionID)")
    campaign_id: int = Field(..., description="ID кампанії, до якої належить реклама")

class UserEngagements(BaseModel):
    engagements: List[UserEngagementItem] = Field(..., description="Список реклам, з якими взаємодіяв користувач")

# --- Caching Utility Function ---

async def get_cached_or_db(
    cache_key: str,
    db_fetch_func: callable,
    ttl_seconds: int,
    force_db_query: bool = False
) -> Optional[Any]:
    """
    Отримує дані з кешу Redis або з БД, якщо не знайдено.
    Зберігає дані в кеші із заданим TTL.
    """
    if redis_client and not force_db_query:
        try:
            cached_data = await redis_client.get(cache_key)
            if cached_data:
                print(f"Cache HIT для ключа: {cache_key}")
                return json.loads(cached_data)
            print(f"Cache MISS для ключа: {cache_key}")
        except redis.exceptions.RedisError as e:
            print(f"Помилка Redis при отриманні: {e}. Запит до БД.")

    db_data = await db_fetch_func()
    if db_data is None:
        return None

    if redis_client and not force_db_query:
        try:
           
            await redis_client.setex(cache_key, ttl_seconds, json.dumps(db_data))
            print(f"Дані встановлено в кеші для ключа: {cache_key} з TTL: {ttl_seconds}s")
        except redis.exceptions.RedisError as e:
            print(f"Помилка Redis при встановленні: {e}. Дані не кешовано.")

    return db_data

# --- API Endpoints ---

@app.get("/campaign/{campaign_id}/performance", response_model=CampaignPerformance)
async def get_campaign_performance(
    campaign_id: int, 
    no_cache: bool = False
):
    """
    Отримує CTR, кліки, покази та витрати на рекламу для конкретної кампанії.
    Кешується на 30 секунд.
    """
    cache_key = f"campaign:{campaign_id}:performance"
    ttl = 30 # seconds

    async def fetch_from_db():
        
        async with await get_db_connection() as conn: 
            async with conn.cursor() as cursor:
            
                await cursor.execute(
                    "SELECT COUNT(ImpressionID) AS total_impressions, SUM(AdCost) AS total_ad_spend "
                    "FROM Impressions WHERE CampaignID = %s", (campaign_id,)
                )
                impression_data = await cursor.fetchone()

                await cursor.execute(
                    "SELECT COUNT(C.ClickID) AS total_clicks FROM Clicks C "
                    "JOIN Impressions I ON C.ImpressionID = I.ImpressionID "
                    "WHERE I.CampaignID = %s", (campaign_id,)
                )
                click_data = await cursor.fetchone()

                if not impression_data or impression_data['total_impressions'] is None:
             
                    await cursor.execute("SELECT 1 FROM Campaigns WHERE CampaignID = %s", (campaign_id,))
                    if not await cursor.fetchone():
                        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Кампанію не знайдено")
                    
                    return {
                        "impressions": 0,
                        "clicks": 0,
                        "ad_spend": 0.0,
                        "ctr": 0.0
                    }


                impressions = impression_data['total_impressions'] or 0
                clicks = click_data['total_clicks'] or 0
                ad_spend = impression_data['total_ad_spend'] or 0.0

                ctr = (clicks / impressions * 100) if impressions > 0 else 0.0

                return {
                    "impressions": impressions,
                    "clicks": clicks,
                    "ad_spend": float(ad_spend), 
                    "ctr": float(ctr)
                }

    start_time = time.perf_counter()
    try:
        result_data = await get_cached_or_db(cache_key, fetch_from_db, ttl, force_db_query=no_cache)
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Помилка при отриманні даних кампанії: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Внутрішня помилка сервера")

    end_time = time.perf_counter()
    response_time_ms = (end_time - start_time) * 1000

    print(f"Час відповіді для /campaign/{campaign_id}/performance (кешовано={not no_cache}): {response_time_ms:.2f} мс")

    return CampaignPerformance(**result_data)


@app.get("/advertiser/{advertiser_id}/spending", response_model=AdvertiserSpending)
async def get_advertiser_spending(
    advertiser_id: int, 
    no_cache: bool = False 
):
    """
    Повертає загальні витрати рекламодавця на рекламу.
    Кешується на 5 хвилин (300 секунд).
    """
    cache_key = f"advertiser:{advertiser_id}:spending"
    ttl = 5 * 60 

    async def fetch_from_db():
        async with await get_db_connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "SELECT SUM(I.AdCost) AS total_ad_spend FROM Impressions I "
                    "JOIN Campaigns C ON I.CampaignID = C.CampaignID "
                    "WHERE C.AdvertiserID = %s", (advertiser_id,)
                )
                result = await cursor.fetchone()

                if not result or result['total_ad_spend'] is None:
                    await cursor.execute("SELECT 1 FROM Advertisers WHERE AdvertiserID = %s", (advertiser_id,))
                    if not await cursor.fetchone():
                        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Рекламодавця не знайдено")
                    return {"total_ad_spend": 0.0}

                return {"total_ad_spend": float(result['total_ad_spend'])}

    start_time = time.perf_counter()
    try:
        result_data = await get_cached_or_db(cache_key, fetch_from_db, ttl, force_db_query=no_cache)
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Помилка при отриманні витрат рекламодавця: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Внутрішня помилка сервера")

    end_time = time.perf_counter()
    response_time_ms = (end_time - start_time) * 1000

    print(f"Час відповіді для /advertiser/{advertiser_id}/spending (кешовано={not no_cache}): {response_time_ms:.2f} мс")

    return AdvertiserSpending(**result_data)


@app.get("/user/{user_id}/engagements", response_model=UserEngagements)
async def get_user_engagements(
    user_id: int, 
    no_cache: bool = False 
):
    """
    Повертає реклами, з якими взаємодіяв користувач.
    Кешується на 1 хвилину (60 секунд).
    """
    cache_key = f"user:{user_id}:engagements"
    ttl = 60 

    async def fetch_from_db():
        async with await get_db_connection() as conn:
            async with conn.cursor() as cursor:
                # First, check if the user exists
                await cursor.execute("SELECT 1 FROM Users WHERE UserID = %s", (user_id,))
                user_exists = await cursor.fetchone()
                if not user_exists:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Користувача не знайдено")

                # Get all impressions for the user, which represent engagements
                await cursor.execute(
                    "SELECT ImpressionID AS ad_id, CampaignID FROM Impressions WHERE UserID = %s", (user_id,)
                )
                engagements_raw = await cursor.fetchall()

                return {"engagements": engagements_raw} # Fits UserEngagements model

    start_time = time.perf_counter()
    try:
        result_data = await get_cached_or_db(cache_key, fetch_from_db, ttl, force_db_query=no_cache)
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Помилка при отриманні взаємодій користувача: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Внутрішня помилка сервера")

    end_time = time.perf_counter()
    response_time_ms = (end_time - start_time) * 1000

    print(f"Час відповіді для /user/{user_id}/engagements (кешовано={not no_cache}): {response_time_ms:.2f} мс")

    return UserEngagements(**result_data)


@app.get("/benchmark_reset_cache")
async def reset_cache():
    """Допоміжний ендпоінт для очищення кешу Redis для бенчмаркінгу."""
    if redis_client:
        try:
            await redis_client.flushdb()
            return {"message": "Кеш Redis успішно очищено."}
        except redis.exceptions.RedisError as e:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Помилка Redis при очищенні кешу: {e}")
    return {"message": "Клієнт Redis не підключено."}

