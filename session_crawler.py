import asyncio
import os
import sys
from datetime import datetime
from typing import Optional

# ---- DEPENDENCY CHECK (Automated healthcheck for Rodion's environment) ----
try:
    import pydantic
    import sqlalchemy
    import aiohttp
    from pydantic import BaseModel, Field, IPvAnyAddress
    from sqlalchemy import String, Integer, DateTime, select
    from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
    from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
except ImportError as e:
    print(f"❌ Error: Missing required dependencies in your environment: {e}")
    print("👉 Run this command to install the required stack: pip install pydantic[email] sqlalchemy aiohttp asyncpg")
    sys.exit(1)

# =====================================================================
# 🗄️ SECTION 2: MODERN SQLALCHEMY 2.0 (Declarative Base & Async Models)
# =====================================================================

class Base(DeclarativeBase):
    """Modern SQLAlchemy 2.0 mapped declarative base class."""
    pass

class GameServerTable(Base):
    """
    SQLAlchemy 2.0 Model representing the 'game_servers' table in PostgreSQL.
    Uses type-annotated Mapped attributes (SQLAlchemy 2.0 standard).
    """
    __tablename__ = "game_servers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    server_name: Mapped[str] = mapped_column(String(100), nullable=False)
    ip_address: Mapped[str] = mapped_column(String(45), nullable=False)  # Supports both IPv4 and IPv6
    port: Mapped[int] = mapped_column(Integer, nullable=False)
    current_players: Mapped[int] = mapped_column(Integer, default=0)
    max_players: Mapped[int] = mapped_column(Integer, default=32)
    status: Mapped[str] = mapped_column(String(10), default="OFF", index=True)  # Indexed for fast lookups
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class AsyncDatabaseManager:
    """
    Advanced async context manager for handling SQLAlchemy 2.0 database sessions.
    Implements __aenter__ and __aexit__ to prevent connection leaks.
    """
    def __init__(self, database_url: str):
        # pool_pre_ping=True checks connection health before issuing SQL queries
        self.engine = create_async_engine(database_url, echo=False, pool_pre_ping=True)
        self.session_factory = async_sessionmaker(
            bind=self.engine, 
            class_=AsyncSession, 
            expire_on_commit=False
        )

    async def __aenter__(self) -> AsyncSession:
        self.session = self.session_factory()
        return self.session

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            await self.session.rollback()
            print(f"⚠️ [DB MANAGER] SQL Transaction failed. Rollback executed. Error: {exc_type.__name__}")
        else:
            await self.session.commit()
        await self.session.close()

# =====================================================================
# 🐍 SECTION 1: ADVANCED PYTHON (Data Validation via Pydantic v2)
# =====================================================================

class ServerStateSchema(BaseModel):
    """
    Strict validation schema for incoming metrics from high-load Rust/CS game hosts.
    """
    ip_address: IPvAnyAddress = Field(..., description="Valid IPv4/IPv6 address of the server")
    port: int = Field(..., ge=1, le=65535, description="Query port strictly within 1-65535 range")
    server_name: str = Field(..., min_length=3, max_length=100)
    current_players: int = Field(default=0, ge=0)
    max_players: int = Field(default=32, ge=1)
    status: str = Field(default="OFF")

    class Config:
        from_attributes = True  # Allows seamless integration between SQLAlchemy and Pydantic

# =====================================================================
# 🚀 ASYNC PROCESSING ENGINE (asyncio + aiohttp + SQLAlchemy Core)
# =====================================================================

async def fetch_server_metrics_async(client: aiohttp.ClientSession, url: str) -> Optional[dict]:
    """Non-blocking async HTTP request to fetch raw JSON payload."""
    try:
        async with client.get(url, timeout=5) as response:
            if response.status == 200:
                return await response.json()
            print(f"⚠️ [NETWORK] Target host {url} returned bad status code: {response.status}")
    except Exception as e:
        print(f"❌ [NETWORK] Connection failed for {url}: {e}")
    return None

async def process_server_monitoring_job(metrics_url: str, db_url: str):
    """
    Core Worker Logic: Fetches, validates, and stores data completely
    using SQLAlchemy 2.0 and Pydantic v2 within an async pipeline.
    """
    print(f"\n🤖 [MONITORING WORKER] Starting job execution loop...")
    
    async with aiohttp.ClientSession() as client:
        raw_data = await fetch_server_metrics_async(client, metrics_url)
        
        if not raw_data:
            print(f"❌ [MONITORING WORKER] Execution aborted: No payload retrieved.")
            return

        try:
            # 1. Validate incoming data via Pydantic
            validated_state = ServerStateSchema(**raw_data)
            print(f"✅ [PYDANTIC] Payload successfully validated for: '{validated_state.server_name}'")
            
            # 2. Write to PostgreSQL using modern SQLAlchemy 2.0 context manager
            db_manager = AsyncDatabaseManager(db_url)
            async with db_manager as session:
                print(f"💾 [SQLALCHEMY 2.0] Session established. Syncing payload to Postgres...")
                
                # Executing modern SQLAlchemy 2.0 select statement
                # We use string casting for IP because Pydantic validates it as an object
                stmt = select(GameServerTable).where(
                    GameServerTable.ip_address == str(validated_state.ip_address),
                    GameServerTable.port == validated_state.port
                )
                result = await session.execute(stmt)
                existing_server = result.scalar_one_or_none()

                if existing_server:
                    # Update existing record matching unique constraints
                    existing_server.server_name = validated_state.server_name
                    existing_server.current_players = validated_state.current_players
                    existing_server.max_players = validated_state.max_players
                    existing_server.status = validated_state.status
                    print(f"🔄 [SQLALCHEMY 2.0] Existing server record updated in database.")
                else:
                    # Insert completely new server entity
                    new_server = GameServerTable(
                        server_name=validated_state.server_name,
                        ip_address=str(validated_state.ip_address),
                        port=validated_state.port,
                        current_players=validated_state.current_players,
                        max_players=validated_state.max_players,
                        status=validated_state.status
                    )
                    session.add(new_server)
                    print(f"➕ [SQLALCHEMY 2.0] New server record inserted into database.")
                
                print(f"🎉 [DB MANAGER] Transaction committed. Session closed automatically.")
                
        except pydantic.ValidationError as val_err:
            print(f"❌ [VALIDATION ERROR] Package structures are corrupted: {val_err.json()}")

# =====================================================================
# 💻 RUNTIME SIMULATION LAYER
# =====================================================================

async def main():
    print("="*75)
    print("🚀 PURE SQLALCHEMY 2.0 + PYDANTIC V2 ASYNC ENGINE CORE")
    print("="*75)
    
    # Connection string using asyncpg driver for pure async PostgreSQL communication
    DATABASE_URL = "postgresql+asyncpg://postgres:secret_password@localhost:5432/broker_db"
    MOCK_METRICS_URL = "http://185.189.15"
    
    # Triggering the pure SQLAlchemy core worker
    await process_server_monitoring_job(metrics_url=MOCK_METRICS_URL, db_url=DATABASE_URL)

if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
