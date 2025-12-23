from sqlalchemy import create_engine, Column, String, Boolean, Integer, DateTime, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# Database connection string: local sqlite file
SQLALCHEMY_DATABASE_URL = "sqlite:///./config/api_keys.db"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

# Database Model
class ApiKey(Base):
    __tablename__ = "api_keys"

    key_hash = Column(String, primary_key=True, index=True) 
    owner = Column(String, index=True)
    allowed_bucket_st = Column(String)
    allowed_bucket_lt = Column(String)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=func.now())
    last_used = Column(DateTime, nullable=True)
    rate_limit_counter = Column(Integer, default=0)
    rate_limit_reseted_at = Column(DateTime, nullable=True)

def create_db_tables():
    Base.metadata.create_all(bind=engine)