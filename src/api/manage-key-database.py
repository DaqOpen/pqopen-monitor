# manage_keys.py
import sys
import argparse
import uuid
from datetime import datetime, timezone

try:
    from keydatabase import SessionLocal, ApiKey, create_db_tables
except ImportError:
    print("Error: Can't find keydatabase lib")
    sys.exit(1)

def initialize_db():
    """
    Create database file if not exist
    """
    create_db_tables()
    print("Initialized Database and Tables")

def add_key(owner_name: str, allowed_bucket_st: str, allowed_bucket_lt: str | None, key_value: str | None):
    """
    Adds a new API-Key (generate new UUID4 in case of None or use given key)    
    """    
    if not key_value:
        key_value = str(uuid.uuid4())
        print(f"🔑 generated key: {key_value}")

    db = SessionLocal()
    try:
        # Check if key exists in Database
        existing_key = db.query(ApiKey).filter(ApiKey.key_hash == key_value).first()
        if existing_key:
            print(f"Error: Key '{key_value}' already in use (Owner: {existing_key.owner}).")
            return

        # New Key
        new_key = ApiKey(
            key_hash=key_value,
            owner=owner_name,
            allowed_bucket_st=allowed_bucket_st,
            allowed_bucket_lt=allowed_bucket_lt,
            is_active=True,
            created_at=datetime.now(timezone.utc),
            rate_limit_reseted_at=datetime.now(timezone.utc)
        )
        db.add(new_key)
        db.commit()
        print(f"✅ NEW KEY ADDED:")
        print(f"   Key: {key_value}")
        print(f"   Owner:  {owner_name}")
        print(f"   Bucket_ST:  {allowed_bucket_st}")
        print(f"   Bucket_LT:  {allowed_bucket_lt}")
        print(f"   Active:     Yes")
        
    except Exception as e:
        db.rollback()
        print(f"An Error occured: {e}")
    finally:
        db.close()

def list_keys():
    db = SessionLocal()
    try:
        keys = db.query(ApiKey).all()
        
        if not keys:
            print("No API-Keys found.")
            return

        print("\n--- Overview of Keys ---")
        for key in keys:
            last_used_str = key.last_used.strftime('%Y-%m-%d %H:%M:%S') if key.last_used else "Never"
            print("-" * 50)
            print(f"  Key: {key.key_hash}")
            print(f"  Owner: {key.owner}")
            print(f"  Bucket_ST: {key.allowed_bucket_st}")
            print(f"  Bucket_LT: {key.allowed_bucket_lt}")
            print(f"  Active:    {'✅ Yes' if key.is_active else '❌ No'}")
            print(f"  Created: {key.created_at.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"  Last used: {last_used_str}")
        print("-" * 50)
        
    finally:
        db.close()

def toggle_key(key_value: str, activate: bool):
    db = SessionLocal()
    try:
        key_record = db.query(ApiKey).filter(ApiKey.key_hash == key_value).first()
        
        if not key_record:
            print(f"Error: Key '{key_value}' not found.")
            return

        action = "activated" if activate else "deactivated"
        key_record.is_active = activate
        db.commit()
        print(f"Key '{key_value}' of '{key_record.owner}' successfully {action}.")
        
    except Exception as e:
        db.rollback()
        print(f"An Error occured: {e}")
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(
        description="CMD Tool for FastAPI API-Keys management in SQLite."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init", help="Init the database tables.")

    parser_add = subparsers.add_parser("add", help="Adds a new key, generate UUID4 if empty")
    parser_add.add_argument("owner", type=str, help="Name of the owner or bucket")
    parser_add.add_argument("allowed_bucket_st", type=str, help="Name of the allowed bucket short term")
    parser_add.add_argument("allowed_bucket_lt", type=str, nargs='?', default=None, help="Name of the allowed bucket long term")
    parser_add.add_argument("key", type=str, nargs='?', default=None, help="Optional: Api-Key will be generated from UUID4 otherwise.")
    
    subparsers.add_parser("list", help="List all Api-Keys.")
    
    parser_deactivate = subparsers.add_parser("deactivate", help="Deactivates an Api-Key.")
    parser_deactivate.add_argument("key", type=str, help="Key to be deactivated.")

    parser_activate = subparsers.add_parser("activate", help="Activates an Api-Key.")
    parser_activate.add_argument("key", type=str, help="Key to be activated.")

    args = parser.parse_args()

    # --- Command worker ---
    if args.command == "init":
        initialize_db()
    elif args.command == "add":
        add_key(args.owner, args.allowed_bucket_st, args.allowed_bucket_lt, args.key) 
    elif args.command == "list":
        list_keys()
    elif args.command == "deactivate":
        toggle_key(args.key, activate=False)
    elif args.command == "activate":
        toggle_key(args.key, activate=True)

if __name__ == "__main__":
    if 'uuid' not in sys.modules:
        import uuid
    main()