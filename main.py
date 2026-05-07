import time
import datetime
from datetime import timezone, timedelta
import requests
import base64
import os
import logging
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, Column, String, Integer, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import google.generativeai as genai

# --- LOGGING SETUP ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("marvel-network")

# --- CONFIGURATION ---
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "AIzaSyAM2yk41iAKpl_Bj09-LJWssz44BIkpREo")
MPESA_CONSUMER_KEY = os.getenv("MPESA_CONSUMER_KEY", "YLg0zahVAwQFkHuab5atcNySEEt328D2YOB6VNYh8wjWz9uu")
MPESA_CONSUMER_SECRET = os.getenv("MPESA_CONSUMER_SECRET", "wXGnKVWBDKL5DKmTfsWNPxp4JtWGSdO8inVDDAJRTORvYgrcA1Hkae5AOJN11DMK")
MPESA_SHORTCODE = "174379" 
MPESA_PASSKEY = "bfb279f9aa9bdbcf158e97dd71a467cd2e0c893059b10f78e6b72ada1ed2c919"
CALLBACK_URL = "https://marvel-network.onrender.com/api/callback"

# Initialize AI
try:
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel('gemini-1.5-flash')
except Exception as e:
    logger.error(f"AI_INIT_FAILED: {str(e)}")

# --- DATABASE SETUP ---
DATABASE_URL = "sqlite:///./marvel_network.db"
Base = declarative_base()

class UserSession(Base):
    __tablename__ = "sessions"
    id = Column(Integer, primary_key=True, index=True)
    mac_address = Column(String, index=True)
    phone_number = Column(String)
    expiry_timestamp = Column(Float)
    checkout_id = Column(String, unique=True)
    status = Column(String, default="pending") 

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base.metadata.create_all(bind=engine)

app = FastAPI(title="Marvel Network Elite Core")

# --- CORS POLICY ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- UTILS ---
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_mpesa_token():
    auth_url = "https://sandbox.safaricom.co.ke/oauth/v1/generate?grant_type=client_credentials"
    try:
        res = requests.get(auth_url, auth=(MPESA_CONSUMER_KEY, MPESA_CONSUMER_SECRET), timeout=10)
        res.raise_for_status()
        return res.json()['access_token']
    except Exception as e:
        logger.error(f"MPESA_AUTH_ERROR: {str(e)}")
        raise Exception("M-Pesa Authentication Failed. Check Consumer Keys.")

# --- API ENDPOINTS ---

@app.get("/")
async def health_check():
    return {"status": "online", "timestamp": time.time(), "node": "Marvel-Alpha"}

@app.post("/api/stk-push")
async def stk_push(data: dict, db: Session = Depends(get_db)):
    try:
        # 1. Precise Data Extraction & Cleaning
        raw_phone = str(data.get('phone', '')).strip()
        raw_amount = str(data.get('amount', '')).replace('Ksh', '').strip()
        hours = int(data.get('hours', 1))

        # 2. Strict Phone Canonicalization
        phone = raw_phone
        if phone.startswith('0'):
            phone = '254' + phone[1:]
        elif phone.startswith('+'):
            phone = phone[1:]
        
        if not phone.startswith('254') or len(phone) != 12:
            raise HTTPException(status_code=400, detail="Invalid Phone: Use 2547XXXXXXXX")

        # 3. Secure Amount Casting
        try:
            amount_int = int(float(raw_amount))
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid Amount Format")

        # 4. Sync Timezone with EAT (Kenya Time)
        eat_now = datetime.datetime.now(timezone.utc) + timedelta(hours=3)
        timestamp = eat_now.strftime('%Y%m%d%H%M%S')
        
        # 5. Generate Password
        password_str = f"{MPESA_SHORTCODE}{MPESA_PASSKEY}{timestamp}"
        password = base64.b64encode(password_str.encode()).decode()

        # 6. Build Payload (Numeric values as Strings for Sandbox stability)
        token = get_mpesa_token()
        payload = {
            "BusinessShortCode": str(MPESA_SHORTCODE),
            "Password": password,
            "Timestamp": timestamp,
            "TransactionType": "CustomerPayBillOnline",
            "Amount": amount_int,
            "PartyA": str(phone),
            "PartyB": str(MPESA_SHORTCODE),
            "PhoneNumber": str(phone),
            "CallBackURL": CALLBACK_URL,
            "AccountReference": "MarvelNetwork",
            "TransactionDesc": f"WiFi {hours}H"
        }
        
        logger.info(f"PUSH_INITIATED: Phone={phone} Amount={amount_int}")
        
        # URL normalized with trailing slash to prevent 404/Redirect issues
        push_url = "https://sandbox.safaricom.co.ke/mpesa/stkpush/v1/processrequest"
        res = requests.post(push_url, json=payload, headers={"Authorization": f"Bearer {token}"}, timeout=15)
        resp_data = res.json()
        
        # 7. Handle Provider Response
        if resp_data.get("ResponseCode") == "0":
            new_session = UserSession(
                mac_address=f"MAC_{phone}", 
                phone_number=phone,
                checkout_id=resp_data['CheckoutRequestID'],
                expiry_timestamp=time.time() + (hours * 3600)
            )
            db.add(new_session)
            db.commit()
            return resp_data
        
        logger.error(f"PROVIDER_REJECTION: {resp_data}")
        error_msg = resp_data.get("CustomerMessage", "STK Push rejected by provider.")
        raise HTTPException(status_code=400, detail=error_msg)
    
except HTTPException:
    raise  # Let FastAPI handle it cleanly
except Exception as e:
    logger.error(f"STK_PUSH_CRASH: {str(e)}")
    raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/query-payment")
async def query_payment(id: str, db: Session = Depends(get_db)):
    session = db.query(UserSession).filter(UserSession.checkout_id == id).first()
    return {"status": session.status if session else "not_found"}

@app.get("/api/session-status")
async def session_status(db: Session = Depends(get_db)):
    session = db.query(UserSession).filter(UserSession.status == "paid").order_by(UserSession.id.desc()).first() 
    if session and session.expiry_timestamp > time.time():
        return {
            "active": True, 
            "expiryTimestamp": session.expiry_timestamp * 1000,
            "phone_mask": f"****{session.phone_number[-4:]}"
        }
    return {"active": False}

@app.post("/api/callback")
async def mpesa_callback(data: dict, db: Session = Depends(get_db)):
    logger.info(f"CALLBACK_DATA: {data}")
    try:
        stk_body = data['Body']['stkCallback']
        checkout_id = stk_body['CheckoutRequestID']
        result_code = stk_body['ResultCode']
        
        session = db.query(UserSession).filter(UserSession.checkout_id == checkout_id).first()
        if not session: return {"status": "ignored"}

        if result_code == 0:
            session.status = "paid"
            logger.info(f"SUCCESS: {checkout_id} ACTIVATED")
        else:
            session.status = "failed"
            logger.warning(f"FAILED: {checkout_id} CODE {result_code}")

        db.commit()
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"CALLBACK_ERROR: {str(e)}")
        return {"status": "error"}

@app.post("/api/terminate-session")
async def terminate_session(db: Session = Depends(get_db)):
    session = db.query(UserSession).filter(UserSession.status == "paid").order_by(UserSession.id.desc()).first()
    if session:
        session.expiry_timestamp = time.time()
        db.commit()
    return {"status": "terminated"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
