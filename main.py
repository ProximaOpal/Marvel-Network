import time
import datetime
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
# This ensures all errors show up clearly in the Render "Logs" tab
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("marvel-network")

# --- CONFIGURATION ---
# Using os.getenv with hardcoded fallbacks for development convenience
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "AIzaSyAM2yk41iAKpl_Bj09-LJWssz44BIkpREo")
MPESA_CONSUMER_KEY = os.getenv("MPESA_CONSUMER_KEY", "YLg0zahVAwQFkHuab5atcNySEEt328D2YOB6VNYh8wjWz9uu")
MPESA_CONSUMER_SECRET = os.getenv("MPESA_CONSUMER_SECRET", "wXGnKVWBDKL5DKmTfsWNPxp4JtWGSdO8inVDDAJRTORvYgrcA1Hkae5AOJN11DMK")
MPESA_SHORTCODE = "174379"  # Daraja Sandbox Shortcode
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
    status = Column(String, default="pending") # pending, paid, failed, reversed

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
        raise Exception("Failed to authenticate with M-Pesa gateway.")

# --- API ENDPOINTS ---

@app.get("/")
async def health_check():
    """
    Endpoint for cron-job.org to ping. 
    Prevents Render from sleeping and confirms system health.
    """
    return {
        "status": "online",
        "timestamp": time.time(),
        "node": "Marvel-Network-Alpha",
        "message": "Tactical HUD Systems Operational"
    }

@app.post("/api/stk-push")
async def stk_push(data: dict, db: Session = Depends(get_db)):
    try:
        logger.info(f"PAYMENT_REQUEST_RECEIVED: {data}")
        
        phone = str(data.get('phone', '')).strip()
        amount = str(data.get('amount', '')).replace('Ksh', '').strip()
        hours = int(data.get('hours', 1))
        
        if not phone or not amount:
            raise HTTPException(status_code=400, detail="INVALID_INPUT: Missing phone or amount.")

        # Canonicalize phone number
        if phone.startswith('0'):
            phone = '254' + phone[1:]
        elif phone.startswith('+'):
            phone = phone[1:]

        token = get_mpesa_token()
        timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        password = base64.b64encode(f"{MPESA_SHORTCODE}{MPESA_PASSKEY}{timestamp}".encode()).decode()
        
        payload = {
            "BusinessShortCode": MPESA_SHORTCODE,
            "Password": password,
            "Timestamp": timestamp,
            "TransactionType": "CustomerPayBillOnline",
            "Amount": int(float(amount)),
            "PartyA": phone,
            "PartyB": MPESA_SHORTCODE,
            "PhoneNumber": phone,
            "CallBackURL": CALLBACK_URL,
            "AccountReference": "MarvelNetwork",
            "TransactionDesc": f"WiFi Access {hours}hrs"
        }
        
        push_url = "https://sandbox.safaricom.co.ke/mpesa/stkpush/v1/process"
        res = requests.post(push_url, json=payload, headers={"Authorization": f"Bearer {token}"}, timeout=15)
        resp_data = res.json()
        
        if resp_data.get("ResponseCode") == "0":
            new_session = UserSession(
                mac_address=f"MAC_{phone}", 
                phone_number=phone,
                checkout_id=resp_data['CheckoutRequestID'],
                expiry_timestamp=time.time() + (hours * 3600)
            )
            db.add(new_session)
            db.commit()
            logger.info(f"STK_PUSH_SUCCESS: {resp_data['CheckoutRequestID']}")
            return resp_data
        
        logger.warning(f"STK_PUSH_REJECTED: {resp_data}")
        raise HTTPException(status_code=400, detail=resp_data.get("CustomerMessage", "STK Push rejected by provider."))
    
    except Exception as e:
        logger.error(f"STK_PUSH_CRASH: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/query-payment")
async def query_payment(id: str, db: Session = Depends(get_db)):
    session = db.query(UserSession).filter(UserSession.checkout_id == id).first()
    if not session: 
        return {"status": "not_found"}
    return {"status": session.status}

@app.get("/api/session-status")
async def session_status(db: Session = Depends(get_db)):
    # Fetch the most recent active session
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
    logger.info(f"CALLBACK_RECEIVED: {data}")
    try:
        stk_body = data['Body']['stkCallback']
        checkout_id = stk_body['CheckoutRequestID']
        result_code = stk_body['ResultCode']
        
        session = db.query(UserSession).filter(UserSession.checkout_id == checkout_id).first()
        if not session: 
            return {"status": "ignored"}

        if result_code == 0:
            # Check for double payment within 5 mins to prevent accidental double charging
            recent = db.query(UserSession).filter(
                UserSession.phone_number == session.phone_number,
                UserSession.status == "paid",
                UserSession.id != session.id
            ).first()
            
            if recent and (time.time() - recent.expiry_timestamp < 300):
                session.status = "reversed"
                logger.info(f"DUPLICATE_PAYMENT_FLAGGED: {checkout_id}")
            else:
                session.status = "paid"
                logger.info(f"SESSION_ACTIVATED: {checkout_id}")
        else:
            session.status = "failed"
            # Agentic Error Analysis via Gemini
            try:
                error_desc = stk_body.get('ResultDesc', 'Unknown error')
                analysis_prompt = (
                    f"Explain this M-Pesa error to a user: '{error_desc}'. "
                    f"ResultCode: {result_code}. Keep it tactical and short."
                )
                ai_response = model.generate_content(analysis_prompt)
                logger.info(f"GEMINI_ERROR_ANALYSIS: {ai_response.text}")
            except: pass

        db.commit()
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"CALLBACK_PROCESSING_ERROR: {str(e)}")
        return {"status": "error", "message": str(e)}

@app.post("/api/terminate-session")
async def terminate_session(db: Session = Depends(get_db)):
    session = db.query(UserSession).filter(UserSession.status == "paid").order_by(UserSession.id.desc()).first()
    if session:
        session.expiry_timestamp = time.time()
        db.commit()
        logger.info("SESSION_TERMINATED_BY_USER")
    return {"status": "terminated"}

if __name__ == "__main__":
    import uvicorn
    # Local development runner
    uvicorn.run(app, host="0.0.0.0", port=8000)
