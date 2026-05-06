import time
import datetime
import requests
import base64
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, Column, String, Integer, DateTime, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import google.generativeai as genai

# --- CONFIGURATION ---
GEMINI_API_KEY = "AIzaSyAM2yk41iAKpl_Bj09-LJWssz44BIkpREo"
MPESA_CONSUMER_KEY = "YLg0zahVAwQFkHuab5atcNySEEt328D2YOB6VNYh8wjWz9uu"
MPESA_CONSUMER_SECRET = "wXGnKVWBDKL5DKmTfsWNPxp4JtWGSdO8inVDDAJRTORvYgrcA1Hkae5AOJN11DMK"
MPESA_SHORTCODE = "174379"  # Sandbox Shortcode
MPESA_PASSKEY = "bfb279f9aa9bdbcf158e97dd71a467cd2e0c893059b10f78e6b72ada1ed2c919"
CALLBACK_URL = "https://marvel-network.onrender.com"

genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-1.5-flash')

# --- DATABASE SETUP ---
DATABASE_URL = "sqlite:///./marvel_network.db"
Base = declarative_base()

class UserSession(Base):
    __tablename__ = "sessions"
    id = Column(Integer, primary_key=True, index=True)
    mac_address = Column(String, unique=True, index=True)
    phone_number = Column(String)
    expiry_timestamp = Column(Float)
    checkout_id = Column(String, unique=True)
    status = Column(String, default="pending") # pending, paid, failed

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base.metadata.create_all(bind=engine)

app = FastAPI()

# Enable CORS for your frontend
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# --- UTILS ---
def get_db():
    db = SessionLocal()
    try: yield db
    finally: db.close()

def get_mpesa_token():
    auth_url = "https://sandbox.safaricom.co.ke/oauth/v1/generate?grant_type=client_credentials"
    res = requests.get(auth_url, auth=(MPESA_CONSUMER_KEY, MPESA_CONSUMER_SECRET))
    return res.json()['access_token']

# --- API ENDPOINTS ---

@app.post("/api/stk-push")
async def stk_push(data: dict, db: Session = Depends(get_db)):
    phone = data['phone']
    amount = data['amount']
    # Format phone: 0712... -> 254712...
    if phone.startswith('0'): phone = '254' + phone[1:]
    
    token = get_mpesa_token()
    timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    password = base64.b64encode(f"{MPESA_SHORTCODE}{MPESA_PASSKEY}{timestamp}".encode()).decode()
    
    payload = {
        "BusinessShortCode": MPESA_SHORTCODE,
        "Password": password,
        "Timestamp": timestamp,
        "TransactionType": "CustomerPayBillOnline",
        "Amount": amount,
        "PartyA": phone,
        "PartyB": MPESA_SHORTCODE,
        "PhoneNumber": phone,
        "CallBackURL": CALLBACK_URL,
        "AccountReference": "MarvelNetwork",
        "TransactionDesc": f"Internet {data['hours']}hrs"
    }
    
    res = requests.post("https://sandbox.safaricom.co.ke/mpesa/stkpush/v1/query", json=payload, headers={"Authorization": f"Bearer {token}"})
    resp_data = res.json()
    
    if "CheckoutRequestID" in resp_data:
        # Create session record
        new_session = UserSession(
            mac_address=f"MAC_{phone}", # In production, extract this from headers
            phone_number=phone,
            checkout_id=resp_data['CheckoutRequestID'],
            expiry_timestamp=time.time() + (data['hours'] * 3600)
        )
        db.add(new_session)
        db.commit()
        return resp_data
    
    raise HTTPException(status_code=400, detail="STK Push failed")

@app.get("/api/query-payment")
async def query_payment(id: str, db: Session = Depends(get_db)):
    session = db.query(UserSession).filter(UserSession.checkout_id == id).first()
    if not session: return {"status": "not_found"}
    return {"status": session.status}

@app.get("/api/session-status")
async def session_status(request: Request, db: Session = Depends(get_db)):
    # Logic to identify user (usually IP/MAC passed by Router)
    user_ip = request.client.host
    session = db.query(UserSession).filter(UserSession.status == "paid").first() 
    
    if session and session.expiry_timestamp > time.time():
        return {"active": True, "expiryTimestamp": session.expiry_timestamp * 1000}
    return {"active": False}

@app.post("/api/callback")
async def mpesa_callback(data: dict, db: Session = Depends(get_db)):
    stk_body = data['Body']['stkCallback']
    checkout_id = stk_body['CheckoutRequestID']
    result_code = stk_body['ResultCode']
    
    session = db.query(UserSession).filter(UserSession.checkout_id == checkout_id).first()
    if not session: return {"status": "ignored"}

    if result_code == 0:
        # Success - Double Payment Check
        recent = db.query(UserSession).filter(
            UserSession.phone_number == session.phone_number,
            UserSession.status == "paid",
            UserSession.id != session.id
        ).first()
        
        if recent and (time.time() - recent.expiry_timestamp < 300):
            # AGENTIC ACTION: TRIGGER REVERSAL LOGIC HERE
            session.status = "reversed"
        else:
            session.status = "paid"
    else:
        session.status = "failed"
        # ANALYZE ERROR WITH GEMINI
        model.generate_content(f"Analyze M-Pesa Error Code {result_code} for user {session.phone_number}")

    db.commit()
    return {"status": "ok"}
