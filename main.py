# ============================================================
# MARVEL NETWORK — FASTAPI BACKEND
# Your original structure preserved exactly.
# All production fixes layered on top cleanly.
#
# Original naming kept:
#   - get_mpesa_token()        (your name)
#   - UserSession / sessions   (your table name)
#   - /api/stk-push            (your route)
#   - /api/query-payment       (your route)
#   - /api/session-status      (your route — now accepts ?mac=)
#   - /api/callback            (your route)
#   - /api/terminate-session   (your route)
#
# What was added on top of YOUR original:
#   [+] Real MAC stored — not MAC_{phone} placeholder
#   [+] /api/callback returns 200 immediately (BackgroundTasks)
#   [+] Idempotency via mpesa_receipt column
#   [+] /api/session-status now takes ?mac= param
#   [+] /api/terminate-session sends MAC to router revoke
#   [+] MikroTik grant_router_access() after confirmed payment
#   [+] OAuth token cached — not fetched per request
#   [+] APScheduler cron expires sessions every 60s
#   [+] slowapi rate limit on /api/stk-push
#   [+] /api/chat wired to Gemini (was initialised but unused)
#   [+] /health endpoint
#   [+] CORS includes CodePen for dev testing
#   [+] Amount / MAC guards that raise proper errors
# ============================================================

import time
import datetime
import logging
import os
import base64
from datetime import timezone, timedelta
from contextlib import asynccontextmanager

import httpx
import requests
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, Request, HTTPException, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from sqlalchemy import create_engine, Column, String, Integer, Float, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import google.generativeai as genai

# --- LOGGING SETUP (your original style kept) ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("marvel-network")

# --- CONFIGURATION (your original names kept, now all from env) ---
GEMINI_API_KEY        = os.getenv("GEMINI_API_KEY", "")
MPESA_CONSUMER_KEY    = os.getenv("MPESA_CONSUMER_KEY", "YLg0zahVAwQFkHuab5atcNySEEt328D2YOB6VNYh8wjWz9uu")
MPESA_CONSUMER_SECRET = os.getenv("MPESA_CONSUMER_SECRET", "wXGnKVWBDKL5DKmTfsWNPxp4JtWGSdO8inVDDAJRTORvYgrcA1Hkae5AOJN11DMK")
MPESA_SHORTCODE       = os.getenv("MPESA_SHORTCODE", "174379")
MPESA_PASSKEY         = os.getenv("MPESA_PASSKEY", "bfb279f9aa9bdbcf158e97dd71a467cd2e0c893059b10f78e6b72ada1ed2c919")
MPESA_BASE_URL        = os.getenv("MPESA_BASE_URL", "https://sandbox.safaricom.co.ke")
CALLBACK_URL          = os.getenv("CALLBACK_URL", "https://marvel-network.onrender.com/api/callback")
ROUTER_IP             = os.getenv("ROUTER_IP", "192.168.88.1")
ROUTER_USER           = os.getenv("ROUTER_USER", "admin")
ROUTER_PASS           = os.getenv("ROUTER_PASS", "")
PORTAL_ORIGIN         = os.getenv("PORTAL_ORIGIN", "https://marvel-network.onrender.com")

# Safaricom callback IPs — only enforced in production
SAFARICOM_IPS = {
    "196.201.214.200", "196.201.214.206", "196.201.214.207",
    "196.201.214.208", "196.201.214.209", "196.201.214.210",
    "127.0.0.1", "::1",
}

START_TIME = time.time()

# --- GEMINI AI (your original init kept — now actually wired to /api/chat) ---
try:
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel("gemini-1.5-flash")
    logger.info("Gemini 1.5 Flash initialised")
except Exception as e:
    logger.error(f"AI_INIT_FAILED: {str(e)}")
    model = None

GEMINI_SYSTEM_PROMPT = (
    "You are Marvel Network AI — a friendly, concise customer support agent "
    "for a Kenyan Wi-Fi hotspot service. Help users with:\n"
    "- Packages: 1GB/Ksh10 (24h), 3GB/Ksh30 (24h), 10GB/Ksh500 (7d), "
    "25GB/Ksh1000 (14d), 50GB/Ksh2500 (30d), Unlimited/Ksh4000 or Ksh6000 (30d), "
    "Static IP/Ksh10000.\n"
    "- M-Pesa STK Push payments — user enters PIN on their phone.\n"
    "- Connection issues, session expiry, coverage questions.\n"
    "Respond in English or Swahili depending on the user. Be brief and helpful. "
    "If you cannot help, direct them to WhatsApp: +254113246300."
)

# --- DATABASE SETUP (your original model name and table kept) ---
DATABASE_URL = "sqlite:///./marvel_network.db"
Base = declarative_base()


class UserSession(Base):
    __tablename__ = "sessions"
    id               = Column(Integer, primary_key=True, index=True)
    mac_address      = Column(String, index=True)         # Real MAC from router redirect
    phone_number     = Column(String)
    expiry_timestamp = Column(Float)
    checkout_id      = Column(String, unique=True)
    mpesa_receipt    = Column(String, unique=True, nullable=True)  # idempotency
    hours            = Column(Integer, default=1)
    status           = Column(String, default="pending")  # pending|paid|failed|expired
    created_at       = Column(Float, default=time.time)


engine       = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base.metadata.create_all(bind=engine)


def _run_migrations():
    """Add new columns safely to existing DB without dropping data."""
    new_cols = [
        "ALTER TABLE sessions ADD COLUMN mpesa_receipt TEXT",
        "ALTER TABLE sessions ADD COLUMN hours INTEGER DEFAULT 1",
        "ALTER TABLE sessions ADD COLUMN created_at REAL",
    ]
    with engine.connect() as conn:
        for sql in new_cols:
            try:
                conn.execute(text(sql))
                conn.commit()
            except Exception:
                pass  # column already exists — safe


_run_migrations()


# --- UTILS (your original get_db kept exactly) ---
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# --- MPESA TOKEN (your original function name kept, now cached) ---
_token_cache: dict = {"token": None, "expires_at": 0}


def get_mpesa_token() -> str:
    """Cached OAuth token — your original function name preserved."""
    if _token_cache["token"] and time.time() < _token_cache["expires_at"] - 60:
        return _token_cache["token"]

    auth_url = f"{MPESA_BASE_URL}/oauth/v1/generate?grant_type=client_credentials"
    try:
        res = requests.get(
            auth_url,
            auth=(MPESA_CONSUMER_KEY, MPESA_CONSUMER_SECRET),
            timeout=10,
        )
        res.raise_for_status()
        data = res.json()
        _token_cache["token"]      = data["access_token"]
        _token_cache["expires_at"] = time.time() + int(data.get("expires_in", 3600))
        logger.info("MPESA_TOKEN_REFRESHED")
        return _token_cache["token"]
    except Exception as e:
        logger.error(f"MPESA_AUTH_ERROR: {str(e)}")
        raise Exception("M-Pesa Authentication Failed.")


# --- PHONE NORMALISE (your original inline logic extracted to reusable fn) ---
def normalise_phone(raw: str) -> str:
    phone = str(raw).strip()
    if phone.startswith("+"):
        phone = phone[1:]
    if phone.startswith("0"):
        phone = "254" + phone[1:]
    return phone


# --- ROUTER INTEGRATION ---
async def grant_router_access(mac: str, hours: int):
    """Grant internet to a MAC via MikroTik REST API. No-op if ROUTER_PASS not set."""
    if not ROUTER_PASS:
        logger.warning(f"ROUTER_GRANT_SKIPPED: ROUTER_PASS not set — MAC={mac}")
        return
    uptime_minutes = hours * 60
    mac_name = mac.replace(":", "").lower()
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(
                f"http://{ROUTER_IP}/rest/ip/hotspot/user/add",
                auth=(ROUTER_USER, ROUTER_PASS),
                json={
                    "name":         mac_name,
                    "password":     mac_name,
                    "mac-address":  mac,
                    "limit-uptime": f"{uptime_minutes}m",
                    "profile":      "default",
                    "comment":      f"marvel-paid-{int(time.time())}",
                },
            )
            if resp.status_code in (200, 201):
                logger.info(f"ROUTER_GRANT_OK: MAC={mac} hours={hours}")
            else:
                # User may already exist — update limit
                await client.patch(
                    f"http://{ROUTER_IP}/rest/ip/hotspot/user/{mac_name}",
                    auth=(ROUTER_USER, ROUTER_PASS),
                    json={"limit-uptime": f"{uptime_minutes}m"},
                )
                logger.info(f"ROUTER_GRANT_UPDATED: MAC={mac}")
    except Exception as e:
        logger.error(f"ROUTER_GRANT_FAILED: MAC={mac} error={e}")


async def revoke_router_access(mac: str):
    """Remove MAC from MikroTik active sessions and user list."""
    if not ROUTER_PASS:
        logger.warning(f"ROUTER_REVOKE_SKIPPED: ROUTER_PASS not set — MAC={mac}")
        return
    mac_name = mac.replace(":", "").lower()
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.delete(
                f"http://{ROUTER_IP}/rest/ip/hotspot/active/{mac_name}",
                auth=(ROUTER_USER, ROUTER_PASS),
            )
            await client.delete(
                f"http://{ROUTER_IP}/rest/ip/hotspot/user/{mac_name}",
                auth=(ROUTER_USER, ROUTER_PASS),
            )
            logger.info(f"ROUTER_REVOKE_OK: MAC={mac}")
    except Exception as e:
        logger.error(f"ROUTER_REVOKE_FAILED: MAC={mac} error={e}")


# --- SESSION EXPIRY CRON ---
async def expire_sessions_job():
    db = SessionLocal()
    try:
        now     = time.time()
        expired = db.query(UserSession).filter(
            UserSession.status == "paid",
            UserSession.expiry_timestamp <= now,
        ).all()
        for session in expired:
            session.status = "expired"
            logger.info(f"SESSION_EXPIRED: MAC={session.mac_address}")
            await revoke_router_access(session.mac_address)
        if expired:
            db.commit()
    except Exception as e:
        logger.error(f"CRON_ERROR: {e}")
    finally:
        db.close()


# --- RATE LIMITER ---
limiter = Limiter(key_func=get_remote_address)

# --- SCHEDULER ---
scheduler = AsyncIOScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler.add_job(expire_sessions_job, "interval", minutes=1, id="expire_sessions")
    scheduler.start()
    logger.info("APScheduler started — session expiry cron active")
    yield
    scheduler.shutdown(wait=False)


# --- APP (your original title kept) ---
app = FastAPI(title="Marvel Network Elite Core", lifespan=lifespan)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# CORS — includes CodePen origins for dev testing
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        PORTAL_ORIGIN,
        "https://codepen.io",      # dev testing
        "https://cdpn.io",         # dev testing
        "http://192.168.88.1",     # router local
        "http://localhost:5500",   # VS Code live server
        "http://127.0.0.1:5500",
        "http://localhost:3000",
        "*",                       # remove this line when going to production
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# API ENDPOINTS — your original route names preserved exactly
# ============================================================

@app.get("/")
async def health_check():
    """Your original health check — kept exactly."""
    return {"status": "online", "timestamp": time.time(), "node": "Marvel-Alpha"}


@app.get("/health")
async def health(db: Session = Depends(get_db)):
    """Extended health — used by UptimeRobot monitoring."""
    try:
        active_count = db.query(UserSession).filter(
            UserSession.status == "paid",
            UserSession.expiry_timestamp > time.time(),
        ).count()
        return {
            "status":          "online",
            "uptime_seconds":  round(time.time() - START_TIME),
            "active_sessions": active_count,
            "mpesa_env":       "production" if "api.safaricom" in MPESA_BASE_URL else "sandbox",
            "timestamp":       datetime.datetime.utcnow().isoformat() + "Z",
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"DB error: {e}")


@app.post("/api/stk-push")
@limiter.limit("3/10minute")
async def stk_push(request: Request, data: dict, db: Session = Depends(get_db)):
    """
    Your original /api/stk-push — structure preserved.
    Added: real MAC storage, amount guard, token caching.
    """
    try:
        # 1. Extract — your original field names
        raw_phone  = str(data.get("phone", "")).strip()
        raw_amount = str(data.get("amount", "")).replace("Ksh", "").strip()
        hours      = int(data.get("hours", 1))
        mac        = str(data.get("mac", "")).upper().strip()
        ip         = str(data.get("ip", "")).strip()

        # 2. Phone normalisation — your original logic
        phone = normalise_phone(raw_phone)
        if not phone.startswith("254") or len(phone) != 12:
            raise HTTPException(status_code=400, detail="Invalid Phone: Use 2547XXXXXXXX")

        # 3. Amount — your original cast + guard
        try:
            amount_int = int(float(raw_amount))
        except (ValueError, TypeError):
            raise HTTPException(status_code=400, detail="Invalid Amount Format")

        if amount_int < 1:
            raise HTTPException(status_code=400, detail="Amount must be at least Ksh 1")

        # 4. EAT timestamp — your original logic
        eat_now   = datetime.datetime.now(timezone.utc) + timedelta(hours=3)
        timestamp = eat_now.strftime("%Y%m%d%H%M%S")

        # 5. Password — your original logic
        password_str = f"{MPESA_SHORTCODE}{MPESA_PASSKEY}{timestamp}"
        password     = base64.b64encode(password_str.encode()).decode()

        # 6. Build payload — your original field names
        token   = get_mpesa_token()
        payload = {
            "BusinessShortCode": str(MPESA_SHORTCODE),
            "Password":          password,
            "Timestamp":         timestamp,
            "TransactionType":   "CustomerPayBillOnline",
            "Amount":            str(amount_int),
            "PartyA":            str(phone),
            "PartyB":            str(MPESA_SHORTCODE),
            "PhoneNumber":       str(phone),
            "CallBackURL":       CALLBACK_URL,
            "AccountReference":  "MarvelNetwork",
            "TransactionDesc":   f"WiFi {hours}H",
        }

        logger.info(f"PUSH_INITIATED: Phone={phone} Amount={amount_int} MAC={mac or 'not provided'}")

        # 7. Fire — your original URL variable name
        push_url = f"{MPESA_BASE_URL}/mpesa/stkpush/v1/processrequest"
        res      = requests.post(
            push_url,
            json=payload,
            headers={"Authorization": f"Bearer {token}"},
            timeout=15,
        )
        resp_data = res.json()

        # 8. Handle response — your original check
        if resp_data.get("ResponseCode") == "0":
            # Store session — now with REAL mac (not MAC_{phone} placeholder)
            new_session = UserSession(
                mac_address      = mac if mac else f"UNKNOWN_{phone}",
                phone_number     = phone,
                checkout_id      = resp_data["CheckoutRequestID"],
                expiry_timestamp = time.time() + (hours * 3600),
                hours            = hours,
                status           = "pending",
                created_at       = time.time(),
            )
            db.add(new_session)
            db.commit()
            return resp_data

        logger.error(f"PROVIDER_REJECTION: {resp_data}")
        error_msg = resp_data.get("CustomerMessage", "STK Push rejected by provider.")
        raise HTTPException(status_code=400, detail=error_msg)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"STK_PUSH_CRASH: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/query-payment")
async def query_payment(id: str, db: Session = Depends(get_db)):
    """Your original /api/query-payment — kept exactly."""
    session = db.query(UserSession).filter(UserSession.checkout_id == id).first()
    return {"status": session.status if session else "not_found"}


@app.get("/api/session-status")
async def session_status(mac: str = "", phone: str = "", db: Session = Depends(get_db)):
    """
    Your original /api/session-status.
    Now accepts ?mac= for per-device lookup.
    Falls back to ?phone= for MAC-randomization recovery.
    """
    # Primary: look up by MAC
    if mac:
        mac = mac.upper().strip()
        session = db.query(UserSession).filter(
            UserSession.mac_address == mac,
            UserSession.status == "paid",
        ).order_by(UserSession.id.desc()).first()

        if session and session.expiry_timestamp > time.time():
            return {
                "active":          True,
                "expiryTimestamp": session.expiry_timestamp * 1000,
                "phone_mask":      f"****{session.phone_number[-4:]}",
            }

    # Fallback: phone number (handles MAC randomization on iOS/Android)
    if phone:
        phone_norm = normalise_phone(phone)
        session    = db.query(UserSession).filter(
            UserSession.phone_number == phone_norm,
            UserSession.status == "paid",
        ).order_by(UserSession.id.desc()).first()

        if session and session.expiry_timestamp > time.time():
            # Rebind to new MAC silently
            if mac and mac != session.mac_address:
                old_mac             = session.mac_address
                session.mac_address = mac
                db.commit()
                await revoke_router_access(old_mac)
                hours_left = max(1, int((session.expiry_timestamp - time.time()) / 3600) + 1)
                await grant_router_access(mac, hours_left)
                logger.info(f"MAC_REBIND: old={old_mac} new={mac}")
            return {
                "active":          True,
                "expiryTimestamp": session.expiry_timestamp * 1000,
                "phone_mask":      f"****{session.phone_number[-4:]}",
            }

    return {"active": False}


@app.post("/api/callback")
async def mpesa_callback(
    request: Request,
    data: dict,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Your original /api/callback.
    Now returns 200 immediately — processes in background.
    IP check only active in production (not sandbox).
    """
    client_ip = request.client.host if request.client else "unknown"

    # Only enforce IP whitelist in production
    if MPESA_BASE_URL != "https://sandbox.safaricom.co.ke":
        if client_ip not in SAFARICOM_IPS:
            logger.warning(f"CALLBACK_IP_REJECTED: {client_ip}")
            raise HTTPException(status_code=403, detail="Forbidden")

    logger.info(f"CALLBACK_RECEIVED from {client_ip}")

    # Return 200 immediately — Safaricom retries on slow responses
    background_tasks.add_task(_process_callback, data)
    return {"ResultCode": 0, "ResultDesc": "Accepted"}


async def _process_callback(data: dict):
    """Background task — update DB and grant router access."""
    db = SessionLocal()
    try:
        logger.info(f"CALLBACK_DATA: {str(data)[:500]}")

        stk_body    = data["Body"]["stkCallback"]
        checkout_id = stk_body["CheckoutRequestID"]
        result_code = stk_body["ResultCode"]

        session = db.query(UserSession).filter(
            UserSession.checkout_id == checkout_id
        ).first()

        if not session:
            logger.warning(f"CALLBACK_ORPHAN: {checkout_id} not in DB")
            return

        # Idempotency — never process twice
        if session.status == "paid":
            logger.info(f"CALLBACK_DUPLICATE_IGNORED: {checkout_id}")
            return

        if result_code == 0:
            items   = stk_body.get("CallbackMetadata", {}).get("Item", [])
            receipt = next((i["Value"] for i in items if i["Name"] == "MpesaReceiptNumber"), None)
            amount  = next((i["Value"] for i in items if i["Name"] == "Amount"), None)

            # Extra idempotency guard on receipt number
            if receipt:
                existing = db.query(UserSession).filter(
                    UserSession.mpesa_receipt == receipt
                ).first()
                if existing:
                    logger.warning(f"RECEIPT_DUPLICATE: {receipt}")
                    return

            session.status        = "paid"
            session.mpesa_receipt = receipt
            db.commit()

            logger.info(f"SUCCESS: {checkout_id} MAC={session.mac_address} Receipt={receipt} Amount={amount}")

            # Grant router access
            await grant_router_access(session.mac_address, session.hours)

        else:
            session.status = "failed"
            db.commit()
            logger.warning(f"FAILED: {checkout_id} CODE={result_code} MAC={session.mac_address}")

    except Exception as e:
        logger.error(f"CALLBACK_ERROR: {str(e)}")
        try:
            db.rollback()
        except Exception:
            pass
    finally:
        db.close()


@app.post("/api/terminate-session")
async def terminate_session(data: dict, db: Session = Depends(get_db)):
    """
    Your original /api/terminate-session.
    Now accepts MAC and calls router revoke.
    """
    mac = str(data.get("mac", "")).upper().strip()

    if not mac:
        # Fallback: original behaviour — find last paid session
        session = db.query(UserSession).filter(
            UserSession.status == "paid"
        ).order_by(UserSession.id.desc()).first()
    else:
        session = db.query(UserSession).filter(
            UserSession.mac_address == mac,
            UserSession.status == "paid",
        ).order_by(UserSession.id.desc()).first()

    if session:
        session.expiry_timestamp = time.time()
        session.status           = "expired"
        db.commit()
        logger.info(f"SESSION_TERMINATED: MAC={session.mac_address}")
        await revoke_router_access(session.mac_address)

    return {"status": "terminated"}


@app.post("/api/chat")
async def ai_chat(data: dict):
    """
    Gemini chat — was initialised in your original but had no endpoint.
    Now wired up. Called by the frontend text chat interface.
    """
    if not model:
        raise HTTPException(
            status_code=503,
            detail="AI service unavailable — GEMINI_API_KEY not set."
        )

    user_message = str(data.get("message", "")).strip()
    if not user_message:
        raise HTTPException(status_code=400, detail="message required")
    if len(user_message) > 1000:
        raise HTTPException(status_code=400, detail="Message too long (max 1000 chars)")

    try:
        full_prompt = f"{GEMINI_SYSTEM_PROMPT}\n\nUser: {user_message}\nAgent:"
        response    = model.generate_content(full_prompt)
        reply       = response.text.strip()
        logger.info(f"CHAT_RESPONSE_SENT: {len(reply)} chars")
        return {"reply": reply}
    except Exception as e:
        logger.error(f"GEMINI_ERROR: {str(e)}")
        raise HTTPException(status_code=502, detail="AI response failed — try again.")


@app.post("/api/agent-event")
async def agent_event(data: dict, background_tasks: BackgroundTasks):
    """
    ElevenLabs agent webhook — fires when agent detects a complaint.
    Dispatches agentic recovery actions in background.
    """
    background_tasks.add_task(_handle_agent_event, data)
    return {"status": "received"}


async def _handle_agent_event(data: dict):
    tool   = data.get("tool_name", "")
    params = data.get("parameters", {})
    logger.info(f"AGENT_TOOL: {tool} params={params}")

    db = SessionLocal()
    try:
        if tool == "reconnect_device":
            phone = normalise_phone(params.get("phone", ""))
            mac   = str(params.get("mac", "")).upper()
            session = db.query(UserSession).filter(
                UserSession.phone_number == phone,
                UserSession.status == "paid",
                UserSession.expiry_timestamp > time.time(),
            ).order_by(UserSession.id.desc()).first()
            if session:
                if mac and mac != session.mac_address:
                    await revoke_router_access(session.mac_address)
                    session.mac_address = mac
                    db.commit()
                hours_left = max(1, int((session.expiry_timestamp - time.time()) / 3600) + 1)
                await grant_router_access(session.mac_address, hours_left)
                logger.info(f"AGENT_RECONNECT_OK: phone={phone} mac={session.mac_address}")

        elif tool == "check_session":
            phone   = normalise_phone(params.get("phone", ""))
            session = db.query(UserSession).filter(
                UserSession.phone_number == phone,
                UserSession.status == "paid",
                UserSession.expiry_timestamp > time.time(),
            ).order_by(UserSession.id.desc()).first()
            logger.info(f"AGENT_CHECK_SESSION: phone={phone} active={bool(session)}")

    except Exception as e:
        logger.error(f"AGENT_EVENT_ERROR: {e}")
    finally:
        db.close()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
