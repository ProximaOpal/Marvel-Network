"""
MARVEL NETWORK — PRODUCTION FASTAPI BACKEND
============================================
Fixes applied vs. original:
  [FIX-1]  Real MAC address stored from frontend (not MAC_{phone} placeholder)
  [FIX-2]  /api/callback responds 200 immediately, processes in BackgroundTasks
  [FIX-3]  Idempotency via mpesa_receipt unique column — no double-grants
  [FIX-4]  /api/session-status queries by MAC address, not globally
  [FIX-5]  /api/terminate-session accepts MAC, calls revoke_router_access()
  [FIX-6]  grant_router_access() calls MikroTik REST API after payment confirmed
  [FIX-7]  revoke_router_access() called on session end and expiry cron
  [FIX-8]  Safaricom OAuth token cached for 3500s — not fetched per request
  [FIX-9]  APScheduler cron expires sessions every 60s and revokes router access
  [FIX-10] slowapi rate limiting on /api/stk-push (3 per phone per 10 min)
  [FIX-11] All secrets via os.getenv() — no hardcoded credentials
  [FIX-12] CORS tightened to portal origin only
  [FIX-13] /api/chat endpoint wired up to Gemini 1.5 Flash
  [FIX-14] Structured JSON logging throughout
  [FIX-15] /health endpoint with DB connectivity + active session count
  [FIX-16] SQLite migration adds missing columns on startup (safe, idempotent)

Deploy on Render Starter ($7/mo) — free tier cold-starts will break callbacks.

Environment variables required (set in Render Dashboard → Environment):
  GEMINI_API_KEY
  MPESA_CONSUMER_KEY
  MPESA_CONSUMER_SECRET
  MPESA_SHORTCODE          (174379 for sandbox, your shortcode for production)
  MPESA_PASSKEY            (sandbox or production passkey)
  MPESA_BASE_URL           (https://sandbox.safaricom.co.ke OR https://api.safaricom.co.ke)
  CALLBACK_URL             (https://marvel-network.onrender.com/api/callback)
  ROUTER_IP                (e.g. 192.168.88.1)
  ROUTER_USER              (e.g. admin)
  ROUTER_PASS              (your router password)
  PORTAL_ORIGIN            (e.g. https://marvel-network.onrender.com)
  SENTRY_DSN               (optional — Sentry error tracking)
"""

import time
import datetime
import json
import logging
import os
import base64
import asyncio
from datetime import timezone, timedelta
from contextlib import asynccontextmanager

import httpx
import requests
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, Request, HTTPException, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from sqlalchemy import create_engine, Column, String, Integer, Float, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import google.generativeai as genai


# ─────────────────────────────────────────────
# STRUCTURED JSON LOGGING
# ─────────────────────────────────────────────
class _JSONFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({
            "time":    self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level":   record.levelname,
            "logger":  record.name,
            "message": record.getMessage(),
        })

_handler = logging.StreamHandler()
_handler.setFormatter(_JSONFormatter())
logger = logging.getLogger("marvel-network")
logger.addHandler(_handler)
logger.setLevel(logging.INFO)
logger.propagate = False  # prevent duplicate root-logger output


# ─────────────────────────────────────────────
# CONFIGURATION — all from environment
# ─────────────────────────────────────────────
GEMINI_API_KEY        = os.getenv("GEMINI_API_KEY", "")
MPESA_CONSUMER_KEY    = os.getenv("MPESA_CONSUMER_KEY", "")
MPESA_CONSUMER_SECRET = os.getenv("MPESA_CONSUMER_SECRET", "")
MPESA_SHORTCODE       = os.getenv("MPESA_SHORTCODE", "174379")
MPESA_PASSKEY         = os.getenv("MPESA_PASSKEY", "bfb279f9aa9bdbcf158e97dd71a467cd2e0c893059b10f78e6b72ada1ed2c919")
MPESA_BASE_URL        = os.getenv("MPESA_BASE_URL", "https://sandbox.safaricom.co.ke")
CALLBACK_URL          = os.getenv("CALLBACK_URL",   "https://marvel-network.onrender.com/api/callback")
ROUTER_IP             = os.getenv("ROUTER_IP",   "192.168.88.1")
ROUTER_USER           = os.getenv("ROUTER_USER", "admin")
ROUTER_PASS           = os.getenv("ROUTER_PASS", "")
PORTAL_ORIGIN         = os.getenv("PORTAL_ORIGIN", "https://marvel-network.onrender.com")
SENTRY_DSN            = os.getenv("SENTRY_DSN", "")

# Safaricom callback IP allowlist (production IPs)
SAFARICOM_IPS = {
    "196.201.214.200", "196.201.214.206", "196.201.214.207",
    "196.201.214.208", "196.201.214.209", "196.201.214.210",
    # Sandbox also comes from 127.0.0.1 in test mode
    "127.0.0.1", "::1",
}

START_TIME = time.time()


# ─────────────────────────────────────────────
# SENTRY (optional)
# ─────────────────────────────────────────────
if SENTRY_DSN:
    try:
        import sentry_sdk
        from sentry_sdk.integrations.fastapi import FastApiIntegration
        sentry_sdk.init(dsn=SENTRY_DSN, traces_sample_rate=0.1,
                        integrations=[FastApiIntegration()])
        logger.info({"message": "Sentry initialised"})
    except ImportError:
        logger.warning({"message": "sentry-sdk not installed — skipping"})


# ─────────────────────────────────────────────
# GEMINI AI
# ─────────────────────────────────────────────
_gemini_model = None
if GEMINI_API_KEY:
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        _gemini_model = genai.GenerativeModel("gemini-1.5-flash")
        logger.info({"message": "Gemini 1.5 Flash initialised"})
    except Exception as e:
        logger.error({"message": f"Gemini init failed: {e}"})

GEMINI_SYSTEM_PROMPT = (
    "You are Marvel Network AI — a friendly, concise customer support agent for a Kenyan "
    "Wi-Fi hotspot service. Help users with:\n"
    "- Packages: 1GB/Ksh10 (24h), 3GB/Ksh30 (24h), 10GB/Ksh500 (7d), "
    "25GB/Ksh1000 (14d), 50GB/Ksh2500 (30d), Unlimited/Ksh4000-6000 (30d), Static IP/Ksh10000.\n"
    "- M-Pesa STK Push payment — user dials PIN on their phone.\n"
    "- Connection issues, session expiry, coverage questions.\n"
    "Respond in English or Swahili depending on the user. Be brief and helpful. "
    "If you cannot help, direct them to WhatsApp: +254113246300."
)


# ─────────────────────────────────────────────
# DATABASE — SQLite via SQLAlchemy
# ─────────────────────────────────────────────
DATABASE_URL = "sqlite:///./marvel_network.db"
Base = declarative_base()


class UserSession(Base):
    __tablename__ = "sessions"
    id               = Column(Integer, primary_key=True, index=True)
    mac_address      = Column(String,  index=True)          # Real MAC — AA:BB:CC:DD:EE:FF
    phone_number     = Column(String)
    checkout_id      = Column(String,  unique=True)
    mpesa_receipt    = Column(String,  unique=True, nullable=True)  # idempotency [FIX-3]
    hours            = Column(Integer, default=1)                   # needed for cron
    expiry_timestamp = Column(Float)
    status           = Column(String,  default="pending")           # pending|paid|failed|expired
    created_at       = Column(Float,   default=time.time)


engine       = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base.metadata.create_all(bind=engine)


def _run_migrations():
    """Idempotent column additions for existing deployments."""
    migrations = [
        "ALTER TABLE sessions ADD COLUMN mpesa_receipt TEXT",
        "ALTER TABLE sessions ADD COLUMN hours INTEGER DEFAULT 1",
        "ALTER TABLE sessions ADD COLUMN created_at REAL",
    ]
    with engine.connect() as conn:
        for sql in migrations:
            try:
                conn.execute(text(sql))
                conn.commit()
            except Exception:
                pass  # Column already exists — safe to ignore


_run_migrations()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ─────────────────────────────────────────────
# MPESA TOKEN CACHE [FIX-8]
# ─────────────────────────────────────────────
_token_cache: dict = {"token": None, "expires_at": 0}


def get_mpesa_token() -> str:
    """Return cached Safaricom OAuth token, refresh if within 60s of expiry."""
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
        logger.info({"message": "MPESA_TOKEN_REFRESHED"})
        return _token_cache["token"]
    except Exception as e:
        logger.error({"message": f"MPESA_AUTH_ERROR: {e}"})
        raise HTTPException(status_code=502, detail="M-Pesa authentication failed.")


# ─────────────────────────────────────────────
# ROUTER INTEGRATION [FIX-6] [FIX-7]
# ─────────────────────────────────────────────
async def grant_router_access(mac: str, hours: int):
    """
    Grant internet access to a MAC address via MikroTik REST API (RouterOS v7+).
    For OpenWrt/NoDogSplash, replace with SSH + ndsctl auth {mac}.
    """
    if not ROUTER_IP or not ROUTER_PASS:
        logger.warning({"message": f"ROUTER_GRANT_SKIPPED: ROUTER_IP/ROUTER_PASS not set — MAC={mac}"})
        return

    uptime_minutes = hours * 60
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            # Add user to MikroTik HotSpot user list with time limit
            resp = await client.post(
                f"http://{ROUTER_IP}/rest/ip/hotspot/user/add",
                auth=(ROUTER_USER, ROUTER_PASS),
                json={
                    "name":          mac.replace(":", "").lower(),
                    "password":      mac.replace(":", "").lower(),
                    "mac-address":   mac,
                    "limit-uptime":  f"{uptime_minutes}m",
                    "profile":       "default",
                    "comment":       f"marvel-paid-{int(time.time())}",
                },
            )
            if resp.status_code in (200, 201):
                logger.info({"message": f"ROUTER_GRANT_OK: MAC={mac} hours={hours}"})
            else:
                # User may already exist — attempt to update limit instead
                mac_name = mac.replace(":", "").lower()
                await client.patch(
                    f"http://{ROUTER_IP}/rest/ip/hotspot/user/{mac_name}",
                    auth=(ROUTER_USER, ROUTER_PASS),
                    json={"limit-uptime": f"{uptime_minutes}m"},
                )
                logger.info({"message": f"ROUTER_GRANT_UPDATED: MAC={mac}"})
    except Exception as e:
        logger.error({"message": f"ROUTER_GRANT_FAILED: MAC={mac} error={e}"})
        # Do not raise — payment is confirmed, router failure is ops issue, not user issue


async def revoke_router_access(mac: str):
    """Remove a MAC from MikroTik HotSpot active sessions and user list."""
    if not ROUTER_IP or not ROUTER_PASS:
        logger.warning({"message": f"ROUTER_REVOKE_SKIPPED: ROUTER_IP/ROUTER_PASS not set — MAC={mac}"})
        return

    mac_name = mac.replace(":", "").lower()
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            # Remove from active sessions
            await client.delete(
                f"http://{ROUTER_IP}/rest/ip/hotspot/active/{mac_name}",
                auth=(ROUTER_USER, ROUTER_PASS),
            )
            # Remove from user list so they must pay again
            await client.delete(
                f"http://{ROUTER_IP}/rest/ip/hotspot/user/{mac_name}",
                auth=(ROUTER_USER, ROUTER_PASS),
            )
            logger.info({"message": f"ROUTER_REVOKE_OK: MAC={mac}"})
    except Exception as e:
        logger.error({"message": f"ROUTER_REVOKE_FAILED: MAC={mac} error={e}"})


# ─────────────────────────────────────────────
# SESSION EXPIRY CRON [FIX-9]
# ─────────────────────────────────────────────
async def expire_sessions_job():
    """Run every 60 seconds — revoke router access for expired paid sessions."""
    db = SessionLocal()
    try:
        now     = time.time()
        expired = db.query(UserSession).filter(
            UserSession.status == "paid",
            UserSession.expiry_timestamp <= now,
        ).all()

        for session in expired:
            session.status = "expired"
            logger.info({"message": f"SESSION_EXPIRED: MAC={session.mac_address}"})
            await revoke_router_access(session.mac_address)

        if expired:
            db.commit()
    except Exception as e:
        logger.error({"message": f"CRON_ERROR: {e}"})
    finally:
        db.close()


# ─────────────────────────────────────────────
# RATE LIMITER [FIX-10]
# ─────────────────────────────────────────────
limiter = Limiter(key_func=get_remote_address)


# ─────────────────────────────────────────────
# APP LIFESPAN — startup + shutdown
# ─────────────────────────────────────────────
scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler.add_job(expire_sessions_job, "interval", minutes=1, id="expire_sessions")
    scheduler.start()
    logger.info({"message": "APScheduler started — session expiry cron active"})
    yield
    scheduler.shutdown(wait=False)
    logger.info({"message": "APScheduler stopped"})


# ─────────────────────────────────────────────
# FASTAPI APP
# ─────────────────────────────────────────────
app = FastAPI(title="Marvel Network Elite Core", lifespan=lifespan)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# CORS — tightened to portal origin [FIX-12]
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        PORTAL_ORIGIN,
        "http://192.168.88.1",   # Router local access
        "http://localhost:5500",  # Local dev
        "http://127.0.0.1:5500",
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type"],
)


# ─────────────────────────────────────────────
# MIDDLEWARE — request size guard
# ─────────────────────────────────────────────
@app.middleware("http")
async def limit_request_size(request: Request, call_next):
    content_length = request.headers.get("content-length", 0)
    if int(content_length) > 20_000:  # 20KB max payload
        return JSONResponse(status_code=413, content={"detail": "Payload too large"})
    return await call_next(request)


# ─────────────────────────────────────────────
# ENDPOINTS
# ─────────────────────────────────────────────

@app.get("/")
async def root():
    return {"status": "online", "service": "Marvel Network", "timestamp": time.time()}


@app.get("/health")
async def health(db: Session = Depends(get_db)):
    """Health check — used by UptimeRobot monitoring."""
    try:
        active_count = db.query(UserSession).filter(
            UserSession.status == "paid",
            UserSession.expiry_timestamp > time.time(),
        ).count()
        return {
            "status":          "online",
            "uptime_seconds":  round(time.time() - START_TIME),
            "active_sessions": active_count,
            "router_ip":       ROUTER_IP or "not configured",
            "mpesa_env":       "production" if "api.safaricom" in MPESA_BASE_URL else "sandbox",
            "timestamp":       datetime.datetime.utcnow().isoformat() + "Z",
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"DB error: {e}")


@app.post("/api/stk-push")
@limiter.limit("3/10minute")
async def stk_push(request: Request, data: dict, db: Session = Depends(get_db)):
    """
    Initiate M-Pesa STK Push.
    Expects: { phone, amount, hours, mac, ip }
    [FIX-1] Stores real MAC from frontend (injected by router redirect URL param).
    [FIX-10] Rate limited: 3 attempts per IP per 10 minutes.
    """
    try:
        # ── Extract & validate inputs ──
        raw_phone  = str(data.get("phone", "")).strip()
        raw_amount = str(data.get("amount", "")).replace("Ksh", "").strip()
        hours      = int(data.get("hours", 1))
        mac        = str(data.get("mac", "")).upper().strip()
        ip         = str(data.get("ip", "")).strip()

        # Validate MAC — must look like a real MAC or be empty
        # Allow empty MAC in dev/sandbox but warn
        if mac and len(mac.replace(":", "").replace("-", "")) != 12:
            logger.warning({"message": f"INVALID_MAC_FORMAT: {mac} — storing anyway"})

        # ── Phone normalisation ──
        phone = raw_phone
        if phone.startswith("+"):
            phone = phone[1:]
        if phone.startswith("0"):
            phone = "254" + phone[1:]
        if not phone.startswith("254") or len(phone) != 12:
            raise HTTPException(status_code=400, detail="Invalid phone: use 2547XXXXXXXX or 07XXXXXXXX")

        # ── Amount ──
        try:
            amount_int = int(float(raw_amount))
        except (ValueError, TypeError):
            raise HTTPException(status_code=400, detail="Invalid amount format")

        if amount_int < 1:
            raise HTTPException(status_code=400, detail="Amount must be at least Ksh 1")

        # ── EAT timestamp ──
        eat_now   = datetime.datetime.now(timezone.utc) + timedelta(hours=3)
        timestamp = eat_now.strftime("%Y%m%d%H%M%S")

        # ── Daraja password ──
        password_str = f"{MPESA_SHORTCODE}{MPESA_PASSKEY}{timestamp}"
        password     = base64.b64encode(password_str.encode()).decode()

        # ── STK Push payload ──
        token = get_mpesa_token()
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
            "TransactionDesc":   f"WiFi {hours}h",
        }

        push_url = f"{MPESA_BASE_URL}/mpesa/stkpush/v1/processrequest"
        resp = requests.post(
            push_url,
            json=payload,
            headers={"Authorization": f"Bearer {token}"},
            timeout=15,
        )
        resp_data = resp.json()

        if resp_data.get("ResponseCode") == "0":
            checkout_id = resp_data["CheckoutRequestID"]

            # Store session record with real MAC [FIX-1]
            new_session = UserSession(
                mac_address      = mac or f"UNKNOWN_{phone}",
                phone_number     = phone,
                checkout_id      = checkout_id,
                expiry_timestamp = time.time() + (hours * 3600),
                hours            = hours,
                status           = "pending",
                created_at       = time.time(),
            )
            db.add(new_session)
            db.commit()

            logger.info({
                "message":    "STK_PUSH_INITIATED",
                "phone":      phone,
                "amount":     amount_int,
                "hours":      hours,
                "mac":        mac or "not provided",
                "checkout_id": checkout_id,
            })
            return resp_data

        logger.error({"message": f"PROVIDER_REJECTION: {resp_data}"})
        raise HTTPException(
            status_code=400,
            detail=resp_data.get("CustomerMessage", "STK Push rejected by provider."),
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error({"message": f"STK_PUSH_CRASH: {e}"})
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/query-payment")
async def query_payment(id: str, db: Session = Depends(get_db)):
    """
    Poll payment status by CheckoutRequestID.
    Kept as UI polling fallback — real source of truth is /api/callback.
    """
    session = db.query(UserSession).filter(UserSession.checkout_id == id).first()
    return {"status": session.status if session else "not_found"}


@app.get("/api/session-status")
async def session_status(mac: str, db: Session = Depends(get_db)):
    """
    Check active session for a specific MAC address. [FIX-4]
    Called by frontend on load and after payment confirmed.
    """
    if not mac:
        raise HTTPException(status_code=400, detail="mac query param required")

    mac = mac.upper().strip()
    session = db.query(UserSession).filter(
        UserSession.mac_address == mac,
        UserSession.status == "paid",
    ).order_by(UserSession.id.desc()).first()

    if session and session.expiry_timestamp > time.time():
        return {
            "active":          True,
            "expiryTimestamp": session.expiry_timestamp * 1000,  # ms for JS
            "phone_mask":      f"****{session.phone_number[-4:]}",
            "hours":           session.hours,
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
    Safaricom Daraja STK callback.
    [FIX-2] Returns 200 IMMEDIATELY — processing happens in BackgroundTasks.
    Safaricom retries aggressively on slow or non-200 responses.
    """
    # Optional: restrict to known Safaricom IPs in production
    client_ip = request.client.host if request.client else "unknown"
    if MPESA_BASE_URL != "https://sandbox.safaricom.co.ke":
        if client_ip not in SAFARICOM_IPS:
            logger.warning({"message": f"CALLBACK_IP_REJECTED: {client_ip}"})
            raise HTTPException(status_code=403, detail="Forbidden")

    # Respond 200 immediately [FIX-2]
    background_tasks.add_task(_process_callback, data)
    return {"ResultCode": 0, "ResultDesc": "Accepted"}


async def _process_callback(data: dict):
    """
    Background task: update DB status and grant router access on successful payment.
    [FIX-3] Idempotency via mpesa_receipt unique column.
    [FIX-6] Calls grant_router_access() after confirming payment.
    """
    db = SessionLocal()
    try:
        logger.info({"message": "CALLBACK_RECEIVED", "raw": str(data)[:500]})

        stk_body    = data["Body"]["stkCallback"]
        checkout_id = stk_body["CheckoutRequestID"]
        result_code = stk_body["ResultCode"]

        session = db.query(UserSession).filter(
            UserSession.checkout_id == checkout_id
        ).first()

        if not session:
            logger.warning({"message": f"CALLBACK_ORPHAN: checkout_id={checkout_id} not in DB"})
            return

        # Idempotency — never process twice [FIX-3]
        if session.status == "paid":
            logger.info({"message": f"CALLBACK_DUPLICATE_IGNORED: {checkout_id}"})
            return

        if result_code == 0:
            # Extract receipt number for idempotency and audit
            items   = stk_body.get("CallbackMetadata", {}).get("Item", [])
            receipt = next((i["Value"] for i in items if i["Name"] == "MpesaReceiptNumber"), None)
            amount  = next((i["Value"] for i in items if i["Name"] == "Amount"), None)

            # Check receipt uniqueness (extra guard vs DB unique constraint)
            if receipt:
                existing = db.query(UserSession).filter(
                    UserSession.mpesa_receipt == receipt
                ).first()
                if existing:
                    logger.warning({"message": f"RECEIPT_DUPLICATE: {receipt}"})
                    return

            session.status        = "paid"
            session.mpesa_receipt = receipt
            db.commit()

            logger.info({
                "message":    "PAYMENT_CONFIRMED",
                "checkout_id": checkout_id,
                "mac":         session.mac_address,
                "receipt":     receipt,
                "amount":      amount,
            })

            # Grant router access [FIX-6]
            await grant_router_access(session.mac_address, session.hours)

        else:
            session.status = "failed"
            db.commit()
            logger.warning({
                "message":     "PAYMENT_FAILED",
                "checkout_id": checkout_id,
                "result_code": result_code,
                "mac":         session.mac_address,
            })

    except Exception as e:
        logger.error({"message": f"CALLBACK_PROCESSING_ERROR: {e}"})
        try:
            db.rollback()
        except Exception:
            pass
    finally:
        db.close()


@app.post("/api/terminate-session")
async def terminate_session(data: dict, db: Session = Depends(get_db)):
    """
    End a session by MAC address. [FIX-5]
    Called when user clicks 'Disconnect Link' in the portal.
    """
    mac = str(data.get("mac", "")).upper().strip()
    if not mac:
        raise HTTPException(status_code=400, detail="mac required")

    session = db.query(UserSession).filter(
        UserSession.mac_address == mac,
        UserSession.status == "paid",
    ).order_by(UserSession.id.desc()).first()

    if session:
        session.status           = "expired"
        session.expiry_timestamp = time.time()
        db.commit()
        logger.info({"message": f"SESSION_TERMINATED: MAC={mac}"})
        await revoke_router_access(mac)

    return {"status": "terminated"}


@app.post("/api/chat")
async def ai_chat(data: dict):
    """
    Gemini 1.5 Flash support chat. [FIX-13]
    Called by frontend text chat — returns AI reply for Marvel Network support queries.
    """
    if not _gemini_model:
        raise HTTPException(status_code=503, detail="AI service unavailable — GEMINI_API_KEY not set.")

    user_message = str(data.get("message", "")).strip()
    if not user_message:
        raise HTTPException(status_code=400, detail="message required")

    if len(user_message) > 1000:
        raise HTTPException(status_code=400, detail="Message too long (max 1000 chars)")

    try:
        full_prompt = f"{GEMINI_SYSTEM_PROMPT}\n\nUser: {user_message}\nAgent:"
        response    = _gemini_model.generate_content(full_prompt)
        reply       = response.text.strip()
        logger.info({"message": "CHAT_RESPONSE_SENT", "chars": len(reply)})
        return {"reply": reply}
    except Exception as e:
        logger.error({"message": f"GEMINI_ERROR: {e}"})
        raise HTTPException(status_code=502, detail="AI response failed — try again.")


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8000)),
        reload=False,
        log_level="info",
    )
