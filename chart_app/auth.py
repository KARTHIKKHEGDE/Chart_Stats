"""Auth feature module."""
from chart_app.application import (
    IST,
    JOBS,
    SUPABASE_KEY,
    SUPABASE_URL,
    app,
    dt,
    hashlib,
    jsonify,
    os,
    re,
    request,
    requests,
    run_job,
    secrets,
    threading,
    uuid,
)

ALLOWED_USERS = {"karthik", "locke", "gunshow"}


ADMIN_USERS = {"karthik", "locke"}


def _supabase_headers(prefer="return=minimal"):
    k = os.getenv("SUPABASE_KEY", "") or SUPABASE_KEY
    h = {
        "apikey": k,
        "Authorization": f"Bearer {k}",
        "Content-Type": "application/json"
    }
    if prefer:
        h["Prefer"] = prefer
    return h


def _db_sync_users():
    """Sync ALLOWED_USERS and ADMIN_USERS from Supabase app_users table."""
    sub_url = (os.getenv("SUPABASE_URL") or SUPABASE_URL).rstrip("/")
    sub_key = os.getenv("SUPABASE_KEY") or SUPABASE_KEY
    if not (sub_url and sub_key):
        return
    try:
        r = requests.get(f"{sub_url}/rest/v1/app_users?select=username,is_admin,can_rename", headers=_supabase_headers(), timeout=5)
        if r.status_code == 200:
            rows = r.json()
            if isinstance(rows, list) and rows:
                global ALLOWED_USERS, ADMIN_USERS
                ALLOWED_USERS = {x["username"].lower() for x in rows if x.get("username")}
                ADMIN_USERS = {x["username"].lower() for x in rows if x.get("is_admin") or x.get("can_rename")}
    except Exception:
        pass


def _db_get_user(username: str):
    u = (username or "").strip().lower()
    if not u:
        return None
    sub_url = (os.getenv("SUPABASE_URL") or SUPABASE_URL).rstrip("/")
    sub_key = os.getenv("SUPABASE_KEY") or SUPABASE_KEY
    if sub_url and sub_key:
        try:
            r = requests.get(f"{sub_url}/rest/v1/app_users?username=eq.{u}&select=*", headers=_supabase_headers(), timeout=5)
            if r.status_code == 200:
                rows = r.json()
                if isinstance(rows, list) and rows:
                    return rows[0]
        except Exception:
            pass
    if u in ALLOWED_USERS:
        is_root = u in {"karthik", "locke"}
        return {"username": u, "is_admin": is_root or u in ADMIN_USERS, "can_rename": is_root}
    return None


def _hash_password(password: str, salt: str = None) -> tuple[str, str]:
    if not salt:
        salt = secrets.token_hex(16)
    pwd_hash = hashlib.sha256((salt + password).encode("utf-8")).hexdigest()
    return pwd_hash, salt


def _verify_password(password: str, pwd_hash: str, salt: str) -> bool:
    if not (password and pwd_hash and salt):
        return False
    calc_hash = hashlib.sha256((salt + password).encode("utf-8")).hexdigest()
    return secrets.compare_digest(calc_hash, pwd_hash)


def _db_set_password(username: str, password: str):
    u = username.strip().lower()
    pwd_hash, salt = _hash_password(password)
    new_token = uuid.uuid4().hex
    now_z = dt.datetime.now(dt.timezone.utc).isoformat()
    sub_url = (os.getenv("SUPABASE_URL") or SUPABASE_URL).rstrip("/")
    sub_key = os.getenv("SUPABASE_KEY") or SUPABASE_KEY
    if sub_url and sub_key:
        payload = {
            "password_hash": pwd_hash,
            "salt": salt,
            "session_token": new_token,
            "session_updated_at": now_z
        }
        requests.patch(f"{sub_url}/rest/v1/app_users?username=eq.{u}", json=payload, headers=_supabase_headers(), timeout=5)
    return new_token


def _db_login(username: str, password: str, force_logout: bool = False):
    u = username.strip().lower()
    user = _db_get_user(u)
    if not user:
        return {"error": "User not authorized"}, 404
    
    if not user.get("password_hash") or not user.get("salt"):
        return {"error": "Password not created yet", "require_create": True}, 400
    
    if not _verify_password(password, user.get("password_hash"), user.get("salt")):
        return {"error": "Incorrect password. Please try again."}, 401
    
    is_exempt = bool(user.get("can_rename")) or (u in {"karthik", "locke"})
    existing_session = user.get("session_token")
    
    # Enforce single browser for non-exempt users
    if not is_exempt and existing_session and not force_logout:
        return {
            "error": "Account is currently logged in on another device/browser.",
            "require_force": True,
            "active_session": True
        }, 409
    
    new_token = uuid.uuid4().hex
    now_z = dt.datetime.now(dt.timezone.utc).isoformat()
    sub_url = (os.getenv("SUPABASE_URL") or SUPABASE_URL).rstrip("/")
    sub_key = os.getenv("SUPABASE_KEY") or SUPABASE_KEY
    if sub_url and sub_key:
        payload = {"session_token": new_token, "session_updated_at": now_z}
        requests.patch(f"{sub_url}/rest/v1/app_users?username=eq.{u}", json=payload, headers=_supabase_headers(), timeout=5)
    
    return {
        "ok": True,
        "username": u,
        "session_token": new_token,
        "is_admin": bool(user.get("is_admin")),
        "can_rename": is_exempt
    }, 200


def _db_validate_session(username: str, token: str) -> bool:
    u = (username or "").strip().lower()
    if not u or not token:
        return False
    user = _db_get_user(u)
    if not user:
        return False
    is_exempt = bool(user.get("can_rename")) or (u in {"karthik", "locke"})
    if is_exempt:
        # Karthik and Locke can be logged in from any number of browsers simultaneously
        return True
    db_token = user.get("session_token")
    return bool(db_token and secrets.compare_digest(db_token, token))


def _db_rename_user(old_name: str, new_name: str):
    old_u = old_name.strip().lower()
    new_u = new_name.strip().lower()
    if not re.fullmatch(r"[a-z0-9_]{2,20}", new_u):
        return {"error": "Username must be 2-20 lowercase letters, numbers, or underscore."}, 400
    
    user = _db_get_user(old_u)
    if not user or not (user.get("can_rename") or old_u in {"karthik", "locke"}):
        return {"error": "Permission denied: Only Karthik and Locke are permitted to change username."}, 403
    
    if old_u == new_u:
        return {"ok": True, "new_username": new_u}, 200
    
    existing = _db_get_user(new_u)
    if existing:
        return {"error": f"Username '{new_u}' is already taken. Please choose another."}, 409
    
    sub_url = (os.getenv("SUPABASE_URL") or SUPABASE_URL).rstrip("/")
    sub_key = os.getenv("SUPABASE_KEY") or SUPABASE_KEY
    if sub_url and sub_key:
        h = _supabase_headers(prefer="return=representation")
        r1 = requests.patch(f"{sub_url}/rest/v1/app_users?username=eq.{old_u}", json={"username": new_u}, headers=h, timeout=10)
        if r1.status_code not in (200, 204):
            return {"error": f"Failed to update database: {r1.text}"}, 500
        
        # Transfer search history in searches table
        requests.patch(f"{sub_url}/rest/v1/searches?user_name=eq.{old_u.upper()}", json={"user_name": new_u.upper()}, headers=h, timeout=10)
    
    global ALLOWED_USERS, ADMIN_USERS
    ALLOWED_USERS.discard(old_u)
    ALLOWED_USERS.add(new_u)
    if old_u in ADMIN_USERS:
        ADMIN_USERS.discard(old_u)
        ADMIN_USERS.add(new_u)
    
    return {"ok": True, "old_name": old_u, "new_name": new_u}, 200


def log_search(user, ticker):
    if not user or not ticker:
        return
    sub_url = (os.getenv("SUPABASE_URL") or SUPABASE_URL).rstrip("/")
    sub_key = os.getenv("SUPABASE_KEY") or SUPABASE_KEY
    if not (sub_url and sub_key):
        return

    now_dt = dt.datetime.now(IST)
    now_ist = now_dt.strftime("%Y-%m-%d %H:%M:%S")
    now_date = now_dt.strftime("%Y-%m-%d")

    try:
        endpoint = f"{sub_url}/rest/v1/searches"
        payload = {
            "user_name": user.upper(),
            "ticker": ticker,
            "time_ist": now_ist,
            "date_ist": now_date
        }
        r = requests.post(endpoint, json=payload, headers=_supabase_headers(), timeout=5)
        if r.status_code not in (200, 201):
            requests.post(endpoint, json={"user_name": user.upper(), "ticker": ticker, "time_ist": now_ist}, headers=_supabase_headers(), timeout=5)
    except Exception:
        pass


def log_keyword_search(user, keyword, start_date, end_date, forms):
    """Record a successful SEC keyword search for the admin activity audit."""
    sub_url = (os.getenv("SUPABASE_URL") or SUPABASE_URL).rstrip("/")
    sub_key = os.getenv("SUPABASE_KEY") or SUPABASE_KEY
    if not (user and keyword and sub_url and sub_key):
        return
    now_dt = dt.datetime.now(IST)
    payload = {
        "user_name": user.upper(),
        "keyword": keyword[:120],
        "start_date": start_date,
        "end_date": end_date,
        "forms": forms or None,
        "time_ist": now_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "date_ist": now_dt.strftime("%Y-%m-%d"),
    }
    try:
        requests.post(f"{sub_url}/rest/v1/keyword_searches", json=payload,
                      headers=_supabase_headers(), timeout=5)
    except Exception:
        app.logger.exception("Could not record keyword search activity")


@app.route("/api/auth/status")
def auth_status():
    u = (request.args.get("user") or "").strip().lower()
    if not u:
        return jsonify({"error": "Username required"}), 400
    user = _db_get_user(u)
    if not user:
        return jsonify({"error": "Username not authorized"}), 404
    is_exempt = bool(user.get("can_rename")) or (u in {"karthik", "locke"})
    has_pwd = bool(user.get("password_hash") and user.get("salt"))
    has_active = bool(user.get("session_token"))
    return jsonify({
        "exists": True,
        "username": u,
        "has_password": has_pwd,
        "is_admin": bool(user.get("is_admin")),
        "can_rename": is_exempt,
        "has_active_session": (has_active and not is_exempt)
    })


@app.route("/api/auth/set-password", methods=["POST"])
def auth_set_password():
    data = request.get_json(silent=True) or request.form
    u = (data.get("user") or "").strip().lower()
    pwd = (data.get("password") or "").strip()
    if not u or not pwd:
        return jsonify({"error": "Username and password required"}), 400
    user = _db_get_user(u)
    if not user:
        return jsonify({"error": "User not authorized"}), 404
    
    token = _db_set_password(u, pwd)
    is_exempt = bool(user.get("can_rename")) or (u in {"karthik", "locke"})
    return jsonify({
        "ok": True,
        "username": u,
        "session_token": token,
        "is_admin": bool(user.get("is_admin")),
        "can_rename": is_exempt
    })


@app.route("/api/auth/login", methods=["POST"])
def auth_login():
    data = request.get_json(silent=True) or request.form
    u = (data.get("user") or "").strip().lower()
    pwd = (data.get("password") or "").strip()
    force = bool(data.get("force_logout") or data.get("force"))
    res, status_code = _db_login(u, pwd, force_logout=force)
    return jsonify(res), status_code


@app.route("/api/auth/verify-session")
def auth_verify_session():
    u = (request.args.get("user") or "").strip().lower()
    token = (request.args.get("token") or "").strip()
    if not u or not token:
        return jsonify({"ok": False, "error": "Missing user or token"}), 400
    ok = _db_validate_session(u, token)
    if not ok:
        return jsonify({"ok": False, "error": "Session terminated: This account logged in from another browser."}), 401
    user = _db_get_user(u)
    is_exempt = bool(user and user.get("can_rename")) or (u in {"karthik", "locke"})
    return jsonify({"ok": True, "can_rename": is_exempt, "is_admin": bool(user and user.get("is_admin"))})


@app.route("/api/auth/rename-user", methods=["POST"])
def auth_rename_user():
    data = request.get_json(silent=True) or request.form
    old_u = (data.get("old_name") or data.get("user") or "").strip().lower()
    new_u = (data.get("new_name") or "").strip().lower()
    token = (data.get("token") or "").strip()
    if not old_u or not new_u:
        return jsonify({"error": "Old and new usernames required"}), 400
    if token and not _db_validate_session(old_u, token):
        return jsonify({"error": "Invalid session"}), 401
    res, code = _db_rename_user(old_u, new_u)
    return jsonify(res), code


@app.route("/api/start")
def start():
    u = (request.args.get("user") or "").strip().lower()
    token = (request.args.get("token") or "").strip()
    if not u or u not in ALLOWED_USERS:
        return jsonify({"error": "Access denied"}), 401
    if token and not _db_validate_session(u, token):
        return jsonify({"error": "Session expired: Logged in on another browser.", "code": "SESSION_REVOKED"}), 401
    t = (request.args.get("ticker") or "").strip().upper()
    if not re.fullmatch(r"[A-Z.\-]{1,10}", t):
        return jsonify({"error": "Invalid ticker"}), 400
    log_search(u, t)
    jid = uuid.uuid4().hex[:10]
    JOBS[jid] = {"status": "running", "msg": "Starting…"}
    threading.Thread(target=run_job, args=(JOBS[jid], t, request.args.get("refresh") == "1"), daemon=True).start()
    return jsonify({"job": jid})


@app.route("/api/admin/searches")
def admin_searches():
    u = (request.args.get("user") or "").strip().lower()
    pwd = (request.args.get("pwd") or "").strip()
    if u not in ADMIN_USERS or pwd != "500":
        return jsonify({"error": "Unauthorized"}), 401

    sub_url = (os.getenv("SUPABASE_URL") or SUPABASE_URL).rstrip("/")
    sub_key = os.getenv("SUPABASE_KEY") or SUPABASE_KEY
    data = []
    err_info = None
    if sub_url and sub_key:
        try:
            endpoint = f"{sub_url}/rest/v1/searches?select=user_name,ticker,time_ist,date_ist&order=id.desc&limit=1000"
            r = requests.get(endpoint, headers=_supabase_headers(), timeout=10)
            if r.status_code == 200:
                rows = r.json()
                if isinstance(rows, list):
                    data = [{"user": (x.get("user_name") or x.get("user") or "").upper(),
                             "ticker": x.get("ticker"), "type": "ticker",
                             "time": x.get("time_ist") or x.get("time"),
                             "date": x.get("date_ist")} for x in rows]
            else:
                err_info = f"Supabase HTTP {r.status_code}: {r.text[:200]}"
            try:
                keyword_endpoint = f"{sub_url}/rest/v1/keyword_searches?select=user_name,keyword,start_date,end_date,forms,time_ist,date_ist&order=id.desc&limit=1000"
                keyword_response = requests.get(keyword_endpoint, headers=_supabase_headers(), timeout=10)
                if keyword_response.status_code == 200:
                    keyword_rows = keyword_response.json()
                    if isinstance(keyword_rows, list):
                        data.extend({"user": (x.get("user_name") or "").upper(),
                                     "ticker": x.get("keyword"), "keyword": x.get("keyword"),
                                     "type": "keyword", "start_date": x.get("start_date"),
                                     "end_date": x.get("end_date"), "forms": x.get("forms"),
                                     "time": x.get("time_ist"), "date": x.get("date_ist")}
                                    for x in keyword_rows)
                        data.sort(key=lambda x: x.get("time") or "", reverse=True)
                elif not err_info:
                    err_info = ("Keyword search audit table unavailable. Apply the migration in "
                                "supabase_keyword_searches.sql. "
                                f"Supabase HTTP {keyword_response.status_code}.")
            except Exception as ex:
                if not err_info:
                    err_info = f"Could not load keyword search history: {ex}"
        except Exception as ex:
            err_info = str(ex)
    else:
        err_info = "SUPABASE_URL or SUPABASE_KEY not configured on server"

    resp = {"searches": data}
    if err_info:
        resp["_warn"] = err_info
    return jsonify(resp)


@app.route("/api/admin/clear-old-searches", methods=["POST", "DELETE", "GET"])
def clear_old_searches():
    u = (request.args.get("user") or request.form.get("user") or "").strip().lower()
    pwd = (request.args.get("pwd") or request.form.get("pwd") or "").strip()
    if u not in ADMIN_USERS or pwd != "500":
        return jsonify({"error": "Unauthorized"}), 401

    sub_url = (os.getenv("SUPABASE_URL") or SUPABASE_URL).rstrip("/")
    sub_key = os.getenv("SUPABASE_KEY") or SUPABASE_KEY
    if not (sub_url and sub_key):
        return jsonify({"error": "Supabase not configured on server"}), 500

    now_ist = dt.datetime.now(IST)
    cutoff_dt = now_ist - dt.timedelta(days=14)
    cutoff_date = cutoff_dt.strftime("%Y-%m-%d")
    cutoff_z = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=14)).strftime("%Y-%m-%dT%H:%M:%SZ")

    deleted_count = 0
    try:
        h = _supabase_headers(prefer="return=representation")

        # Delete records older than 14 days by date_ist
        r1 = requests.delete(f"{sub_url}/rest/v1/searches?date_ist=lt.{cutoff_date}", headers=h, timeout=15)
        if r1.status_code in (200, 204):
            data1 = r1.json() if r1.text else []
            deleted_count += len(data1) if isinstance(data1, list) else 0

        # Delete any rows where created_at is older than 14 days
        r2 = requests.delete(f"{sub_url}/rest/v1/searches?created_at=lt.{cutoff_z}", headers=h, timeout=15)
        if r2.status_code in (200, 204):
            data2 = r2.json() if r2.text else []
            deleted_count += len(data2) if isinstance(data2, list) else 0

        r3 = requests.delete(f"{sub_url}/rest/v1/keyword_searches?date_ist=lt.{cutoff_date}", headers=h, timeout=15)
        if r3.status_code in (200, 204):
            data3 = r3.json() if r3.text else []
            deleted_count += len(data3) if isinstance(data3, list) else 0

        return jsonify({
            "ok": True,
            "deleted": deleted_count,
            "cutoff": cutoff_date,
            "message": f"Successfully deleted {deleted_count} search record(s) older than 2 weeks ({cutoff_date})."
        })
    except Exception as ex:
        return jsonify({"error": f"Failed to delete old records: {ex}"}), 500
