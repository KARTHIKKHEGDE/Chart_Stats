"""Scanner Settings feature module."""
from chart_app.application import (
    JOBS,
    Path,
    SUPABASE_KEY,
    SUPABASE_URL,
    _db_get_user,
    _db_sync_users,
    _db_validate_session,
    _supabase_headers,
    app,
    json,
    jsonify,
    os,
    quote,
    re,
    request,
    requests,
    threading,
)
from chart_app import auth as _auth

DEFAULT_SCANNER_SETTINGS = {
    "version": 4,
    "voice_enabled": False,
    "voice_uri": "",
    "approach_pct": 2.0,
    "vwap_enabled": True,
}


_scanner_user_settings_lock = threading.RLock()


_scanner_user_settings_file = Path(__file__).resolve().parent.parent / ".scanner_user_settings.json"


_APP_UI_VISIBILITY_KEY = "app_ui_visibility"


_APP_UI_VISIBILITY_USER_KEY = "app_ui_visibility_user"


_APP_UI_VISIBILITY_DEFAULT_KEY = "app_ui_visibility_default"


_APP_UI_VISIBILITY_DEFAULTS = {"scanner": False, "runner_stats": True}


_app_ui_visibility_file = Path(__file__).resolve().parent.parent / ".app_ui_visibility.json"


_app_ui_user_visibility_file = Path(__file__).resolve().parent.parent / ".app_ui_user_visibility.json"


_app_ui_visibility_lock = threading.RLock()


def _normalize_scanner_settings(value):
    source = value if isinstance(value, dict) else {}
    try:
        version = int(source.get("version", 0))
    except (TypeError, ValueError):
        version = 0
    try:
        approach_pct = float(source.get("approach_pct", DEFAULT_SCANNER_SETTINGS["approach_pct"]))
    except (TypeError, ValueError):
        approach_pct = DEFAULT_SCANNER_SETTINGS["approach_pct"]
    return {
        "version": 4,
        "voice_enabled": False if version < 4 else bool(source.get("voice_enabled", DEFAULT_SCANNER_SETTINGS["voice_enabled"])),
        "voice_uri": str(source.get("voice_uri") or "")[:300],
        "approach_pct": round(max(0.1, min(10.0, approach_pct)), 2),
        "vwap_enabled": bool(source.get("vwap_enabled", DEFAULT_SCANNER_SETTINGS["vwap_enabled"])),
    }


def _read_local_scanner_settings():
    try:
        with _scanner_user_settings_lock:
            data = json.loads(_scanner_user_settings_file.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _write_local_scanner_settings(username, settings):
    with _scanner_user_settings_lock:
        data = _read_local_scanner_settings()
        data[username] = settings
        tmp = _scanner_user_settings_file.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(_scanner_user_settings_file)


def _normalize_app_ui_visibility(value):
    source = value if isinstance(value, dict) else {}
    return {
        "scanner": False,
        "runner_stats": source.get("runner_stats") is not False,
    }


def _parse_admin_scanner_settings(value):
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            value = {}
    return value if isinstance(value, dict) else {}


def _read_local_app_ui_visibility():
    try:
        with _app_ui_visibility_lock:
            value = json.loads(_app_ui_visibility_file.read_text(encoding="utf-8"))
        return _normalize_app_ui_visibility(value)
    except Exception:
        return None


def _write_local_app_ui_visibility(visibility):
    with _app_ui_visibility_lock:
        temp_path = _app_ui_visibility_file.with_suffix(".tmp")
        temp_path.write_text(json.dumps(visibility, indent=2), encoding="utf-8")
        temp_path.replace(_app_ui_visibility_file)


def _read_local_user_ui_visibility(username):
    try:
        with _app_ui_visibility_lock:
            saved = json.loads(_app_ui_user_visibility_file.read_text(encoding="utf-8"))
        value = saved.get(username) if isinstance(saved, dict) else None
        return _normalize_app_ui_visibility(value) if isinstance(value, dict) else None
    except Exception:
        return None


def _write_local_user_ui_visibility(username, visibility):
    with _app_ui_visibility_lock:
        try:
            saved = json.loads(_app_ui_user_visibility_file.read_text(encoding="utf-8"))
            if not isinstance(saved, dict):
                saved = {}
        except Exception:
            saved = {}
        saved[username] = _normalize_app_ui_visibility(visibility)
        temp_path = _app_ui_user_visibility_file.with_suffix(".tmp")
        temp_path.write_text(json.dumps(saved, indent=2), encoding="utf-8")
        temp_path.replace(_app_ui_user_visibility_file)


def _get_app_ui_visibility(username=None):
    target = (username or "").strip().lower()
    if target:
        local_user = _read_local_user_ui_visibility(target)
        if local_user is not None:
            return local_user
    sub_url = (os.getenv("SUPABASE_URL") or SUPABASE_URL).rstrip("/")
    sub_key = os.getenv("SUPABASE_KEY") or SUPABASE_KEY
    if sub_url and sub_key:
        try:
            response = requests.get(
                f"{sub_url}/rest/v1/app_users?select=username,scanner_settings,is_admin,can_rename",
                headers=_supabase_headers(), timeout=5
            )
            if response.status_code == 200:
                rows = response.json()
                row = next((item for item in rows if (item.get("username") or "").lower() == target), None)
                if row:
                    saved = _parse_admin_scanner_settings(row.get("scanner_settings")).get(_APP_UI_VISIBILITY_USER_KEY)
                    if isinstance(saved, dict):
                        return _normalize_app_ui_visibility(saved)
                admins = [item for item in rows if item.get("is_admin") or item.get("can_rename")]
                admins.sort(key=lambda item: ((item.get("username") or "").lower() not in ("karthik", "locke"),
                                             (item.get("username") or "").lower()))
                for admin_row in admins:
                    admin_settings = _parse_admin_scanner_settings(admin_row.get("scanner_settings"))
                    saved = admin_settings.get(_APP_UI_VISIBILITY_DEFAULT_KEY)
                    if not isinstance(saved, dict):  # retain settings saved by older versions
                        saved = admin_settings.get(_APP_UI_VISIBILITY_KEY)
                    if isinstance(saved, dict):
                        return _normalize_app_ui_visibility(saved)
        except Exception:
            pass
    return _read_local_app_ui_visibility() or dict(_APP_UI_VISIBILITY_DEFAULTS)


def _save_app_ui_visibility(visibility):
    clean = _normalize_app_ui_visibility(visibility)
    sub_url = (os.getenv("SUPABASE_URL") or SUPABASE_URL).rstrip("/")
    sub_key = os.getenv("SUPABASE_KEY") or SUPABASE_KEY
    if sub_url and sub_key:
        response = requests.get(
            f"{sub_url}/rest/v1/app_users?select=username,scanner_settings,is_admin,can_rename",
            headers=_supabase_headers(), timeout=5
        )
        if response.status_code != 200:
            raise RuntimeError(f"Could not load admin settings (HTTP {response.status_code})")
        rows = response.json()
        admins = [row for row in rows if row.get("is_admin") or row.get("can_rename")]
        if not admins:
            raise RuntimeError("No admin accounts are available to store app visibility settings")
        for row in admins:
            username = (row.get("username") or "").strip().lower()
            if not username:
                continue
            settings = _parse_admin_scanner_settings(row.get("scanner_settings"))
            settings[_APP_UI_VISIBILITY_DEFAULT_KEY] = clean
            result = requests.patch(
                f"{sub_url}/rest/v1/app_users?username=eq.{quote(username, safe='')}",
                json={"scanner_settings": settings}, headers=_supabase_headers(), timeout=6
            )
            if result.status_code not in (200, 204):
                raise RuntimeError(f"Could not save visibility settings for {username} "
                                   f"(HTTP {result.status_code})")
        try:
            _write_local_app_ui_visibility(clean)
        except Exception:
            pass
        return clean, "Supabase"
    _write_local_app_ui_visibility(clean)
    return clean, "local fallback"


def _get_user_scanner_settings(username):
    record = _db_get_user(username) or {}
    remote = record.get("scanner_settings")
    if isinstance(remote, dict) and remote:
        return _normalize_scanner_settings(remote), True, "Supabase"
    local = _read_local_scanner_settings().get(username)
    if isinstance(local, dict):
        return _normalize_scanner_settings(local), True, "local fallback"
    return _normalize_scanner_settings(DEFAULT_SCANNER_SETTINGS), False, "default"


def _save_user_scanner_settings(username, settings):
    clean_settings = _normalize_scanner_settings(settings)
    existing = _db_get_user(username) or {}
    preserved = _parse_admin_scanner_settings(existing.get("scanner_settings"))
    for visibility_key in (_APP_UI_VISIBILITY_KEY, _APP_UI_VISIBILITY_USER_KEY, _APP_UI_VISIBILITY_DEFAULT_KEY,
                           "runner_csv_collections"):
        if isinstance(preserved.get(visibility_key), dict):
            clean_settings[visibility_key] = preserved[visibility_key]
        elif visibility_key == "runner_csv_collections" and isinstance(preserved.get(visibility_key), list):
            clean_settings[visibility_key] = preserved[visibility_key]
    local_error = None
    try:
        _write_local_scanner_settings(username, clean_settings)
    except Exception as exc:
        local_error = str(exc)

    sub_url = (os.getenv("SUPABASE_URL") or SUPABASE_URL).rstrip("/")
    sub_key = os.getenv("SUPABASE_KEY") or SUPABASE_KEY
    if sub_url and sub_key:
        try:
            response = requests.patch(
                f"{sub_url}/rest/v1/app_users?username=eq.{quote(username, safe='')}",
                json={"scanner_settings": clean_settings}, headers=_supabase_headers(), timeout=6
            )
            if response.status_code in (200, 204):
                return clean_settings, "Supabase"
        except Exception:
            pass
    if local_error:
        raise RuntimeError(f"Could not save scanner preferences: {local_error}")
    return clean_settings, "local fallback"


@app.route("/api/ui-visibility")
def app_ui_visibility():
    username = (request.args.get("user") or "").strip().lower()
    token = (request.args.get("token") or "").strip()
    if not username or not token or not _db_validate_session(username, token):
        return jsonify({"error": "Unauthorized"}), 401
    return jsonify(_get_app_ui_visibility(username))


@app.route("/api/admin/users/<target>/ui-visibility", methods=["GET", "POST"])
def admin_user_ui_visibility(target):
    admin = (request.args.get("user") or (request.get_json(silent=True) or {}).get("user") or "").strip().lower()
    token = (request.args.get("token") or (request.get_json(silent=True) or {}).get("token") or "").strip()
    password = (request.args.get("pwd") or (request.get_json(silent=True) or {}).get("pwd") or "").strip()
    if admin not in {"karthik", "locke"} or password != "500" or not token or not _db_validate_session(admin, token):
        return jsonify({"error": "Unauthorized"}), 401
    target = (target or "").strip().lower()
    if not re.fullmatch(r"[a-z0-9_]{2,20}", target) or not _db_get_user(target):
        return jsonify({"error": "User not found"}), 404
    if request.method == "GET":
        return jsonify({"visibility": _get_app_ui_visibility(target)})
    body = request.get_json(silent=True) or {}
    visibility = body.get("visibility")
    if not isinstance(visibility, dict) or any(type(visibility.get(key)) is not bool for key in ("scanner", "runner_stats")):
        return jsonify({"error": "Both visibility options must be true or false."}), 400
    record = _db_get_user(target) or {}
    settings = _parse_admin_scanner_settings(record.get("scanner_settings"))
    settings[_APP_UI_VISIBILITY_USER_KEY] = _normalize_app_ui_visibility(visibility)
    sub_url = (os.getenv("SUPABASE_URL") or SUPABASE_URL).rstrip("/")
    if sub_url:
        result = requests.patch(f"{sub_url}/rest/v1/app_users?username=eq.{quote(target, safe='')}",
                                json={"scanner_settings": settings}, headers=_supabase_headers(), timeout=6)
        if result.status_code not in (200, 204):
            return jsonify({"error": f"Could not save access settings (HTTP {result.status_code})"}), 500
    else:
        try:
            _write_local_user_ui_visibility(target, settings[_APP_UI_VISIBILITY_USER_KEY])
        except Exception as exc:
            return jsonify({"error": f"Could not save access settings: {exc}"}), 500
    return jsonify({"ok": True, "visibility": settings[_APP_UI_VISIBILITY_USER_KEY]})


@app.route("/api/admin/ui-visibility", methods=["GET", "POST"])
def admin_app_ui_visibility():
    body = request.get_json(silent=True) or {}
    username = (request.args.get("user") or body.get("user") or "").strip().lower()
    token = (request.args.get("token") or body.get("token") or "").strip()
    password = (request.args.get("pwd") or body.get("pwd") or "").strip()
    if (username not in _auth.ADMIN_USERS or password != "500" or not token or
            not _db_validate_session(username, token)):
        return jsonify({"error": "Unauthorized"}), 401
    if request.method == "GET":
        return jsonify({"visibility": _get_app_ui_visibility(), "storage": "Supabase or local fallback"})

    requested = body.get("visibility")
    if (not isinstance(requested, dict) or type(requested.get("scanner")) is not bool or
            type(requested.get("runner_stats")) is not bool):
        return jsonify({"error": "Both visibility options must be true or false."}), 400
    try:
        visibility, storage = _save_app_ui_visibility(requested)
        return jsonify({"ok": True, "visibility": visibility, "storage": storage})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/scanner/settings", methods=["GET", "POST"])
def scanner_user_settings():
    body = request.get_json(silent=True) or {}
    username = (request.args.get("user") or body.get("user") or "").strip().lower()
    token = (request.args.get("token") or body.get("token") or "").strip()
    if not username or not token or not _db_validate_session(username, token):
        return jsonify({"error": "Session expired. Please sign in again."}), 401
    if request.method == "GET":
        settings, configured, storage = _get_user_scanner_settings(username)
        return jsonify({"settings": settings, "configured": configured, "storage": storage})
    try:
        settings, storage = _save_user_scanner_settings(username, body.get("settings"))
        return jsonify({"ok": True, "settings": settings, "storage": storage})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/admin/users")
def admin_get_users():
    u = (request.args.get("user") or "").strip().lower()
    pwd = (request.args.get("pwd") or "").strip()
    if u not in _auth.ADMIN_USERS or pwd != "500":
        return jsonify({"error": "Unauthorized"}), 401
    _db_sync_users()
    sub_url = (os.getenv("SUPABASE_URL") or SUPABASE_URL).rstrip("/")
    sub_key = os.getenv("SUPABASE_KEY") or SUPABASE_KEY
    users_data = []
    if sub_url and sub_key:
        try:
            r = requests.get(f"{sub_url}/rest/v1/app_users?select=username,is_admin,can_rename", headers=_supabase_headers(), timeout=5)
            if r.status_code == 200:
                users_data = r.json()
        except Exception:
            pass
    u_list = [x["username"] for x in users_data] if users_data else sorted(list(_auth.ALLOWED_USERS))
    admins = [x["username"] for x in users_data if x.get("is_admin") or x.get("can_rename")] if users_data else sorted(list(_auth.ADMIN_USERS))
    can_rename = [x["username"] for x in users_data if x.get("can_rename")] if users_data else ["karthik", "locke"]
    return jsonify({"users": sorted(u_list), "admins": sorted(admins), "can_rename": sorted(can_rename)})


@app.route("/api/admin/users/<target>/details")
def admin_user_details(target):
    admin = (request.args.get("user") or "").strip().lower()
    pwd = (request.args.get("pwd") or "").strip()
    if admin not in _auth.ADMIN_USERS or pwd != "500":
        return jsonify({"error": "Unauthorized"}), 401
    target = (target or "").strip().lower()
    if not re.fullmatch(r"[a-z0-9_]{2,20}", target):
        return jsonify({"error": "Invalid username"}), 400
    record = _db_get_user(target) or {}
    if target not in _auth.ALLOWED_USERS and not record:
        return jsonify({"error": "User not found"}), 404

    profile_fields = ("username", "is_admin", "can_rename", "created_at", "updated_at", "session_updated_at")
    profile = {key: record[key] for key in profile_fields if key in record}
    profile.setdefault("username", target)
    profile["has_password"] = bool(record.get("password_hash") and record.get("salt"))
    profile["active_session"] = bool(record.get("session_token"))

    searches = []
    search_error = None
    sub_url = (os.getenv("SUPABASE_URL") or SUPABASE_URL).rstrip("/")
    sub_key = os.getenv("SUPABASE_KEY") or SUPABASE_KEY
    if sub_url and sub_key:
        try:
            offset = 0
            while offset < 10000:
                endpoint = (f"{sub_url}/rest/v1/searches?user_name=eq.{quote(target, safe='')}"
                            f"&select=ticker,time_ist,date_ist&order=id.desc&limit=1000&offset={offset}")
                response = requests.get(endpoint, headers=_supabase_headers(), timeout=8)
                if response.status_code != 200:
                    search_error = f"Search history unavailable (HTTP {response.status_code})."
                    break
                batch = response.json() if isinstance(response.json(), list) else []
                searches.extend(batch)
                if len(batch) < 1000:
                    break
                offset += len(batch)
        except Exception as exc:
            search_error = f"Search history unavailable: {exc}"
    else:
        search_error = "Supabase is not configured; search history is unavailable."

    return jsonify({"profile": profile,
                    "ui_visibility": _get_app_ui_visibility(target),
                    "recent_searches": searches, "search_error": search_error})


@app.route("/api/admin/users/add", methods=["POST"])
def admin_add_user():
    u = (request.args.get("user") or request.form.get("user") or "").strip().lower()
    pwd = (request.args.get("pwd") or request.form.get("pwd") or "").strip()
    if u not in _auth.ADMIN_USERS or pwd != "500":
        return jsonify({"error": "Unauthorized"}), 401
    new_user = (request.args.get("name") or request.form.get("name") or "").strip().lower()
    if not new_user or not re.fullmatch(r"[a-z0-9_]{2,20}", new_user):
        return jsonify({"error": "Invalid username (2-20 lowercase letters/digits/underscore)"}), 400
    if new_user in _auth.ALLOWED_USERS:
        return jsonify({"error": f"User '{new_user}' already exists"}), 409
    
    sub_url = (os.getenv("SUPABASE_URL") or SUPABASE_URL).rstrip("/")
    sub_key = os.getenv("SUPABASE_KEY") or SUPABASE_KEY
    if sub_url and sub_key:
        requests.post(f"{sub_url}/rest/v1/app_users", json={"username": new_user, "is_admin": False, "can_rename": False}, headers=_supabase_headers(), timeout=5)
    
    _auth.ALLOWED_USERS.add(new_user)
    return jsonify({"ok": True, "added": new_user, "users": sorted(list(_auth.ALLOWED_USERS))})


@app.route("/api/admin/users/remove", methods=["POST"])
def admin_remove_user():
    u = (request.args.get("user") or request.form.get("user") or "").strip().lower()
    pwd = (request.args.get("pwd") or request.form.get("pwd") or "").strip()
    if u not in _auth.ADMIN_USERS or pwd != "500":
        return jsonify({"error": "Unauthorized"}), 401
    target = (request.args.get("name") or request.form.get("name") or "").strip().lower()
    user_row = _db_get_user(target)
    if user_row and (user_row.get("can_rename") or user_row.get("is_admin")):
        return jsonify({"error": f"Cannot remove admin user '{target}'"}), 403
    if target not in _auth.ALLOWED_USERS:
        return jsonify({"error": f"User '{target}' not found"}), 404
    
    sub_url = (os.getenv("SUPABASE_URL") or SUPABASE_URL).rstrip("/")
    sub_key = os.getenv("SUPABASE_KEY") or SUPABASE_KEY
    if sub_url and sub_key:
        requests.delete(f"{sub_url}/rest/v1/app_users?username=eq.{target}", headers=_supabase_headers(), timeout=5)
    
    _auth.ALLOWED_USERS.discard(target)
    return jsonify({"ok": True, "removed": target, "users": sorted(list(_auth.ALLOWED_USERS))})


@app.route("/api/job/<jid>")
def job(jid):
    j = JOBS.get(jid)
    if not j:
        return jsonify({"status": "error", "error": "Unknown job"}), 404
    return jsonify(j)
