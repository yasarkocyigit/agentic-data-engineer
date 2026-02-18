# 🧠 Project Memory — Known Issues & Solutions

> Bu dosya projede karşılaşılan hataları ve çözümlerini loglar.  
> Aynı sorunu tekrar yaşadığında buraya bak.

---

## 1. Airflow Web UI'da DAG'lar Görünmüyor ("0 Dags")

**Tarih:** 2026-02-16  
**Belirtiler:** Airflow UI'a login olunuyor ama DAG listesi boş. CLI'da `airflow dags list` ile DAG görünüyor.

**Root Cause:**  
Airflow 3'te DAG'lar **"DAG Bundles"** üzerinden yönetiliyor. `airflow-webserver` container'ı yeniden oluşturulduğunda, `scheduler` ve `dag-processor` eski bundle version cache'ini kullanmaya devam ediyor. API server güncel bundle bilgisini alamıyor.

**Çözüm (Manuel):**
```bash
docker compose restart airflow-scheduler airflow-dag-processor
```
Ardından Airflow UI'ı yenile (`Cmd+Shift+R`).

**Kalıcı Çözüm (Auto-Recovery):**  
`/api/airflow/route.ts` → `list_dags` 0 DAG döndüğünde otomatik olarak `docker compose restart airflow-scheduler airflow-dag-processor` çalıştırır, 8s bekler ve retry eder. 60s cooldown ile restart loop engellenir.

---

## 2. Airflow 3.x Login Sorunu ("Invalid Credentials" / 401)

**Tarih:** 2026-02-16  
**Belirtiler:** `http://localhost:8081` login sayfasında admin/admin ile giriş yapılamıyor, hata mesajı yok.

**Root Cause:**  
Airflow 3'te `SimpleAuthManager` config formatı `username:ROLE` (şifre DEĞİL). Şifreler ayrı bir JSON dosyasında tutulur ve otomatik generate edilir.

**Doğru Konfigürasyon (docker-compose.yml):**
```yaml
- AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS=admin:ADMIN
- AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE=/opt/airflow/config/simple_auth_manager_passwords.json
```

**Şifre Dosyası (`config/simple_auth_manager_passwords.json`):**
```json
{"admin": "admin"}
```

> ⚠️ `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True` kullanma — tüm kullanıcıları "Anonymous" yapar ve DAG'lar API'dan görünmez olur!

---

## 3. Airflow REST API v2 Authentication Sorunu

**Tarih:** 2026-02-16  
**Belirtiler:** HTTP Basic Auth ile `/api/v2/dags` endpoint'ine erişilemiyor. `"Not authenticated"` hatası.

**Root Cause:**  
Airflow 3'te eski `webserver` komutu kaldırıldı, yerini `api-server` (FastAPI) aldı. API artık JWT token-based auth kullanıyor, Basic Auth desteklemiyor.

**Workaround — CLI Üzerinden Entegrasyon:**  
Next.js API route'u (`/api/airflow`) `docker exec` ile CLI komutları çalıştırıyor:
```bash
docker exec airflow_webserver airflow dags list -o json    # DAG listesi
docker exec airflow_webserver airflow dags trigger <dag_id> # Trigger
docker exec airflow_webserver airflow dags pause <dag_id>   # Pause
docker exec airflow_webserver airflow dags unpause <dag_id> # Unpause
```

**Alternatif — Token ile API Erişimi:**
```bash
# 1. Token al
TOKEN=$(curl -s http://localhost:8081/auth/token \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"admin"}' | python3 -c 'import sys,json; print(json.load(sys.stdin)["access_token"])')

# 2. API çağrısı
curl -s http://localhost:8081/api/v2/dags -H "Authorization: Bearer $TOKEN"
```

---

## 4. `airflow users create` Komutu Çalışmıyor

**Tarih:** 2026-02-16  
**Belirtiler:** `airflow users create --username admin ...` → `invalid choice: 'users'`

**Root Cause:**  
Airflow 3.0.0'da `users` CLI komutu tamamen kaldırıldı. Kullanıcı yönetimi `SimpleAuthManager` config'i ile yapılıyor.

**Çözüm:**  
`docker-compose.yml`'de env var ile tanımla:
```yaml
- AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS=admin:ADMIN
```

---

## Quick Reference — Container'lar

| Container | Komut | Port | Amacı |
|---|---|---|---|
| `airflow_webserver` | `api-server` | 8081 | Web UI + REST API |
| `airflow_scheduler` | `scheduler` | — | DAG'ları zamanlar |
| `airflow_dag_processor` | `dag-processor` | — | DAG dosyalarını parse eder |
| `airflow_init` | `db migrate` | — | Başlangıçta DB'yi kurar |

## Quick Reference — Portlar

| Servis | Port |
|---|---|
| Airflow API Server | `http://localhost:8081` |
| Web UI (Next.js) | `http://localhost:3010` |
| Trino | `http://localhost:8083` |
| MinIO | `http://localhost:9001` |
| Spark Master | `http://localhost:8082` |
| Gitea (Git + CI/CD) | `http://localhost:3030` |
| Gitea SSH | `ssh://localhost:2222` |
| Superset | `http://localhost:8089` |
| Marquez Web | `http://localhost:8085` |
