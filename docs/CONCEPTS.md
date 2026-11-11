# Concepts — har design decision ka "kyun"

Yeh doc tumhare liye hai, interviewer ke liye nahi. README English mein hai
kyunki woh public-facing hai. Yeh Hinglish mein hai kyunki concepts isi tarah
dimaag mein baithte hain.

Rule: **code padhne se pehle yeh padho, phir code kholo.** Har section ke end
mein file path diya hai.

---

## 0. Poore system ka ek line summary

> Kaam accept karna aur kaam karna — do alag processes. Beech mein ek queue.
> Baaki sab uss decoupling ki keemat chukane ke liye hai.

Woh "keemat" kya hai? Jab do alag processes ho jaate hain, toh teen naye sawaal
paida ho jaate hain jo monolith mein kabhi nahi the:

1. **Message kho gaya toh?** → at-least-once delivery (`acks_late`)
2. **Message do baar aa gaya toh?** → idempotency ledger
3. **DB commit hua par enqueue nahi (ya ulta)?** → transactional outbox

Is repo ka 80% code inhi teen sawaalon ka jawab hai. Baaki 20% actual business
logic hai. Yeh ratio yaad rakhna — interview mein jab koi poochhe "background
jobs mein mushkil kya hai", jawab yahi hai: **kaam karna aasaan hai, failure
handle karna mushkil hai.**

---

## 1. Producer kabhi kaam nahi karta

📄 `app/main.py`

FastAPI ke kisi bhi endpoint mein gateway call nahi hai. Kyun?

Request thread user ka hai. User wait kar raha hai. Agar tum request ke andar
gateway ko call karoge:

- User 3 second wait karega (best case)
- Gateway slow hua toh request timeout
- Aur sabse bura: tumhare paas limited worker threads hain. 10 slow requests =
  saare threads busy = **baaki har user ke liye site down**, chahe unka kaam
  fast ho.

Yeh aakhri wala point hi asli reason hai. Slow request sirf uss user ko affect
nahi karti — woh poore system ka capacity kha jaati hai.

**Interview line:** "Request threads ek shared, bounded resource hain. Slow work
ko unke andar rakhna matlab ek slow dependency se poora app down kar dena."

---

## 2. 202 Accepted, 201 Created nahi

📄 `app/main.py` → `create_payout`

Choti si baat lagti hai par yeh honesty ka sawaal hai.

- `201 Created` = "ho gaya"
- `202 Accepted` = "maine zimmedari le li, par abhi hua nahi"

Job queue mein doosra wala sach hai. Agar tum 201 bhejoge toh client sochega
kaam ho gaya aur uske hisaab se apna logic likhega — phir jab kaam fail hoga
client ko pata hi nahi chalega.

202 ke saath hamesha ek `poll_url` do. Client ka contract: "poll karo, ya
webhook ka wait karo."

---

## 3. Idempotency — is repo ka dil

📄 `app/services/idempotency.py`

### 3.1 Sabse pehle: naive version kyun galat hai

Har tutorial yeh likhta hai:

```python
if already_processed(key):   # SELECT
    return "duplicate"
do_the_work()                # ← yahan gap hai
mark_processed(key)          # INSERT
```

SELECT aur INSERT ke beech gap hai. Do workers ne ek hi redelivered message
uthaya:

```
Worker A: SELECT → "nahi mila"
Worker B: SELECT → "nahi mila"     ← dono ko same jawab mila
Worker A: paisa bhej diya
Worker B: paisa bhej diya          ← double payout
```

Testing mein kabhi nahi pakdega, kyunki ismein do worker aur unlucky timing
chahiye. Production mein busiest din pe pakdega.

### 3.2 Sahi version: INSERT pehle

Database se **sawaal mat poochho, usse faisla karwao.**

```python
try:
    db.add(ProcessedOperation(key=key))
    db.flush()          # UNIQUE constraint yahin check hoga
    return PROCEED      # main jeeta
except IntegrityError:
    return check_existing_row()   # koi aur jeeta
```

Gap khatam, kyunki gap hai hi nahi. UNIQUE constraint database ke andar
atomically evaluate hota hai — aur database hi woh akeli jagah hai jo **saare
workers ko ek saath dekh sakti hai.**

**Yeh line interview mein bolo:** "Idempotency application code mein enforce
nahi hoti, database constraint mein hoti hai. Application sirf constraint ke
result ko interpret karta hai."

### 3.3 Lease — jo tutorials nahi batate

"INSERT pehle" ek naya problem banata hai:

```
Worker A: claim kiya (in_progress)
Worker A: 💀 SIGKILL (OOM / node evict / docker kill)
--- ab hamesha ke liye ---
Har redelivery: "in_progress hai, skip karta hoon"
```

Payout ab **permanently atka** hai. Kabhi hua nahi, par hamesha ke liye
"ho-raha-hai" mark hai. Reliability code jo chupke se kaam kho de, woh bina
reliability code ke bhi bura hai.

Isliye claim ek **lease** hai, lock nahi — sirf bounded time ke liye trusted:

| Claim ki haalat | Faisla |
|---|---|
| `COMPLETED` | Asli duplicate. Skip karo, purana result wapas do. |
| `IN_PROGRESS`, same `task_id` | Yeh apna hi retry hai. Chalo. |
| `IN_PROGRESS`, lease expire | Purana owner mar chuka. Cheen lo. |
| `IN_PROGRESS`, lease fresh | Koi zinda worker kar raha hai. Haath mat lagao. |

Yeh SQL mein likha hua **distributed lock with timeout** hai.

### 3.4 Key kahan se aayegi — sabse silent bug

```python
def idempotency_key(reference: str) -> str:
    return f"payout:{reference}"
```

Key **business identifier** se banni chahiye. Agar tumne Celery ka `task_id`
ya koi fresh `uuid4()` use kar liya, toh har redelivery ek nayi key layegi,
ledger ko har baar naya kaam dikhega, aur payout do baar jaayega.

Code bilkul sahi dikhega. Tests pass ho jaayenge. Production mein har duplicate
seedha nikal jaayega.

**Test:** `tests/test_services.py::test_idempotency_key_derives_from_the_business_reference`

---

## 4. Transactional Outbox

📄 `app/services/outbox.py`, `app/tasks/periodic.py`

### 4.1 Problem

```python
db.commit()                      # PostgreSQL
process_payout.delay(payout.id)  # Redis
```

Do alag systems, do alag commits, beech mein koi transaction nahi. Teen cheezein
galat ho sakti hain aur **do chupchaap hoti hain**:

1. **Commit ✔, enqueue ✘** → payout row hai, use process karne wala koi nahi.
   Paisa dena hai, hamesha ke liye pending.
2. **Enqueue ✔, commit ✘** → worker payout id 91 padhta hai, kuch milta nahi.
   Crash, retry, crash.
3. **Enqueue ✔, commit slow** → worker row ko commit hone se **pehle** padh
   leta hai. Yeh waala sabse gandaa heisenbug hai — kabhi-kabhi hota hai.

`try/except` se fix nahi hoga. Jo compensating write tum add karoge, woh bhi
fail ho sakti hai, aur phir uske liye compensating write chahiye.

### 4.2 Solution

Request se enqueue mat karo. **Enqueue ka IRAADA** usi DB mein, usi transaction
mein likho:

```python
with session_scope() as db:
    db.add(payout)
    record_event(db, "payout.requested", {...})
# ek hi commit decide karega — dono, ya koi nahi
```

Phir ek alag poller outbox padhkar asli enqueue karta hai.

### 4.3 `FOR UPDATE SKIP LOCKED`

```sql
SELECT * FROM outbox_events
WHERE status = 'pending'
ORDER BY id
FOR UPDATE SKIP LOCKED
LIMIT 50;
```

- `FOR UPDATE` → row-level write lock, transaction khatam hone tak
- `SKIP LOCKED` → row already locked hai? ruko mat, agli utha lo

`SKIP LOCKED` ke bina doosra poller pehle wale ki rows par **block** ho jaata —
parallelism zero, bas pollers ki line. Iske saath N pollers alag-alag batch le
jaate hain.

**Yeh ek clause hai jo "Postgres as a queue" ko possible banata hai.** Jab
interviewer poochhe "bina Redis ke queue kaise banaoge?" — jawab yahi hai.

### 4.4 Outbox ki imaandaar keemat

- **Latency** — job ek poll interval baad shuru hoga (yahan 5s)
- **Exactly-once nahi milta** — poller enqueue karke, `published` mark karne se
  pehle crash ho sakta hai → agle tick pe dobara enqueue

Matlab outbox bhi **at-least-once** hi deta hai. Theek hai — kyunki consumer
idempotent hai.

> **Har reliability layer at-least-once deta hai. Exactly-once hamesha consumer
> ki idempotency se aata hai, kabhi delivery se nahi.**

Yeh line ratt lo.

---

## 5. Delivery guarantees

📄 `app/celery_app.py`

```python
task_acks_late=True
task_reject_on_worker_lost=True
```

`acks_late=False` (default) → worker message uthate hi ack kar deta hai. Crash
hua toh **kaam gaya**.

`acks_late=True` → kaam khatam hone ke **baad** ack. Crash hua toh broker ko ack
mila hi nahi, woh kisi aur ko redeliver kar dega. **Kaam kabhi nahi khota, par
do baar chal sakta hai.**

Teesra option hai hi nahi.

### Exactly-once kyun impossible hai

Sender message bhejta hai. Receiver kaam karta hai. Receiver ack bhejta hai.
**Ack network par kho jaata hai.**

Ab sender ke paas do possibilities hain jo bilkul ek jaisi dikhti hain:
- receiver ne kaam kiya, ack kho gaya
- receiver ne kaam hi nahi kiya

Sender inme farq **kar hi nahi sakta**. Toh woh dobara bhejta hai. Isliye
consumer ko idempotent hona hi padega.

**Interview answer:** "Exactly-once delivery ek myth hai kyunki ack kho sakta
hai aur sender ke paas 'kaam hua par ack kho gaya' aur 'kaam hi nahi hua' mein
farq karne ka koi tareeka nahi. Practically hum at-least-once delivery +
idempotent consumer se effectively exactly-once haasil karte hain."

---

## 6. Visibility timeout — Redis ka chupa hua trap

📄 `app/config.py` → `visibility_timeout`

Redis asli message broker nahi hai (RabbitMQ ki tarah AMQP ack nahi hai). Toh
Celery ack ko **emulate** karta hai:

```
Worker message uthata hai → message N second ke liye "invisible"
N second mein ack nahi aaya → message wapas visible → koi aur utha lega
```

**Trap:** tumhara task 10 minute leta hai, visibility timeout 5 minute hai.

```
0:00  Worker A ne uthaya
5:00  message wapas visible → Worker B ne uthaya    ← A abhi bhi chal raha hai!
10:00 A ne payout kar diya
15:00 B ne payout kar diya                          ← double payout
```

Koi error nahi, koi log nahi, koi alert nahi. Bas paisa do baar gaya.

**Rule: visibility timeout > sabse lambe task se, aaram se.** Yahan 3600s hai
jabki sabse lamba task ~30s ka hai.

---

## 7. Retry, backoff, jitter

📄 `app/tasks/payouts.py`

```python
autoretry_for=(TransientGatewayError,),
retry_backoff=True,        # 1s, 2s, 4s, 8s...
retry_backoff_max=600,
retry_jitter=True,
max_retries=5,
```

**Backoff kyun:** turant retry karoge toh jo service abhi girri hai, usse aur
maaroge. Har baar zyaada ruko.

**Jitter kyun:** yeh sabse zyada miss kiya jaane wala part hai. Socho 500 tasks
ek saath fail hue (service down thi). Bina jitter ke, 500 ke 500 **exactly 1
second baad ek saath** retry karenge. Service abhi-abhi uthi thi — dobara gir
gayi. Phir 2 second baad phir 500 ek saath. Yeh **thundering herd** hai.

Jitter = har retry mein thoda random delay → herd bikhar jaata hai.

### Transient vs Permanent — yeh classification hi sab kuch hai

📄 `app/services/gateway.py`

| | Matlab | Kya karo |
|---|---|---|
| **Transient** | timeout, 503, rate limit | Retry — baad mein shayad chal jaaye |
| **Permanent** | account band, amount invalid | DLQ — dobara bhi wahi fail hoga |

Permanent ko transient samjhoge → 5 bekaar retries, aur human ko 5 backoff
interval baad pata chalega.
Transient ko permanent samjhoge → jo payment ho jaata, usko DLQ mein daal diya.

Isiliye code mein yeh **do alag exception classes** hain, ek `error_code`
string nahi. Type system se decision automatic ho jaata hai.

---

## 8. Dead Letter Queue

📄 `app/services/dlq.py`

Poison message = woh task jo **hamesha** fail hoga (corrupt data, permanent
bug, band account).

Do galat raaste:

1. **Infinite retry** → har cycle mein ek worker slot khaata hai, peeche ki
   queue dheere-dheere bhookhi marti hai. Aur alert kabhi nahi bajta kyunki
   technically abhi tak "fail" nahi hua.
2. **Exception nigal jaana** → job gayab. Payments mein iska matlab hai paisa
   jo ledger se nikla par kisi ke account mein nahi gaya, aur koi record nahi.

Sahi raasta: **park karo**, poore context ke saath (exact arguments, error,
traceback), aur **replay possible rakho**.

> DLQ jisse replay na ho sake, woh queue nahi kabristan hai.

`POST /dlq/{id}/replay` isi liye hai. Replay karte waqt purana idempotency claim
delete karna padta hai — warna replay chupchaap no-op ho jaayega (kyunki claim
`COMPLETED` mark hai). Yeh detail `app/tasks/payouts.py::replay_dead_letter`
mein hai.

---

## 9. Queue routing — starvation

📄 `app/celery_app.py` → `task_routes`, `docker-compose.yml`

Ek hi queue mein 10-minute ka report aur 1-second ka receipt? Receipt report ke
**peeche** khada rahega. User ko lagega "email system toota hai" — jabki email
system bilkul theek hai, bas line mein khada hai.

Fix priority nahi hai. **Alag queue + alag worker** hai:

```
payouts queue  → worker-payouts (concurrency 4)
fast queue     → worker-fast    (concurrency 8)
heavy queue    → worker-heavy   (concurrency 2)
```

Ab heavy lane jam ho bhi jaaye, fast lane chalti rehti hai.

**Note:** `worker-payouts` ki concurrency jaan-boojh kar kam hai. Yeh
performance decision nahi, **business decision** hai — payment partner ko 32
parallel request maarne se tum rate-limit ya block ho jaaoge.

---

## 10. Concurrency pool — prefork vs threads vs gevent

| Pool | Kya hai | Kiske liye |
|---|---|---|
| `prefork` | alag OS processes | **CPU-bound** — report, PDF, QR, encoding |
| `threads` | ek process, kai threads | I/O-bound, GIL release karne wali libs |
| `gevent` | green threads | bahut saare I/O calls (hazaar API calls) |
| `asyncio` (arq) | native event loop | modern async I/O apps |

**Samajhne wala rule:**

- **CPU-bound** → `prefork`, concurrency ≈ CPU cores. Kyunki GIL ki wajah se
  threads se asli parallelism milta hi nahi.
- **I/O-bound** → threads/gevent/async, concurrency bahut zyada ho sakti hai.
  Kyunki threads zyadatar **wait** kar rahe hain, aur waiting mein GIL koi
  problem nahi.

**Interview line:** "CPU-bound ke liye processes, I/O-bound ke liye green
threads ya async — kyunki I/O mein hum wait time overlap kar rahe hain, compute
nahi."

⚠️ **Windows:** `prefork` chalta hi nahi (`fork(2)` nahi hai). Local pe
`--pool=solo` use karo. Docker mein Linux hai toh prefork normal chalega.

---

## 11. Beat — aur do beat ka disaster

📄 `app/celery_app.py` → `beat_schedule`

Beat kaam **nahi** karta. Woh sirf ghadi dekhkar enqueue karta hai — timer wala
producer hai.

Isiliye do beat process = har periodic task **do baar** enqueue:
- reconciliation do baar
- stuck payouts do baar retry
- purge do baar

**Rule: poore system mein exactly ek beat.** Kubernetes mein `replicas: 1`, ya
leader-election lock.

Compose mein `deploy: replicas: 1` isi liye likha hai, aur comment mein warning
bhi.

### `expires` option

```python
"schedule": 5.0,
"options": {"expires": 30},
```

Agar beat ne 30 second pehle yeh enqueue kiya aur abhi tak kisi worker ne nahi
uthaya, toh phenk do. Iske bina: worker 10 minute down raha → wapas aate hi 120
purane tick tasks ek saath chalenge. Bekaar.

---

## 12. Defence in depth — worker mar gaya toh

Ek hi failure ke liye **teen independent** recovery:

| Mechanism | Kitni der mein | File |
|---|---|---|
| `acks_late` broker redelivery | visibility timeout (1h) | `celery_app.py` |
| `sweep_stuck_payouts` (beat) | 60s | `tasks/periodic.py` |
| nightly reconciliation | 24h | `tasks/periodic.py` |

Teeno ek hi payout ke liye fire ho sakte hain. **Koi problem nahi** — idempotency
ledger sab sambhaal lega.

> Har layer maan ke chalti hai ki baaki layers duplicate kar sakti hain.

Yeh design philosophy hai: overlapping safety nets, aur unka overlap safe isliye
hai kyunki neeche idempotency hai.

---

## 13. Result backend — kab store karo, kab nahi

📄 `app/tasks/notifications.py` → `ignore_result=True`

- **Store karo** — jab koi poll karega ("report ready?"), ya downstream task ko
  result chahiye (chord!)
- **Store MAT karo** — pure fire-and-forget. Email bhejne ka result kaun
  padhega? Koi nahi. Par Redis usko TTL ke saath likhta rahega.

Har email ke liye ek Redis key, 1 ghante ka TTL, aur **uska akela reader expiry
timer hai.** Busy system pe yeh gigabytes ban jaata hai.

`result_expires=3600` doosri safety hai — jo results store hote hain woh bhi
hamesha ke liye nahi rehte.

### `PENDING` ka matlab "queued" nahi hai

📄 `app/main.py` → `read_task`

Celery ke result backend ko task ka pata tab chalta hai jab koi worker use
**chhuta** hai. Toh:

- galat/typo task id → `PENDING`
- abhi tak start nahi hua task → `PENDING`

Dono **bilkul ek jaise** dikhte hain. Galat id daaloge toh hamesha PENDING poll
karte rahoge.

**Isiliye is system mein asli state `payouts.status` column mein hai, Celery
state mein nahi.** Celery state progress dikhane ke liye theek hai; **source of
truth tumhara database hai.**

---

## 14. Chord — fan-out / fan-in

📄 `app/tasks/reports.py`

```python
chord([summarise_chunk.s(c) for c in chunks])(build_statement.s())
```

- **fan-out**: N tasks parallel chalte hain
- **fan-in**: sabse aakhri wala khatam hote hi callback ko saare results milte
  hain

Valuable part **synchronization** hai. "Saare N khatam ho gaye?" — yeh khud
track karoge toh Redis mein counter rakhoge aur race condition galat karoge.
Chord yeh kaam kar deta hai.

**Catch (interview mein poochha jaata hai):** chord ko **result backend chahiye
hi chahiye**, kyunki callback ka input group ke stored results hain. Matlab
fan-in aur `ignore_result=True` ek saath possible nahi hain.

`summarise_chunk` chhota dict return karta hai, rows nahi — warna poori table
Redis se do baar guzregi.

---

## 15. Prefetch multiplier

📄 `app/celery_app.py` → `worker_prefetch_multiplier=1`

Prefetch = worker pehle se kitne message reserve kar leta hai.

Default (4 per slot) hazaaron millisecond-wale tasks ke liye tuned hai. Lambe
tasks ke saath yeh **ulta nuksaan** karta hai:

```
Worker A: 4 lambe tasks reserve kar liye, 1 pe kaam kar raha hai
Worker B: khaali baitha hai
Worker C: khaali baitha hai
```

`=1` matlab: ek uthao, khatam karo, agla uthao. Fair distribution.

Bonus: isse `/stats` ka queue depth bhi **imaandaar** ho jaata hai, kyunki
reserved-but-not-started messages queue se gayab nahi hote.

---

## 16. Pass IDs, not objects

📄 `app/tasks/payouts.py` → `process_payout(payout_id, reference)`

```python
send_invoice.delay(user)          # ❌
send_invoice.delay(user_id=u.id)  # ✅
```

**Do reasons, dono important:**

1. **Payload size** — broker har queued message memory mein rakhta hai.
   Serialized ORM object = kilobytes. Ek int = bytes. 100k queued jobs pe yeh
   healthy Redis aur OOM kill ka farq hai.
2. **Staleness** — job queue mein minute bhar baith sakta hai. Enqueue time pe
   serialize kiya object **ateet ka snapshot** hai. Task ke andar id se fetch
   karoge toh hamesha current state milega.

---

## 17. JSON serializer, pickle nahi

📄 `app/celery_app.py`

```python
task_serializer="json"
accept_content=["json"]
```

Pickle deserialize karte waqt **arbitrary code execute** kar sakta hai. Jo bhi
tumhare broker mein message likh sakta hai, usko har worker pe **remote code
execution** mil jaata hai.

Redis default mein bina password ke chalta hai. Ek misconfigured firewall aur
tumhara poora fleet gaya.

JSON ki keemat sirf discipline hai: arguments plain types hone chahiye (int,
str, list, dict). Jo tum waise bhi chahte the — "pass IDs, not objects".

---

## 18. Testing — teen levels, aur eager mode ka jhooth

📄 `tests/`

| Level | Kya pakadta hai | Speed |
|---|---|---|
| Plain function tests | business logic bugs | milliseconds |
| Eager mode | wiring, end state | fast |
| Real broker (testcontainers) | concurrency, redelivery, serialization | slow |

Is repo mein pehle do hain. Teesre ke liye `docker compose up` karke manually
demo karo.

### Eager mode kya NAHI pakadta

`task_always_eager=True` task ko **synchronously, usi process mein** chalata
hai. Matlab yeh **nahi** pakad sakta:

- concurrency bugs (do worker ek saath)
- redelivery behaviour
- serialization errors (kyunki kuch serialize hota hi nahi!)
- asli retry timing

**"Humne task_always_eager se test kiya" ek aisa vaakya hai jo coverage jaisa
sunai deta hai par coverage hai nahi.**

Iska live example is repo mein hai:
`tests/test_tasks.py::test_transient_failure_retries_then_succeeds` — eager mode
mein retry chain sirf tab chalti hai jab `task_eager_propagates=False` ho.
Matlab eager mode retry semantics ko theek se model karta hi nahi. Test ke
docstring mein yeh likha hai.

---

## 19. Graceful shutdown

Deploy ke waqt worker ko `SIGTERM` milta hai. Celery **warm shutdown** karta
hai: naye task lena band, current task poora karke band.

Yeh payments mein critical hai — paisa transfer hone ke beech worker mat maaro.

Kubernetes mein `terminationGracePeriodSeconds` ko apne sabse lambe task se
bada rakho, warna k8s `SIGKILL` bhej dega aur warm shutdown ka koi matlab nahi
rahega.

(Waise, humare paas `acks_late` + sweeper hai, toh kaam khoyega nahi — bas
duplicate ka risk le lenge. Defence in depth ka faayda.)

---

## 20. `max_tasks_per_child`

📄 `app/celery_app.py` → `worker_max_tasks_per_child=100`

Har 100 task ke baad worker child process restart.

Python libraries memory leak karti hain — khaaskar woh jo C wrap karti hain
(imaging, PDF, crypto). Leak dhoondhne ke bajaye uski **umar bound kar do**.

Yeh workaround nahi, standard production hygiene hai.

---

## 21. Validation edge pe, queue mein nahi

📄 `app/schemas.py`

Galat request → user ke saamne **turant, loudly** fail ho, jo usse theek kar
sakta hai.

Galat message → 20 minute baad worker ke andar **chupchaap** fail hoga, 5 baar
retry hoga, DLQ mein jaayega, aur kisi ko triage karna padega.

**Queue typo dhoondhne ki jagah nahi hai.** Pydantic edge pe hi rok deta hai.

Isiliye `amount_paise` int hai, float nahi. Float lene ka matlab 199.99 ka
`199.98999999999998` ban jaana — aur uske baad rounding wali baat finance team
ke saath nahi karni.

---

## 22. Chhoti cheezein jo dhyaan deni chahiye

| Cheez | Kyun | Kahan |
|---|---|---|
| `pool_pre_ping=True` | raat bhar idle worker ki dead connections | `db.py` |
| Session slow call se pehle band | connection pool exhaustion | `notifications.py` |
| Explicit `name="payouts.process"` | function rename se purane messages orphan na ho | `tasks/payouts.py` |
| `include=[...]` | warna "Received unregistered task" | `celery_app.py` |
| Outbox pe composite index | published rows badhne pe full scan | `models.py` |
| Docker healthcheck | `depends_on` sirf container start ka wait karta hai, DB ready ka nahi | `docker-compose.yml` |
| Redis `--appendonly yes` | restart pe queued jobs na khoyein | `docker-compose.yml` |
| `PrimaryKey = BigInteger().with_variant(Integer, "sqlite")` | SQLite sirf `INTEGER PRIMARY KEY` autoincrement karta hai | `models.py` |
| Non-root Docker user | queue message se code chalta hai — blast radius chhota rakho | `Dockerfile` |

---

## 23. Aage kya

Is project ko aur gehra karna ho toh, priority order mein:

1. **Alembic migrations** — `create_all()` hata do. Schema change karke dekho.
2. **Testcontainers** — asli Redis + Postgres ke saath ek integration test.
   Do worker chala ke sach mein duplicate delivery test karo.
3. **Prometheus metrics** — queue depth, task latency histogram, failure rate.
   Grafana dashboard. Backlog ke **slope** pe alert.
4. **Saga pattern** — multi-step payout (reserve → transfer → notify) with
   compensating actions.
5. **arq version** — wahi system async-native tools se. Dono ka comparison
   README mein likho — yeh interview mein bahut strong signal hai.

---

## Aakhri baat

Har cheez jo yahan hai, ek line mein:

> **Kaam accept karna aur kaam karna alag karo (queue). Phir har us cheez ko
> sambhaalo jo tab tootegi jab dono alag processes hain.**

Baaki sab detail hai.
