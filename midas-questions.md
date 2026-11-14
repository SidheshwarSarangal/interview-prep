# Midas — Interview Question Bank (Blinkit Backend Intern, Round 1: Project + DSA)

> Project: `~/Desktop/midas` — Spring Boot + Kafka + JPA/H2 + REST transaction processor, built for the JPMorganChase SWE Job Simulation (Forage).
> Source code was NOT modified. Every claim below comes from the files; anything I couldn't confirm from code is marked **[Confirm: …]**.

## ⭐ Top 25 most likely questions (ranked)

Rank = how likely × how much it hurts if you fumble it. Learn these cold; each links to the full answer.

| # | Question (as they'd say it) | 1-line core of your answer | Full answer |
|---|---|---|---|
| 1 | "Tell me about this project." | Kafka consumer → validate → incentive API with timeouts and fallback → atomic debit/credit/audit in JPA → `GET /balance`. 16 tests and CI. Know the gaps. | 1.1 |
| 2 | "A message lands on the topic. Walk me through exactly what happens." | Deserializer → listener → `@Transactional` proxy → validate → load → funds check → HTTP incentive → dirty-checked UPDATEs + INSERT → commit → offset commit. | 1.2, Step 1 |
| 3 | "What did *you* write vs what Forage gave you?" | Honest split. Your extras: error handling, timeouts and fallback, BigDecimal, stand-in API, tests, CI. | 1.3 |
| 4 | "Why Kafka? Why not just REST or RabbitMQ?" | Decoupling, buffering, replay, per-partition ordering, consumer groups. A REST + 202 hybrid is a valid alternative. | 3.1, 2.1, 12.5 |
| 5 | "What's your delivery guarantee?" | At-least-once: the offset is committed after the DB commit. Kafka EOS doesn't cover the DB, so the consumer must be idempotent. | 7.10, 12.10 |
| 6 | "Same message arrives twice — what happens? Fix it." | Money moves twice today. Add a `transactionId` + UNIQUE constraint in the same DB transaction, and treat a duplicate as already done. | 8.5, 8.6, W1 |
| 7 | "Two transfers from the same sender at once — can the balance break?" | Lost update / double spend if run in parallel. Safe today only because there's 1 partition and 1 thread. | 8.1 |
| 8 | "How do you fix that? Optimistic or pessimistic?" | Conditional `UPDATE … WHERE balance >= amt`, or `FOR UPDATE` with id-ordered locking, or `@Version` + retry. Choose by contention. Add `CHECK (balance >= 0)`. | 8.2, 8.3, 12.9 |
| 9 | "How does `@Transactional` work? When does it roll back?" | CGLIB proxy + ThreadLocal EntityManager. Rolls back on unchecked exceptions only. Self-invocation pitfall. Rejections return, they don't throw. | 7.2, 16.3 |
| 10 | "Where do you save the updated balance? I don't see `save(sender)`." | Managed entities + dirty checking → UPDATE at flush. Only the new `TransactionRecord` needs `save`. | 7.4, 7.5 |
| 11 | "Your HTTP call is inside the DB transaction — why?" | Anti-pattern: it holds a pooled connection for up to 7s. Call the API before the transaction, or make incentives async via an outbox. | 2.4, W2, 10.10 |
| 12 | "The Incentive API is down / slow. What happens?" | Down: fall back to 0 after 2s. Slow is worse: ~5s per message and lag grows. Add a circuit breaker, tighter timeout, async. | 9.1, 9.2, 12.4 |
| 13 | "The DB goes down for 30 seconds." | 2 retries × 500ms, then skip, so transfers are lost. Fix: exponential backoff + DLT + alerts. | 9.3, 7.13 |
| 14 | "Explain partitions, offsets, consumer groups — using your project." | 1 partition = 1 active consumer. Offsets live in `__consumer_offsets`. `earliest` only applies with no committed offset. | 7.9, 16.13, 16.16 |
| 15 | "What's a poison message and how do you handle it?" | `ErrorHandlingDeserializer` + `DefaultErrorHandler`. A DeserializationException is skipped without retry. Admit it's not tested. | 7.11, 9.6, 11.3 |
| 16 | "Why BigDecimal? Why `compareTo`, not `equals`?" | double can't represent 0.1. `equals` checks scale. String constructor. HALF_EVEN. Validate scale ≤ 2. | 3.6, 16.2, 16.25, 4.12 |
| 17 | "Why H2? What would you use in production?" | Demo only: in-memory + `create-drop` lose data. Postgres + Flyway + Testcontainers. Know the restart/offset inconsistency. | 3.2, 4.9, 9.13 |
| 18 | "What indexes do you have / need?" | PKs + unique(name). Add `(sender_id, processed_at)` and `(recipient_id, processed_at)`. Postgres doesn't auto-index FKs. | 4.3, 15.3 |
| 19 | "1,000 transfers/sec — what breaks first?" | Single partition/thread → correctness under parallelism → incentive latency → H2 → DB writes → hot accounts → reads. | 10.1, 10.5, 10.7 |
| 20 | "Would you cache balances in Redis?" | Display only, cache-aside, invalidate after commit, short TTL. Never use the cache for funds checks. Try replicas first. | 10.2, 10.3 |
| 21 | "How is `/balance` secured?" | It isn't: IDOR. Spring Security JWT, `/me/balance`. Also Kafka is unauthenticated and the H2 console is exposed. | 6.1, 6.6, 6.7 |
| 22 | "Unknown user returns 200 with 0? Status codes?" | Agree it's wrong (spec-driven). 404 + ProblemDetail via `@RestControllerAdvice`. Missing param → 400. | 5.3, 5.2, 14.4 |
| 23 | "How did you test it? What's NOT tested?" | Slices: WebMvc, DataJpa, RestClient, plus embedded-Kafka E2E. Not tested: poison message, timeout, concurrency, duplicates. | 11.1, 11.3, 17.7 |
| 24 | "Blinkit: last packet in stock, two checkouts at once." | Same as double spend: `UPDATE inventory SET available = available - q WHERE … AND available >= q`. Reserve → confirm → release. | 13.2, 13.1, 13.3 |
| 25 | "Phase 2 — what would you change first?" | Ranked by risk to money: idempotency + atomic debit → Postgres → DLT → async incentive → security → observability. | 12.1, 12.2 |

**Also be ready to:** trace the README sample batch (17.3); explain `Long.equals` vs `==` (16.1); write the atomic transfer (14.7) and an LRU cache (14.14); walk through "what happens when you `curl …/balance`" (15.15).

**Personal answers** (role, hardest bug, dates, AI use, lessons, `com.jpmc`): drafted in **"Your story — drafted personal answers"** at the end of this file (P.1–P.6).

**Do before the interview:**
1. **The local repo has no commits** (1.8). Push it with an honest history.
2. Settle the answer to "what did Forage provide?" (1.3).
3. Resolve the **[Confirm: …]** tags. Run `mvn dependency:tree`, and run once with `spring.jpa.show-sql=true` to see the real SQL (7.5).
4. Practise the 17.3 sample-batch trace and the 8.1 double-spend timeline out loud.

---

## Path aliases used in this document

| Alias | File (relative to `midas/`) |
|---|---|
| **TS** | `midas-core/src/main/java/com/jpmc/midascore/service/TransactionService.java` |
| **BS** | `midas-core/src/main/java/com/jpmc/midascore/service/BalanceService.java` |
| **BC** | `midas-core/src/main/java/com/jpmc/midascore/controller/BalanceController.java` |
| **TL** | `midas-core/src/main/java/com/jpmc/midascore/kafka/TransactionListener.java` |
| **IC** | `midas-core/src/main/java/com/jpmc/midascore/client/IncentiveClient.java` |
| **KC** | `midas-core/src/main/java/com/jpmc/midascore/config/KafkaConfig.java` |
| **DDL** | `midas-core/src/main/java/com/jpmc/midascore/config/DemoDataLoader.java` |
| **UR** | `midas-core/src/main/java/com/jpmc/midascore/entity/UserRecord.java` |
| **TR** | `midas-core/src/main/java/com/jpmc/midascore/entity/TransactionRecord.java` |
| **URepo / TRRepo** | `midas-core/.../repository/UserRepository.java` / `TransactionRecordRepository.java` |
| **core.yml** | `midas-core/src/main/resources/application.yml` |
| **IAC** | `incentive-api/src/main/java/com/jpmc/incentiveapi/IncentiveController.java` |

`TS:49` means TransactionService.java line 49.

---

# STEP 0 — Coverage inventory

## 0.1 Every file read (34 files, ~1,250 lines total)

| # | Path | Purpose |
|---|---|---|
| 1 | `README.md` | Project overview, architecture diagram (mermaid), run steps, config table, test list, design notes, next steps |
| 2 | `pom.xml` | Maven parent POM. Parent is `spring-boot-starter-parent 3.2.5`, Java 17, modules `midas-core` and `incentive-api` |
| 3 | `midas-core/pom.xml` | Deps: starter-web, starter-data-jpa, **starter-validation (unused)**, spring-kafka, h2 (runtime); test: starter-test, spring-kafka-test, awaitility |
| 4 | `incentive-api/pom.xml` | Deps: starter-web, starter-test |
| 5 | `docker-compose.yml` | Single-node Kafka `apache/kafka:3.7.0` in KRaft mode (broker+controller), port 9092, replication factor 1 |
| 6 | `.github/workflows/ci.yml` | GitHub Actions: on push to `main` and on PRs, Temurin 17 with Maven cache, `mvn -B verify` |
| 7 | `.gitignore` | Ignores target/, *.class, *.log, IDE files |
| 8 | `midas-core/src/main/resources/application.yml` | Port 33400, topic, incentive URL, demo-data flag, Kafka consumer/producer serde, H2 datasource, `ddl-auto: create-drop`, `open-in-view: false`, H2 console on |
| 9 | `incentive-api/src/main/resources/application.yml` | Port 8080, app name |
| 10 | `scripts/send-transactions.sh` | `docker exec` into the Kafka container and pipe a JSONL file into `kafka-console-producer.sh` |
| 11 | `scripts/check-balances.sh` | `curl` GET /balance for users 1..5 |
| 12 | `scripts/sample-transactions.jsonl` | 6 sample messages: 3 valid, 3 rejected (insufficient funds, unknown recipient 99, negative amount) |
| 13 | `MidasCoreApplication.java` | `@SpringBootApplication` entry point for midas-core |
| 14 | **TL** `kafka/TransactionListener.java` | `@KafkaListener` on `${general.kafka-topic}` that calls `TransactionService.process` |
| 15 | **TS** `service/TransactionService.java` | `@Transactional process()`: validate, load users, funds check, incentive call, debit/credit, save audit record |
| 16 | **BS** `service/BalanceService.java` | `@Transactional(readOnly=true) balanceFor()`: returns 0 for an unknown user |
| 17 | **BC** `controller/BalanceController.java` | `GET /balance?userId=` |
| 18 | **IC** `client/IncentiveClient.java` | `RestTemplate` POST to the Incentive API; 2s connect / 5s read timeout; falls back to 0 on error, null or negative |
| 19 | **KC** `config/KafkaConfig.java` | `NewTopic` bean (1 partition, 1 replica); `DefaultErrorHandler(FixedBackOff(500ms, 2))` |
| 20 | **DDL** `config/DemoDataLoader.java` | `SmartInitializingSingleton` that seeds 5 users when the table is empty; `@ConditionalOnProperty` |
| 21 | **UR** `entity/UserRecord.java` | `@Entity users`: id, name (unique), balance; domain methods `debit/credit/canAfford` |
| 22 | **TR** `entity/TransactionRecord.java` | `@Entity transactions`: two LAZY `@ManyToOne` to users, amount, incentive, processedAt |
| 23 | `foundation/Transaction.java` | Record `(Long senderId, Long recipientId, BigDecimal amount)`: the Kafka message DTO |
| 24 | `foundation/Incentive.java` | Record `(BigDecimal amount)` plus `none()` factory |
| 25 | `foundation/Balance.java` | Record `(BigDecimal amount)`: the /balance response |
| 26 | **URepo** `repository/UserRepository.java` | `JpaRepository<UserRecord, Long>` plus `findByName` (**unused**) |
| 27 | **TRRepo** `repository/TransactionRecordRepository.java` | `JpaRepository<TransactionRecord, Long>` |
| 28 | `IncentiveApiApplication.java` | Entry point for the stand-in incentive service |
| 29 | **IAC** `IncentiveController.java` | `POST /incentive`: 2% for amounts ≥ 100.00 (HALF_EVEN, scale 2), otherwise 0 |
| 30 | `test/.../TransactionServiceTest.java` | 6 tests, `@SpringBootTest` + `@EmbeddedKafka`, `@MockBean IncentiveClient` |
| 31 | `test/.../KafkaIntegrationTest.java` | 1 end-to-end test: KafkaTemplate → embedded broker → listener → H2, using Awaitility |
| 32 | `test/.../IncentiveClientTest.java` | 3 tests, `@RestClientTest` + `MockRestServiceServer` |
| 33 | `test/.../BalanceControllerTest.java` / `BalanceServiceTest.java` | 2 tests each: `@WebMvcTest` + `@MockBean`; `@DataJpaTest` |
| 34 | `incentive-api/src/test/.../IncentiveControllerTest.java` | 2 tests, `@WebMvcTest` |

Test count check: 6 + 1 + 3 + 2 + 2 + 2 = **16**. This matches the README badge.

## 0.2 Every API route / entry point

| Kind | Method + path / topic | Auth? | Handler |
|---|---|---|---|
| REST | `GET /balance?userId={Long}` (midas-core, port 33400) | **None** | **BC** `getBalance` → **BS** `balanceFor` |
| REST | `POST /incentive` (incentive-api, port 8080) | **None** | **IAC** `incentive` |
| REST (dev tool) | `/h2-console` (midas-core, 33400) | H2 login `sa` with an empty password | Spring Boot H2 console auto-config (core.yml:44-47) |
| Kafka consumer | topic `midas-transactions`, group `midas-core` | **None** (PLAINTEXT) | **TL** `onTransaction` → **TS** `process` |
| Outbound HTTP | `POST ${incentive.api.url}` | None | **IC** `fetchIncentive` |
| Startup hook | `afterSingletonsInstantiated` | n/a | **DDL** |

## 0.3 Every DB model

**`users`** (UR)
| Field | Column | Constraints |
|---|---|---|
| id | `id BIGINT` | PK, `GenerationType.IDENTITY` |
| name | `name VARCHAR(255)` | `NOT NULL`, `UNIQUE` (this creates a unique index) |
| balance | `balance NUMERIC(19,2)` | `NOT NULL`. **No `CHECK (balance >= 0)`, no `@Version`** |

**`transactions`** (TR)
| Field | Column | Constraints |
|---|---|---|
| id | `id BIGINT` | PK, IDENTITY |
| sender | `sender_id BIGINT` | FK → users.id, `@ManyToOne(LAZY, optional=false)` |
| recipient | `recipient_id BIGINT` | FK → users.id, `@ManyToOne(LAZY, optional=false)` |
| amount | `NUMERIC(19,2)` | NOT NULL |
| incentive | `NUMERIC(19,2)` | NOT NULL |
| processedAt | `processed_at` (a timestamp type) | NOT NULL, set in the constructor with `Instant.now()` [Confirm the exact column type by turning on `spring.jpa.show-sql`] |

Relations: users 1—N transactions (as sender) and users 1—N transactions (as recipient). The mapping is unidirectional: `UserRecord` has no collections.
Indexes: PKs, plus a unique index on `users.name`. No explicit index on `sender_id`, `recipient_id` or `processed_at`. H2 creates indexes for FK columns automatically; **PostgreSQL does not**.
Schema management: Hibernate `ddl-auto: create-drop`. There are no migrations (no Flyway/Liquibase).

## 0.4 External services / libraries that matter

| Library / service | Where used |
|---|---|
| Spring Boot 3.2.5 (parent) | `pom.xml`. It manages all versions below. [Confirm resolved versions with `mvn dependency:tree`; from memory: Spring Framework 6.1.x, Spring Kafka 3.1.x, kafka-clients 3.6.x, Hibernate 6.4.x, H2 2.2.x] |
| Spring Web MVC (embedded Tomcat) | BC, IAC |
| Spring Data JPA / Hibernate | URepo, TRRepo, UR, TR, `@Transactional` in TS/BS |
| H2 in-memory DB | core.yml `jdbc:h2:mem:midas;DB_CLOSE_DELAY=-1` |
| Spring for Apache Kafka | TL (`@KafkaListener`), KC (`NewTopic`, `DefaultErrorHandler`), core.yml serde |
| `ErrorHandlingDeserializer` + `JsonDeserializer` | core.yml:25-29 |
| Apache Kafka broker 3.7.0 (KRaft) | docker-compose.yml |
| `RestTemplate` via `RestTemplateBuilder` | IC |
| Jackson | JSON for Kafka messages, REST bodies and records |
| SLF4J/Logback | TS, IC, TL, DDL |
| spring-boot-starter-validation | **Declared in midas-core/pom.xml:26-29 but never used** (no `@Valid`, `@NotNull` or `@Positive` anywhere) |
| JUnit 5, Mockito, AssertJ, MockMvc, MockRestServiceServer, @DataJpaTest, @EmbeddedKafka, Awaitility | tests |
| GitHub Actions, Temurin JDK 17 | ci.yml |

## 0.5 Every config / env variable

| Key | Value | Env override | Where |
|---|---|---|---|
| `server.port` | 33400 (core) / 8080 (incentive) | — | both application.yml |
| `general.kafka-topic` | `midas-transactions` | — (the script uses its own `TOPIC` env var) | core.yml:5, TL:22, KC:16 |
| `incentive.api.url` | `http://localhost:8080/incentive` | `INCENTIVE_API_URL` | core.yml:9, IC:28 |
| `midas.demo-data.enabled` | `true` | (Spring relaxed binding: `MIDAS_DEMODATA_ENABLED`) | core.yml:13, DDL:21 |
| `spring.kafka.bootstrap-servers` | `localhost:9092` | `KAFKA_BOOTSTRAP_SERVERS` | core.yml:20 |
| `spring.kafka.consumer.group-id` | `midas-core` | — | core.yml:22, TL:22 |
| `spring.kafka.consumer.auto-offset-reset` | `earliest` | — | core.yml:23 |
| consumer key/value deserializer | `StringDeserializer` / `ErrorHandlingDeserializer` → delegate `JsonDeserializer` | — | core.yml:24-27 |
| `spring.json.value.default.type` | `com.jpmc.midascore.foundation.Transaction` | — | core.yml:28 |
| `spring.json.use.type.headers` | `false` | — | core.yml:29 |
| producer serializers + `spring.json.add.type.headers: false` | String / JsonSerializer (only used by tests' `KafkaTemplate`) | — | core.yml:30-34 |
| `spring.datasource.*` | `jdbc:h2:mem:midas;DB_CLOSE_DELAY=-1`, user `sa`, empty password | — | core.yml:36-39 |
| `spring.jpa.hibernate.ddl-auto` | `create-drop` | — | core.yml:42 |
| `spring.jpa.open-in-view` | `false` | — | core.yml:43 |
| `spring.h2.console.enabled/path` | `true`, `/h2-console` | — | core.yml:45-47 |
| `logging.level.com.jpmc.midascore` | INFO | — | core.yml:51 |
| Hard-coded | RestTemplate timeouts 2s/5s (IC:30-31); retry backoff 500ms ×2 (KC:23); topic partitions 1 / replicas 1 (KC:17); incentive threshold 100.00 and rate 0.02 (IAC:18-19) | — | — |
| docker-compose | `KAFKA_NODE_ID=1`, `PROCESS_ROLES=broker,controller`, listeners 9092/9093, all RF/ISR = 1, `GROUP_INITIAL_REBALANCE_DELAY_MS=0` | — | docker-compose.yml |
| scripts | `TOPIC` (default midas-transactions), `$1` file | — | send-transactions.sh |

## 0.6 Every README claim (each needs a defensible answer)

| # | Claim | Verified? |
|---|---|---|
| R1 | Reads from Kafka, validates, gets incentive from REST API, saves with JPA/H2, serves balances over REST | ✅ code matches |
| R2 | Built in the JPMorganChase Forage simulation, completed Dec 2025 | Not verifiable from code. **The local git repo has zero commits**. [Confirm: is it pushed somewhere with history?] |
| R3 | Badges: Java 17, Boot 3.2.5, Kafka 3.7, 16 tests passing | ✅ Java/Boot/test count. "Kafka 3.7" is the **broker image**; the client lib comes from Boot's BOM [Confirm] |
| R4 | A malformed message is logged and skipped, so it never blocks the partition | ✅ ErrorHandlingDeserializer + DefaultErrorHandler. (DeserializationException is non-retryable by default, so it is skipped without retries) |
| R5 | Four rejection rules (missing/≤0, self-transfer, unknown user, insufficient balance) | ✅ TS:77-93 and TS:61 |
| R6 | Incentive API down means the transaction goes through with incentive 0 | ✅ IC:43-45 |
| R7 | Debit, credit and record are in a single `@Transactional` unit | ✅ TS:49. **But the HTTP call also sits inside that transaction** |
| R8 | Expected balances table after the sample batch | ✅ hand-traced: alice 850, bob 577.75, carol 325.25, dave 306, erin 1700 |
| R9 | `GET /balance?userId=1` → `{"amount":850.00}` | ✅ |
| R10 | H2 console at /h2-console | ✅ |
| R11 | Config table with env overrides | ✅ |
| R12 | Tests use embedded Kafka + in-memory H2, so Docker is not needed | ✅ |
| R13 | Money uses BigDecimal, precision 19 / scale 2; "no rounding errors" | ⚠️ BigDecimal ✅, but input scale is never validated (e.g. `10.005`). "No rounding errors" is an overclaim |
| R14 | 2s connect / 5s read timeout | ✅ IC:30-31 |
| R15 | DefaultErrorHandler: 2 retries, then skip | ✅ KC:23. **"Skip" means a valid money transfer is silently lost after a transient DB error** |
| R16 | Settings overridable by env vars | ✅ partially: only 2 vars use explicit `${…}` |
| R17 | Next steps: Postgres + Flyway + Testcontainers, idempotency, DLT, Micrometer, containerize | Acknowledged gaps |
| R18 | incentive-api is a stand-in with the same contract; ≥100 earns 2% | ✅ IAC |

---

# STEP 1 — Project understanding

**Purpose.** A Spring Boot service consumes JSON money-transfer events from Kafka, validates them, asks an external Incentive API for a bonus, updates both users' balances and writes an audit row atomically, and exposes `GET /balance`. It was the JPMC Forage "Midas" simulation. This repo is a cleaned-up, self-contained version that also includes a stand-in for the provided incentive JAR.

**Stack.** Java 17, Spring Boot 3.2.5 (Web MVC, Data JPA, Kafka), Hibernate, H2, Kafka 3.7 (KRaft, Docker), Maven multi-module, JUnit 5, Mockito, embedded Kafka, GitHub Actions.

**Folder structure (layered).** `kafka` (inbound adapter) → `service` (business logic + transactions) → `repository` (Spring Data) → `entity` (JPA). Plus `client` (outbound HTTP), `controller` (inbound HTTP), `foundation` (DTO records) and `config`.

**Entry points.** `MidasCoreApplication.main`, `IncentiveApiApplication.main`.

**Startup order (matters!).** Spring creates the singletons. `DemoDataLoader.afterSingletonsInstantiated()` seeds 5 users if the table is empty. After that, `finishRefresh()` starts the SmartLifecycle beans: the Kafka listener containers and the embedded Tomcat web server. So no message or request can arrive before the seed data exists (the DDL javadoc says this).

**Flow 1 — Kafka transaction (the core):**
1. The `KafkaMessageListenerContainer` thread polls the broker (group `midas-core`, 1 partition, so concurrency is effectively 1).
2. `ErrorHandlingDeserializer` delegates to `JsonDeserializer`. Type headers are ignored and the target is always `Transaction` (core.yml:28-29). If parsing fails, the record carries a `DeserializationException` and the error handler logs and skips it.
3. `TL.onTransaction(Transaction)` (TL:23) calls `TS.process` through the Spring **proxy**, which opens a JPA transaction.
4. `validate()` (TS:77): null checks → `signum() <= 0` → `senderId.equals(recipientId)` → `existsById(sender)` → `existsById(recipient)`.
5. `findById` ×2 (TS:57-58) → `canAfford` (TS:61).
6. `incentiveClient.fetchIncentive` makes a blocking HTTP POST, **while the DB transaction is open** (TS:66).
7. `sender.debit`, `recipient.credit(amount+incentive)` mutate managed entities. Dirty checking turns these into UPDATEs at flush.
8. `transactionRepository.save(new TransactionRecord(...))`. With IDENTITY the INSERT runs immediately.
9. Method returns, the proxy commits (flush UPDATEs, then COMMIT). Then the listener returns and the container commits the offset (default ack mode BATCH, auto-commit disabled by Spring Kafka). **At-least-once delivery.**
10. If an exception is thrown, the transaction rolls back and `DefaultErrorHandler` re-seeks and retries twice at 500ms intervals. After that it logs, skips and commits the offset.

**Flow 2 — GET /balance:** Tomcat → DispatcherServlet → `BC.getBalance(@RequestParam Long userId)` (a missing or non-numeric param gives 400) → `BS.balanceFor` (read-only tx) → `findById` → `Balance` record → Jackson `{"amount":850.00}`. An unknown user returns `{"amount":0}` with **200**.

**Flow 3 — POST /incentive (stand-in):** `IAC.incentive`: a null amount or one below 100.00 returns `0.00`; otherwise `amount × 0.02`, `setScale(2, HALF_EVEN)`.

**Middleware / filters.** None custom. There is no Spring Security, no CORS config, no `@ControllerAdvice`, no interceptors.
**Auth.** None anywhere.
**Real-time.** None (no sockets). Clients poll `/balance`.
**Error handling.** Kafka: ErrorHandlingDeserializer + DefaultErrorHandler. HTTP client: catch `RestClientException` and fall back. REST: Spring Boot's default `/error` (BasicErrorController).
**Deploy.** No Dockerfile for the apps; only Kafka is containerized. CI runs build + tests only.

**Things I could not confirm from code:**
- [Confirm: which parts came from Forage's starter repo (e.g., names like `UserRecord`, `foundation`, `general.kafka-topic`, port 33400 look like the simulation's skeleton) and which you wrote or restructured yourself.]
- [Confirm: the date and history, since the repo has no commits locally.]
- [Confirm: resolved dependency versions with `mvn dependency:tree`.]

---

# STEP 2 — Skeptical-interviewer audit (weak spots)

Each entry: **the weak spot** → the question it triggers → the best honest answer.

### Reliability / correctness

**W1. Not idempotent; at-least-once delivery can double-apply a transfer.** The message has no transaction ID (`Transaction.java`). The container commits the offset *after* the DB commit. A crash between the two means the message is redelivered and the money moves twice.
- Q: "Your consumer crashes right after the DB commit. What happens?"
- A: "It gets redelivered and applied twice, because the offset commit happens after the DB commit and I have no dedupe key. I'd add a producer-generated `transactionId` and a UNIQUE column on `transactions.external_id`, and insert the record first in the same DB transaction. A duplicate then fails on the constraint, and I catch that and ack. That's effectively-once *processing* on top of at-least-once *delivery*. The README lists this as a next step."

**W2. Remote HTTP call inside `@Transactional` (TS:66).** A DB connection and row state stay held for up to 2s+5s while waiting on the network. Under load this exhausts the Hikari pool (default 10).
- Q: "Why is a network call inside your DB transaction?"
- A: "It's a known anti-pattern. A slow incentive API holds a pooled connection for up to 7s. I'd fetch the incentive *before* opening the transaction: validate, call the API, then start a short transaction that re-reads with a lock, re-checks funds and writes. Or decouple it fully: commit the transfer, then credit the incentive asynchronously from an outbox event."

**W3. A retry re-calls the non-idempotent external API.** If the flush fails after `fetchIncentive`, `DefaultErrorHandler` retries the whole listener, so the incentive API is called up to 3 times.
- A: "Fine for a pure function like my stand-in, but if the real API records payouts, I'd send an idempotency key (the transaction ID) in the header."

**W4. "Skip after 2 retries" silently drops valid money transfers.** A transient DB outage of more than about 1s means transfers are logged and lost forever (KC:23). There is no dead-letter topic.
- A: "I'd split errors: non-retryable (bad data) goes straight to a DLT through `DeadLetterPublishingRecoverer`; transient (DB/network) gets exponential backoff, or I stop the container instead of skipping. Plus an alert on DLT lag."

**W5. Silent incentive fallback has no reconciliation.** When the API is down, the recipient gets 0 forever and nobody knows except a WARN log (IC:44).
- A: "Availability over correctness was the spec's choice. In production I'd persist `incentive_status=PENDING` and have a retry job or event pay it later, so it's eventually correct."

**W6. Rejected transactions aren't persisted.** They are only an INFO log (TS:53, 62), so there's no audit trail and no way to tell the producer.
- A: "I'd store them with a `status` and `reason`, or publish to a `transactions.rejected` topic so upstream can notify the user."

**W7. No locking / no `@Version` (UR).** Today it's safe only because there is 1 partition and 1 consumer thread, so processing is serial. With concurrency > 1, two transfers from the same sender can both pass `canAfford` and cause a lost update or a negative balance.
- A: "I'd key messages by `senderId` so one account's events stay ordered in one partition. Then add `@Version` optimistic locking, or `SELECT … FOR UPDATE` via `@Lock(PESSIMISTIC_WRITE)` for hot accounts, plus `CHECK (balance >= 0)` as a last line of defense. Lock both rows in a consistent order (lower id first) to avoid deadlocks."

**W8. In-memory H2 with `create-drop` plus committed Kafka offsets leads to inconsistency on restart.** When the app restarts, the DB is wiped and reseeded, but group `midas-core` already committed its offsets, so old transfers are **not** replayed. The balances "reset" while Kafka thinks they're processed. (`auto-offset-reset: earliest` only applies when there's no committed offset.)
- A: "For a demo that's acceptable. For real use: a durable DB (Postgres) and Flyway, so state survives. If I wanted rebuild-from-log, I'd reset offsets deliberately."

**W9. Input scale isn't validated.** `amount` `10.005` passes validation. The in-memory balance would have scale 3 while the column is scale 2 [Confirm H2's behavior: it rounds or errors on scale overflow]. The README's "no rounding errors" is an overclaim.
- A: "I'd reject `amount.scale() > 2` in validation, or normalize with `setScale(2, RoundingMode.UNNECESSARY)`, which throws on non-cent values."

**W10. Redundant queries.** `existsById` ×2 followed by `findById` ×2 makes 4 SELECTs where 2 would do. `existsById` does not populate the persistence context, so `findById` still hits the DB.
- A: "I'd just `findById` once each and reject on empty. Same behaviour, half the round-trips."

### Security

**W11. No authentication or authorization anywhere.** `GET /balance?userId=` lets anyone read anyone's balance (IDOR / BOLA).
**W12. The Kafka topic is effectively an unauthenticated "move money" API.** It runs PLAINTEXT with no SASL and no ACLs, so anyone who can reach the broker can produce a transfer from any sender. There's no notion of who authorized the transfer.
**W13. H2 console enabled, `sa` with an empty password, on the same port as the API** (core.yml:38-47). That's remote SQL access if exposed.
**W14. Financial data logged at INFO** (`Rejected Transaction[senderId=…, amount=…]`): PII/compliance concern.
**W15. No rate limiting, no input validation annotations** (the validation starter is unused), no CORS config (default: same-origin only, which is actually safe).
- Combined answer: "It's a local simulation, so security was out of scope. That's exactly what I'd fix first: Spring Security with JWT (OAuth2 resource server), where the user can only read their own balance from the token `sub`, or `ROLE_ADMIN`. Kafka with SASL/SCRAM or mTLS plus topic ACLs, so only the payments service can produce. Disable the H2 console outside a `dev` profile. Mask amounts and IDs in logs."

### Scalability

**W16. 1 partition, 1 replica** (KC:17; compose RF=1): no parallelism and no fault tolerance.
**W17. Blocking RestTemplate per message**: throughput is capped by incentive API latency (one message at a time).
**W18. In-memory DB, single instance, no caching, no pagination**. Not applicable yet, since there's no list endpoint.
**W19. GenerationType.IDENTITY disables Hibernate JDBC insert batching.**
- Answer: "More partitions keyed by sender, concurrency = partitions, multiple stateless instances in one consumer group, Postgres with a pool, async/batched incentive calls or async incentives."

### Code quality / API

**W20. Unknown user → 200 `{"amount":0}`** (BS:26): ambiguous; a 404 is more honest. (It was likely the simulation's spec. [Confirm])
**W21. Unused code:** `spring-boot-starter-validation`, `UserRepository.findByName`, the producer config (used only by tests).
**W22. `TransactionServiceTest` is a full `@SpringBootTest` + `@EmbeddedKafka` + `@DirtiesContext`**: slow. Its logic could be a plain Mockito unit test.
**W23. No API versioning, no standard error body** (Spring 6 `ProblemDetail`), no OpenAPI.
**W24. No Dockerfile for the apps, no health/metrics** (no Actuator), no structured logs or correlation IDs.
**W25. The repo has no git commits.** An interviewer asking "show me your commit history" is a risk. Fix before the interview.

---

# PART 1 — Question bank, categories 1–6

## Category 1 — Opening

### 1.1 Q: "Okay, tell me about this project."
- **Why they ask:** Warm-up. They test whether you can explain clearly, and they pick hooks from your words.
- **Follow-up ladder:** "Why Kafka?" → "What happens if the same message comes twice?" → "How did you test that?" → "What would break at 100x?"
- **Strong answer (30-second pitch):**
  - "Midas is an event-driven backend I built in JPMorgan's Forage software-engineering simulation, then cleaned up into a standalone repo."
  - "It consumes money-transfer events from a Kafka topic, validates them (positive amount, both users exist, not a self-transfer, sufficient funds), and calls an external Incentive REST API with timeouts and a fallback."
  - "Then, in one `@Transactional` unit, it debits the sender, credits the recipient with amount plus incentive, and writes an audit row through Spring Data JPA."
  - "A `GET /balance` endpoint serves balances. 16 tests, including an end-to-end test on an embedded Kafka broker, run in GitHub Actions CI."
  - "I also know its gaps: it isn't idempotent yet, it uses in-memory H2, and there's no auth. I can walk you through how I'd fix each."
- **Avoid:** "production-grade", "highly scalable", "JPMorgan uses this". Don't pretend it was an internship. Say "job simulation" clearly.
- **Likelihood:** High

### 1.2 Q: "Walk me through it step by step. A message lands on the topic. What happens?"
- **Why:** Tests whether you actually wrote or understand the code, or just copied it.
- **Follow-ups:** "Where exactly does the transaction start?" → "When is the Kafka offset committed relative to the DB commit?" → "So what if you crash in between?" (→ W1)
- **Strong answer (2-minute walkthrough script):**
  1. "The producer sends JSON like `{"senderId":1,"recipientId":2,"amount":150.00}` to `midas-transactions`."
  2. "The Spring Kafka listener container polls it. `ErrorHandlingDeserializer` wraps `JsonDeserializer`. I disabled type headers and set a default type of `Transaction`, so any producer's JSON maps to my record. A malformed message becomes a `DeserializationException` that the error handler logs and skips, so it doesn't block the partition."
  3. "`TransactionListener.onTransaction` hands off to `TransactionService.process`. It's `@Transactional`, so the Spring proxy opens a DB transaction."
  4. "`validate()` checks nulls, `amount.signum() > 0`, sender ≠ recipient, and that both users exist. Then I load both users and check `canAfford`, which uses `compareTo`, not `equals`."
  5. "I call the incentive API with `RestTemplate`: 2s connect and 5s read timeout. On any `RestClientException`, a null body or a negative value, I use zero."
  6. "`sender.debit(amount)`, `recipient.credit(amount + incentive)`. These are managed entities, so Hibernate dirty-checks them and issues UPDATEs at commit. I `save` a `TransactionRecord` as the audit row."
  7. "The commit happens, then the listener returns and the container commits the offset. If anything throws, the transaction rolls back and `DefaultErrorHandler` retries twice at 500ms, then logs and skips."
  8. "Separately, `GET /balance?userId=` reads the user in a read-only transaction and returns `{"amount": …}`."
- **Avoid:** Saying "exactly-once". Saying the offset is committed "automatically by Kafka". Spring Kafka disables `enable.auto.commit` and commits after the listener returns.
- **Likelihood:** High

### 1.3 Q: "What was YOUR role? What did Forage give you and what did you write?"
- **Why:** They're checking ownership and honesty. Simulations come with skeletons.
- **Follow-ups:** "Show me something you wrote that wasn't required." → "What did you change after finishing the simulation?" → "Why?"
- **Strong answer:**
  - "Solo project. Forage gave the task breakdown (Kafka listener, H2/JPA persistence, incentive API call, balance endpoint) and a pre-built incentive JAR. [Confirm: and a starter skeleton/test harness?]"
  - "I wrote the listener, the service logic with validation, the entities and repositories, the REST client and controller, and the tests."
  - "Beyond the spec, I added: the `ErrorHandlingDeserializer` + `DefaultErrorHandler` for poison messages, RestTemplate timeouts and fallback, `BigDecimal` money with precision/scale, a multi-module Maven build, my own stand-in `incentive-api` so the repo runs on its own, Docker Compose Kafka in KRaft mode, scripts, and GitHub Actions CI."
  - [Confirm each of these against what you actually did. Only claim what's true.]
- **Avoid:** Claiming you designed the task, or that it ran in production at JPMC.
- **Likelihood:** High

### 1.4 Q: "Why did you build this? What did you want to learn?"
- **Strong answer:** "I wanted hands-on experience with how real fintech backends are wired. That means async messaging with Kafka, transactional consistency for money, and integrating a flaky third-party API. Those are exactly the problems in order and payment systems."
- **Follow-ups:** "What was the hardest part?" (→ 1.5)
- **Avoid:** "Just for my resume."
- **Likelihood:** Medium

### 1.5 Q: "What was the hardest part / a bug you hit?"
- **Strong answer options (pick one that's true for you) [Confirm]:**
  - Getting Kafka JSON deserialization right. By default `JsonDeserializer` relies on `__TypeId__` headers containing the *producer's* class name, so I set `use.type.headers: false` and a default type. Without `ErrorHandlingDeserializer`, a bad JSON message makes the consumer loop on the same offset forever, because deserialization happens before the listener and the error handler sees it repeatedly.
  - The startup race: demo users must exist before the listener starts consuming. I used `SmartInitializingSingleton`, which runs after all singletons are created but before the SmartLifecycle beans (the Kafka containers and Tomcat) start.
  - Test flakiness with async Kafka: I replaced sleeps with Awaitility `untilAsserted`.
- **Avoid:** A made-up dramatic bug. They will drill into it.
- **Likelihood:** High

### 1.6 Q: "Draw the architecture."
- **1-minute diagram:**
```
 [Producer/upstream] --JSON--> (Kafka topic: midas-transactions, 1 partition)
                                       |
                          [midas-core :33400]
                          TransactionListener (@KafkaListener)
                                       |
                          TransactionService (@Transactional)
                           |                     |
             POST /incentive (2s/5s,       Spring Data JPA
             fallback 0)                         |
                 v                               v
        [incentive-api :8080]         (H2: users, transactions)
                                                 ^
   [Client] --GET /balance?userId--> BalanceController -> BalanceService
```
- **Likelihood:** High (sometimes they just say "draw it")

### 1.7 Q: "How big is it? How many endpoints and tables?"
- **Answer:** "About 1,250 lines including tests. Two Maven modules, one REST endpoint plus a Kafka consumer in core, one endpoint in the stand-in, two tables, 16 tests." Being precise signals ownership.
- **Likelihood:** Medium

### 1.8 Q: "Can you show me the GitHub? Why are there no commits?" / "When did you do this?"
- **Why:** Interviewers check commit history for authenticity.
- **Strong answer:** [Confirm. **Fix this before the interview**: the local repo has zero commits. Push it with an honest history. If you rewrote it later, say so: "I did the simulation in Dec 2025 and refactored it into this repo afterwards."]
- **Avoid:** Vague dates. Inconsistent stories between your resume and the README.
- **Likelihood:** Medium

---

## Category 2 — Architecture

### 2.1 Q: "Why is this event-driven? Why not just a POST /transfer endpoint?"
- **Why:** Tests whether you understand sync vs async trade-offs, not just "Kafka is cool".
- **Follow-ups:** "So how does the user know the transfer succeeded?" → "How would you return the result?" → "What's the latency cost?"
- **Strong answer:**
  - "The simulation's upstream system publishes transactions to Kafka. Midas is a downstream consumer."
  - "Benefits: decoupling, since the producer doesn't wait on my DB or the incentive API. Buffering, since a spike just builds consumer lag instead of failing requests. Replay. Per-partition ordering."
  - "Cost: no synchronous result. Today a rejection is only a log (TS:53). Properly, I'd emit a result event (`transaction.completed/rejected`) or expose `GET /transactions/{id}` status, and the client polls or gets a push."
- **Avoid:** "Kafka is faster than REST." It's not about speed.
- **Likelihood:** High

### 2.2 Q: "What are the layers and why separate them?"
- **Strong answer:** "Listener and controller are thin adapters (TL is 4 lines of logic, BC 1 line). `TransactionService` owns the rules and the transaction boundary. Repositories handle data access. Entities hold behavior: `debit`, `credit` and `canAfford` live on `UserRecord`, not in the service. `foundation` records are wire DTOs, separate from entities, so the JSON shape and DB shape can change independently. `client` isolates the outbound HTTP call, so tests can `@MockBean` it."
- **Follow-ups:** "Why put `debit` on the entity? Isn't that mixing concerns?" → "Rich vs anemic domain model." → "Why records for DTOs?"
- **Avoid:** "That's just how Spring does it."
- **Likelihood:** High

### 2.3 Q: "Trace `GET /balance?userId=1` from the socket to the DB and back."
- **Strong answer:** "Tomcat accepts the connection and a worker thread parses the HTTP request. `DispatcherServlet` asks the handler mappings, finds `BalanceController.getBalance`, and resolves `@RequestParam Long userId`. A missing param gives `MissingServletRequestParameterException` → 400; `abc` gives a type mismatch → 400. The method calls `BalanceService.balanceFor` through a proxy that opens a read-only transaction. `findById` runs `select … from users where id=?`. Map to a `Balance` record, or `BigDecimal.ZERO` if empty. Jackson's `HttpMessageConverter` writes `{"amount":850.00}`, status 200."
- **Follow-ups:** "What does `readOnly=true` actually do?" (Category 7) → "Why 200 for an unknown user?" (5.3)
- **Likelihood:** High

### 2.4 Q: "Trace the incentive call. What happens when the API is slow?"
- **Strong answer:** "`IncentiveClient.fetchIncentive` → `restTemplate.postForObject(url, transaction, Incentive.class)`. Jackson serializes the `Transaction` record as the body. Timeouts: 2s for the TCP connect, 5s waiting for response bytes. On timeout, Spring throws `ResourceAccessException`, a `RestClientException`, which I catch and treat as 0. A 5xx gives `HttpServerErrorException`, also caught (tested in `IncentiveClientTest.serverErrorFallsBackToZero`). A negative or null body also gives 0."
- **Follow-ups:** "Worst case, how long does one message take?" (~7s plus DB, and it holds a DB connection: W2) → "What about 1000 msgs/s?" → circuit breaker.
- **Likelihood:** High

### 2.5 Q: "Why two modules? Why is incentive-api even in your repo?"
- **Answer:** "In the simulation the incentive API was a provided JAR. I wrote a minimal replacement with the same contract so anyone can clone and run everything. Maven multi-module lets one `mvn verify` build and test both. In a real company they'd be separate repos and deployables owned by different teams, and I'd use a contract test (Pact or Spring Cloud Contract) instead of sharing code. Notice the modules share no code: `incentive-api` defines its own `TransactionRequest` record."
- **Likelihood:** Medium

### 2.6 Q: "Is this a microservice? Is the architecture monolithic?"
- **Answer:** "Two independently deployable Spring Boot apps talking over HTTP, plus Kafka. Midas itself is one small service with a single responsibility. I wouldn't oversell it as a 'microservices architecture'."
- **Likelihood:** Medium

### 2.7 Q: "Where is the state? Is your service stateless?"
- **Answer:** "All business state is in the DB. But the DB is *embedded in-process* (H2 mem), so right now the service is **not** stateless: two instances would have two different databases. Moving to an external Postgres makes the app tier stateless and horizontally scalable."
- **Likelihood:** Medium-High (leads into scaling)

---

## Category 3 — Tech choices: why / why not

### 3.1 Q: "Why Kafka? Why not RabbitMQ / SQS / just a DB table as a queue?"
- **Follow-ups:** "What's a partition?" → "How does ordering work?" → "Consumer groups?" → "What's the delivery guarantee?" (Part 2)
- **Strong answer:**
  - "The simulation required Kafka. But it fits: a durable, replayable log, per-partition ordering (important for balances, since one account's events must be applied in order), consumer groups for horizontal scaling, and high throughput."
  - "RabbitMQ is a broker with per-message ack and routing; messages are gone after ack. Good for task queues, but no replay and weaker ordering under multiple consumers."
  - "SQS is managed and simple, but standard queues aren't ordered; FIFO queues have throughput limits."
  - "A DB table as a queue is fine at small scale (outbox pattern), but polling and locking don't scale like a log."
- **Avoid:** "Kafka is exactly-once." "Kafka is a queue." It's a log; consumers track offsets.
- **Likelihood:** High

### 3.2 Q: "Why H2? Would you use it in production?"
- **Answer:** "No. It was the simulation's choice for zero-setup local runs and fast tests. In-memory plus `create-drop` means data dies on restart (W8). Production would be PostgreSQL: ACID, row locks, `CHECK` constraints, `SELECT FOR UPDATE`, replication. Plus Flyway migrations and Testcontainers so tests run against real Postgres, not an H2 dialect that differs in subtle ways."
- **Follow-up:** "What differences between H2 and Postgres could bite you?" → FK indexes not automatic in PG; locking semantics; types; case sensitivity.
- **Likelihood:** High

### 3.3 Q: "SQL or NoSQL for this? Why not MongoDB?"
- **Answer:** "Relational. A transfer touches two account rows plus an audit row that must commit atomically. That's a multi-row ACID transaction with foreign keys and constraints, which is classic RDBMS. Mongo has multi-document transactions now, but you'd be fighting the model. NoSQL would make sense for, say, the high-volume transaction *history* read model, or a Cassandra-style ledger at huge scale."
- **Likelihood:** High

### 3.4 Q: "Why Spring Boot and not Node/Express or Go?"
- **Answer:** "The simulation was Java/Spring. Honestly, Spring fits here: mature Kafka integration (`@KafkaListener`, error handlers), declarative transactions, JPA, and great test slices (`@WebMvcTest`, `@DataJpaTest`, `@EmbeddedKafka`). Node would work but the money/transaction tooling is weaker. Go is fast and lean but more hand-wiring."
- **Likelihood:** Medium

### 3.5 Q: "Why JPA/Hibernate? Why not plain JDBC / JdbcTemplate / jOOQ?"
- **Answer:** "JPA gives me repositories for free and dirty-checking (I just call `debit()` and Hibernate writes the UPDATE). The downside is hidden queries; for example, `existsById` + `findById` makes 4 SELECTs where 2 would do (W10). For a hot money path, an explicit single-statement `UPDATE users SET balance = balance - ? WHERE id = ? AND balance >= ?` via JdbcTemplate is atomic and race-free without locks. That's an attractive alternative."
- **Likelihood:** High (the atomic UPDATE answer impresses)

### 3.6 Q: "Why BigDecimal? Why not double? Why not long paise?"
- **Follow-ups:** "Show me why 0.1+0.2 fails." → "`equals` vs `compareTo` on BigDecimal?" → "What rounding mode and why?"
- **Strong answer:**
  - "`double` is binary floating point; 0.1 has no exact representation, so `0.1+0.2 = 0.30000000000000004`. That's unacceptable for money."
  - "BigDecimal is exact decimal. The columns are `NUMERIC(19,2)` (UR:23, TR:33)."
  - "`long` in minor units (paise) is also great: faster and simple, and many payment systems (Stripe-style) do it. BigDecimal is more readable and handles percentages like the 2% incentive."
  - "I compare with `compareTo`, not `equals`, because `new BigDecimal("2.0").equals(new BigDecimal("2.00"))` is false (scale differs). See `canAfford` (UR:44) and the tests' `isEqualByComparingTo`."
  - "The incentive uses `HALF_EVEN` (banker's rounding) to avoid systematic upward bias."
- **Avoid:** Repeating the README's "no rounding errors at all". Rounding is still a decision (W9).
- **Likelihood:** High

### 3.7 Q: "Why RestTemplate? Isn't it deprecated?"
- **Answer:** "It's in maintenance mode, not deprecated. Boot 3.2 / Spring 6.1 introduced `RestClient`, the modern synchronous fluent API, so I'd use that today. `WebClient` (reactive) makes sense if I wanted non-blocking calls or high concurrency. Because my consumer is single-threaded and blocking anyway, RestTemplate wasn't the bottleneck. I built it through `RestTemplateBuilder` so Boot's auto-config (message converters, metrics) applies and `@RestClientTest` can bind a `MockRestServiceServer`."
- **Likelihood:** Medium-High

### 3.8 Q: "Why Java records for Transaction/Incentive/Balance?"
- **Answer:** "They're immutable data carriers with `equals/hashCode/toString` for free. Jackson 2.12+ supports them natively. Perfect for wire DTOs. Entities can't be records because JPA needs a no-arg constructor, mutable fields and proxies (hence `protected UserRecord()`)."
- **Likelihood:** Medium

### 3.9 Q: "Why KRaft mode in Docker Compose? What's ZooKeeper?"
- **Answer:** "ZooKeeper used to store Kafka cluster metadata and run controller election. KRaft (Kafka Raft) moves that into Kafka itself: the node is both `broker,controller` (docker-compose.yml:10). It's simpler, with one container, and ZooKeeper mode is removed in Kafka 4.0."
- **Likelihood:** Low-Medium

### 3.10 Q: "Why `apache/kafka:3.7.0` and not Confluent/Bitnami? Why Maven not Gradle? Why GitHub Actions?"
- **Answer:** "Official image, pinned version for reproducibility. Maven because it was the simulation's default, it's declarative, and Boot's parent POM manages versions. Gradle would be faster with build caching. Actions because it's free and integrated with GitHub. The CI just runs `mvn -B verify` on push and PR, with Maven caching."
- **Likelihood:** Low

### 3.11 Q: "Why Awaitility instead of Thread.sleep in tests?"
- **Answer:** "The Kafka flow is async. A fixed sleep is either flaky (too short) or slow (too long). `await().atMost(20s).untilAsserted(...)` polls and passes as soon as the condition holds (KafkaIntegrationTest:50)."
- **Likelihood:** Low-Medium

### 3.12 Q: "Why `SmartInitializingSingleton` for seed data and not `CommandLineRunner` / `data.sql`?"
- **Answer:** "`CommandLineRunner` runs *after* the context is refreshed, by which point the Kafka listener containers and Tomcat are already started, so a message could arrive before the users exist. `afterSingletonsInstantiated` runs before SmartLifecycle beans start. `data.sql` would also work but is less flexible (the `count() > 0` guard, logging). It's gated by `@ConditionalOnProperty` so tests disable it."
- **Likelihood:** Medium (great "why did you write it this way" moment)

---

## Category 4 — Database

### 4.1 Q: "Walk me through your schema."
- **Strong answer:** Two tables (see 0.3). "`users(id PK identity, name unique not null, balance numeric(19,2) not null)`, and `transactions(id, sender_id FK, recipient_id FK, amount, incentive, processed_at)`. Two many-to-one relations to users. `TransactionRecord` is an append-only audit log; `UserRecord.balance` is a materialized running balance."
- **Follow-ups:** "Why store balance at all if you have the transaction log?" → "How would you check they agree?" (4.6) → "Ledger / double-entry?"
- **Likelihood:** High

### 4.2 Q: "Write the DDL by hand."
```sql
CREATE TABLE users (
  id       BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  name     VARCHAR(255)   NOT NULL UNIQUE,
  balance  NUMERIC(19,2)  NOT NULL
  -- I'd add: , version BIGINT NOT NULL DEFAULT 0, CHECK (balance >= 0)
);

CREATE TABLE transactions (
  id            BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  sender_id     BIGINT        NOT NULL REFERENCES users(id),
  recipient_id  BIGINT        NOT NULL REFERENCES users(id),
  amount        NUMERIC(19,2) NOT NULL,
  incentive     NUMERIC(19,2) NOT NULL,
  processed_at  TIMESTAMP WITH TIME ZONE NOT NULL
  -- I'd add: external_id VARCHAR(64) NOT NULL UNIQUE  (idempotency)
);
CREATE INDEX idx_tx_sender_time    ON transactions(sender_id, processed_at DESC);
CREATE INDEX idx_tx_recipient_time ON transactions(recipient_id, processed_at DESC);
```
- Note: Hibernate generated this via `ddl-auto: create-drop`; I didn't hand-write migrations. [Confirm the exact generated DDL with `spring.jpa.show-sql=true`]
- **Likelihood:** Medium

### 4.3 Q: "What indexes do you have? What's missing?"
- **Follow-ups:** "How does a B-tree index work?" → "Composite vs two single indexes?" → "Leftmost-prefix rule?" → "Cost of indexes on writes?"
- **Strong answer:**
  - "PKs on both tables, plus a unique index on `users.name` from `unique = true`."
  - "Missing: indexes on `transactions.sender_id` and `recipient_id`. H2 auto-indexes FK columns but Postgres doesn't. For 'show user X's history newest first' I'd use composite `(sender_id, processed_at DESC)`, and the same for recipient. With leftmost prefix it also serves `WHERE sender_id = ?` alone."
  - "Balance lookup is by PK, so it's already O(log n) on the PK B-tree."
  - "Trade-off: every index slows inserts. `transactions` is write-heavy, so add only what queries need."
- **Avoid:** "I added indexes for performance." You didn't explicitly.
- **Likelihood:** High

### 4.4 Q: "What's the relationship mapping? Why `@ManyToOne(LAZY)` and no `@OneToMany` on User?"
- **Answer:** "A transaction has exactly one sender and one recipient (`optional=false`, so NOT NULL FKs). I kept it unidirectional: a `List<TransactionRecord>` on `UserRecord` could be huge and tempts N+1 or accidental full loads. Queries like 'history for user' go through the repository with paging. `@ManyToOne` defaults to EAGER in JPA, so I set `LAZY` explicitly to avoid joining users every time I load a transaction."
- **Follow-up:** "What's N+1? Where could it happen here?" → "If I list 100 transactions and print `getSender().getName()`, that's 1 + up to 100 queries. Fix with `JOIN FETCH`, `@EntityGraph` or a DTO projection."
- **Likelihood:** High

### 4.5 Q: "How do you make the debit and credit atomic? What isolation level?"
- **Follow-ups:** "What's ACID?" → "What isolation is the default?" → "What anomalies does it allow?" → "How would two concurrent transfers corrupt the balance?"
- **Strong answer:**
  - "`@Transactional` on `process` (TS:49). Both UPDATEs and the INSERT commit or roll back together. Any RuntimeException rolls back."
  - "I didn't set isolation, so it's the DB default: READ COMMITTED for both H2 and Postgres."
  - "Under READ COMMITTED, the read-check-write pattern (`findById` → `canAfford` → set balance) is vulnerable to **lost updates** if two transactions touch the same user concurrently. Today that's prevented only by single-threaded consumption (1 partition)."
  - "Fixes: `@Lock(PESSIMISTIC_WRITE)` (SELECT … FOR UPDATE), `@Version` optimistic locking with retry, or a conditional atomic `UPDATE … SET balance = balance - :amt WHERE id = :id AND balance >= :amt` and check rows-affected = 1."
- **Avoid:** "@Transactional makes it thread-safe." It doesn't.
- **Likelihood:** High

### 4.6 Q: "Write a query to verify money isn't being created or destroyed."
- **Answer:** Invariant: total balance now = initial total + total incentives paid.
```sql
SELECT SUM(balance) FROM users;                 -- current total
SELECT SUM(incentive) FROM transactions;        -- money "created" by incentives
-- Per-user check if you keep opening balances:
SELECT u.id,
       u.balance,
       u.opening_balance
       - COALESCE((SELECT SUM(amount) FROM transactions t WHERE t.sender_id = u.id), 0)
       + COALESCE((SELECT SUM(amount + incentive) FROM transactions t WHERE t.recipient_id = u.id), 0) AS expected
FROM users u;
```
- Note: `opening_balance` doesn't exist today. The seeded balance is overwritten. Point out that you'd need it (or an "opening" ledger entry) to reconcile.
- **Likelihood:** Medium (great differentiator)

### 4.7 Q: "Other queries: top 5 senders by volume, user's last 10 transactions, daily totals."
```sql
-- Top 5 senders by amount sent
SELECT sender_id, SUM(amount) AS total_sent, COUNT(*) AS n
FROM transactions GROUP BY sender_id ORDER BY total_sent DESC LIMIT 5;

-- Last 10 transactions involving user 1
SELECT * FROM transactions
WHERE sender_id = 1 OR recipient_id = 1
ORDER BY processed_at DESC LIMIT 10;   -- OR defeats a single index; UNION ALL of the two indexed queries is faster

-- Daily volume
SELECT CAST(processed_at AS DATE) AS day, SUM(amount), COUNT(*)
FROM transactions GROUP BY CAST(processed_at AS DATE) ORDER BY day;

-- Users who never sent anything
SELECT u.* FROM users u
LEFT JOIN transactions t ON t.sender_id = u.id
WHERE t.id IS NULL;
```
- Spring Data equivalent: `List<TransactionRecord> findTop10BySenderIdOrderByProcessedAtDesc(Long senderId);` or `Page<TransactionRecord> findBySenderId(Long id, Pageable p);`
- **Likelihood:** Medium-High (SQL is a common live-ask)

### 4.8 Q: "Why `GenerationType.IDENTITY`? What are the alternatives?"
- **Answer:** "IDENTITY uses the DB's auto-increment. It's simple, but Hibernate must INSERT immediately to learn the ID, which **disables JDBC batch inserts**. `SEQUENCE` with `allocationSize` (pooled optimizer) lets Hibernate pre-allocate IDs and batch, so it's better on Postgres. UUIDs avoid a central sequence (good for distributed/sharded systems), but random UUIDv4 fragments B-tree indexes. UUIDv7/ULID are time-ordered and fix that."
- **Likelihood:** Medium

### 4.9 Q: "What does `ddl-auto: create-drop` do? Would you ship it?"
- **Answer:** "Hibernate creates the schema at startup and drops it on shutdown. Never in prod: one restart and data is gone. Prod uses `validate` (or `none`) plus Flyway versioned migrations (`V1__init.sql`), reviewed in PRs and applied in CI/CD. Also W8: with in-memory DB plus committed Kafka offsets, a restart loses state that Kafka thinks was processed."
- **Likelihood:** High

### 4.10 Q: "Is `name` unique a good idea? Why is it there?"
- **Answer:** "It prevents duplicate seed users and gives a natural lookup (`findByName`, which is actually unused now, W21). Real systems wouldn't make display names unique; I'd use email/phone with a unique index, and keep `id` as the surrogate key."
- **Likelihood:** Low-Medium

### 4.11 Q: "Should balance be a column at all? What's a ledger?"
- **Answer:** "Storing a running balance gives O(1) reads but can drift from history. Fintech uses **double-entry ledgers**: every transfer writes immutable debit and credit entries that sum to zero, and the balance is derived (or cached and reconciled). My `transactions` table is half of that idea. I'd add an entries table and keep `balance` as a cached projection, updated in the same transaction."
- **Likelihood:** Medium (fintech-flavoured, good signal)

### 4.12 Q: "What happens if `amount` has 3 decimal places?"
- **Answer:** W9. "Validation doesn't check scale. The entity's BigDecimal would carry 3 decimals in memory, and what the DB does on write depends on the engine [Confirm H2: round vs error]. I'd reject `amount.scale() > 2` in `validate()`."
- **Likelihood:** Medium (classic edge-case probe)

---

## Category 5 — API design

### 5.1 Q: "Why `GET /balance?userId=1` and not `/users/1/balance`?"
- **Answer:** "The simulation specified the query-param shape. RESTfully, the balance is a sub-resource of a user, so `GET /users/{id}/balance` (or `/accounts/{id}`) is cleaner. Path for identity, query params for filtering, sorting and paging. And with auth, it should really be `GET /me/balance`, with the user taken from the token, not from a param (prevents IDOR)."
- **Likelihood:** High

### 5.2 Q: "What status codes does your API return?"
- **Answer:** "200 with `{"amount":…}`. 400 if `userId` is missing (tested: `BalanceControllerTest.missingUserIdIsBadRequest`) or non-numeric (type mismatch; not explicitly tested). 500 for unexpected errors via Spring Boot's default `/error` JSON. No 401/403 because there's no auth. No 404 because unknown users return 0 (5.3)."
- **Follow-ups:** "400 vs 422?" → "401 vs 403?" → "When is 409 right?" (idempotency/duplicate transfer) → "202 Accepted?" (async transfers: return 202 + status URL).
- **Likelihood:** High

### 5.3 Q: "Unknown user returns 200 with balance 0? That seems wrong."
- **Answer:** "Agreed. It hides bugs and makes 'doesn't exist' look like 'broke'. It matched the simulation's expected behavior [Confirm], and `BalanceServiceTest.unknownUserHasZeroBalance` documents it. I'd return 404 with a `ProblemDetail` body (RFC 7807, built into Spring 6). The service would throw `UserNotFoundException`, mapped via `@RestControllerAdvice`."
- **Follow-up:** "Doesn't 404 leak which user IDs exist?" → "Yes. With auth you'd only query yourself, so it's moot; otherwise 404 for both 'not found' and 'not yours'."
- **Likelihood:** High

### 5.4 Q: "How do you validate input?"
- **Answer:** "Manually in `TransactionService.validate` (TS:77-93): nulls, positive amount, self-transfer, existence. Funds are checked after loading. On the REST side, Spring's binding enforces a required `Long`. I added `spring-boot-starter-validation` but never used it. Honestly that's a leftover. I'd annotate the DTO (`@NotNull Long senderId`, `@Positive @Digits(integer=17, fraction=2) BigDecimal amount`) and validate the Kafka payload with `@Valid` / `@Payload` validation in the listener, keeping business rules (exists, funds) in the service."
- **Avoid:** Claiming you used Bean Validation. The dependency exists but no annotations are used.
- **Likelihood:** High

### 5.5 Q: "How do you handle errors in the API? Any global handler?"
- **Answer:** "No `@ControllerAdvice`; I rely on Boot's default error handling. I'd add a `@RestControllerAdvice` returning `ProblemDetail`, with consistent `type/title/status/detail`, a correlation ID, and no stack traces leaked."
- **Likelihood:** Medium-High

### 5.6 Q: "Is your transfer 'API' idempotent? Is GET idempotent?"
- **Answer:** "GET /balance is safe and idempotent. POST /incentive is a pure function, so it's effectively idempotent too. The Kafka transfer path is NOT idempotent (W1): the same message twice moves money twice. For an HTTP version I'd require an `Idempotency-Key` header: store key → result with a unique constraint and return the stored result on replay (Stripe-style)."
- **Follow-ups:** "Where do you store keys? For how long?" → "What if two requests with the same key arrive concurrently?" (unique constraint wins; the loser waits or gets 409).
- **Likelihood:** High

### 5.7 Q: "Pagination? Versioning? Docs?"
- **Answer:** "No list endpoints yet, so no pagination. For `GET /users/{id}/transactions` I'd use cursor (keyset) pagination on `(processed_at, id)`, not OFFSET, because OFFSET scans and skips rows and gets slow and inconsistent as new rows arrive. Versioning: `/v1/` prefix. Docs: springdoc-openapi."
- **Likelihood:** Medium

### 5.8 Q: "Your balance JSON is `{"amount": 850.00}`, a number. Any problem?"
- **Answer:** "JavaScript clients parse JSON numbers as IEEE-754 doubles, so precision can be lost for large values. Many payment APIs send money as a string (`"850.00"`) or integer minor units plus a currency (`{"amount": 85000, "currency":"INR"}`). Also there's no currency field at all. I'd add one."
- **Likelihood:** Medium (shows depth)

### 5.9 Q: "What's the contract with the Incentive API? What if it changes?"
- **Answer:** "`POST /incentive` with the transaction JSON returns `{"amount": number}`. I defend against a null body, a null amount and negative values (IC:39). Unknown fields: Spring Boot's Jackson has `FAIL_ON_UNKNOWN_PROPERTIES` off by default, so added fields don't break me. A renamed field would silently become 0 though, which is a risk. A consumer-driven contract test (Pact) would catch it in CI."
- **Likelihood:** Medium

---

## Category 6 — Auth & security

> Honest framing first: **this project has no authentication or authorization.** Expect questions on "how would you add it" plus fundamentals. Never pretend otherwise.

### 6.1 Q: "How do users log in? How is the balance endpoint protected?"
- **Strong answer:** "They don't. It's a simulation service with no auth, so anyone can call `/balance?userId=` for any user. That's an IDOR / broken object-level authorization issue, the top OWASP API risk. To fix it, I'd add Spring Security as an OAuth2 resource server validating a JWT from an auth service. The endpoint becomes `/me/balance`, using the `sub` claim, or checks `token.sub == userId || hasRole('ADMIN')`."
- **Follow-ups:** "Walk me through a JWT login flow." (6.2) → "Where is the token stored client-side?" → "How do you revoke it?"
- **Avoid:** Inventing an auth layer. Saying "it's internal so it's fine."
- **Likelihood:** High

### 6.2 Q: "Explain the full login flow you'd build: hashing, JWT, expiry."
- **Answer:**
  - "Signup: hash the password with bcrypt (or argon2), which uses a per-user salt and a cost factor, and store only the hash."
  - "Login: look up by email, `BCrypt.matches(raw, hash)`. Compare in constant time, and give the same error for 'no user' and 'bad password'."
  - "Issue a short-lived access JWT (~15 min), signed with RS256 so services verify with the public key, with claims `sub`, `roles`, `exp`, `iat`. Plus a long-lived refresh token, stored server-side (hashed) so it can be rotated and revoked."
  - "Each request: the resource server verifies the signature, `exp` and `iss/aud`. No DB hit."
  - "Client storage: an httpOnly, Secure, SameSite cookie for web avoids XSS token theft; mobile uses the secure keystore."
- **Likelihood:** High

### 6.3 Q: "Hashing vs encryption? Why bcrypt and not SHA-256?"
- **Answer:** "Hashing is one-way (verify, never recover). Encryption is reversible with a key. Passwords are hashed. SHA-256 is designed to be *fast*, so GPUs try billions per second. bcrypt/argon2 are deliberately slow and salted, which defeats rainbow tables and makes brute force expensive. Encryption is for data you need back, like a card number (AES-GCM, key in a KMS)."
- **Likelihood:** High

### 6.4 Q: "JWT vs server sessions? Which would you choose here?"
- **Answer:** "Session: a random ID in a cookie, state in a server store like Redis. Easy revoke, but every request needs a lookup. JWT: self-contained and stateless to verify, which suits multiple services (Midas, incentive, gateway). The downside is it's hard to revoke before `exp`, so keep it short-lived plus refresh tokens plus a denylist for emergencies. For service-to-service (Midas → Incentive API), I'd use mTLS or OAuth2 client credentials, not user JWTs."
- **Follow-up:** "Token stolen. What now?" (6.5)
- **Likelihood:** High

### 6.5 Q: "Someone steals a JWT. What's the damage and how do you limit it?"
- **Answer:** "They can act as that user until `exp`. Mitigations: short TTL, refresh-token rotation with reuse detection (a reused refresh token revokes the whole family), httpOnly cookies against XSS theft, binding tokens to the client (DPoP or mTLS) in high-security cases, a denylist by `jti` for incidents, and anomaly detection (new device or geo). For money movement, step-up auth (OTP) on the transfer itself."
- **Likelihood:** Medium-High

### 6.6 Q: "Who can publish to your Kafka topic? Couldn't anyone send a transfer?"
- **Why:** The sharpest security question on this project (W12).
- **Answer:** "Yes. Today the topic is effectively an unauthenticated 'move money' API: PLAINTEXT listener, no SASL, no ACLs, and the message doesn't say *who authorized* it. I'd fix it with TLS plus SASL/SCRAM or mTLS auth on the broker, and topic ACLs so only the payments API's principal can WRITE and only `midas-core` can READ. Upstream would authenticate the user and authorize the transfer *before* publishing, and the event would carry an `initiatedBy` and `transactionId` (optionally signed) for audit."
- **Likelihood:** Medium-High

### 6.7 Q: "Is your app vulnerable to SQL injection?"
- **Answer:** "No raw SQL anywhere. Spring Data derived queries and `findById` use prepared statements with bound parameters, so input is never concatenated into SQL. The risk would come back with string-built JPQL or native queries; I'd use `@Query` with `:params`. The bigger SQL hole is actually the **H2 console** (W13): enabled, `sa`, empty password, on port 33400. Anyone who can reach it gets full SQL. I'd restrict it to a `dev` profile."
- **Likelihood:** High

### 6.8 Q: "XSS? CSRF? CORS?"
- **Answer:**
  - "XSS: the API returns JSON (`application/json`), not HTML, and there's no user-generated text. Risk is low."
  - "CSRF: only GET endpoints and no cookie auth, so not applicable. If I used cookie-based auth for a POST transfer, I'd need SameSite cookies or CSRF tokens. With bearer tokens in headers, CSRF doesn't apply."
  - "CORS: none configured, so browsers block cross-origin reads by default, which is safe. I'd whitelist specific origins, never `*` with credentials."
- **Likelihood:** Medium

### 6.9 Q: "How are secrets managed?"
- **Answer:** "There are basically no secrets: H2 with `sa` and an empty password. Config uses `${ENV:default}` placeholders for the broker and API URL (core.yml:9, 20). In prod, the DB password, Kafka credentials and signing keys would come from env vars injected by a secret manager (Vault, AWS Secrets Manager, K8s Secrets), never committed. Rotate them, and scan the repo with something like gitleaks in CI."
- **Likelihood:** Medium

### 6.10 Q: "Anything sensitive in your logs?"
- **Answer:** "Yes. Rejections log the full `Transaction` (ids and amount) at INFO (TS:53, 62), and the incentive failure logs it at WARN (IC:44). For a real fintech, that's PII/financial data in logs. I'd log a transaction ID and the reason, mask amounts, and use structured JSON logs with a correlation ID."
- **Likelihood:** Low-Medium

### 6.11 Q: "Rate limiting / abuse protection?"
- **Answer:** "None. `/balance` could be scraped by enumerating `userId` (sequential IDENTITY ids make that trivial). I'd put an API gateway in front with per-user/IP token-bucket limits (Redis-backed for multiple instances), use non-guessable public IDs, and require auth. On the Kafka side, quotas per producer client."
- **Likelihood:** Medium

### 6.12 Q: "Should the Incentive API call be authenticated? What if someone spoofs it?"
- **Answer:** "Currently it's plain HTTP to a configurable URL. If an attacker controls DNS/network or the `INCENTIVE_API_URL` env var, they could return a huge incentive, and I only reject *negative* values (IC:39). I'd use HTTPS with mTLS or a client-credentials token, and add a sanity cap (e.g., incentive ≤ X% of amount) as a business rule."
- **Likelihood:** Medium (good "what if" catch)

---

*End of PART 1.*

---

# PART 2 — Question bank, categories 7–11

## Category 7 — Framework internals (Spring, JPA/Hibernate, Kafka)

### 7.1 Q: "How does `TransactionService` get its dependencies? Why constructor injection?"
- **Why:** Basic Spring DI check. Also tests whether you know *why* and not just *how*.
- **Follow-up ladder:** "What's a bean?" → "What's the default scope? Is your service thread-safe?" → "What if two beans depend on each other?" → "How does Spring know what to scan?"
- **Strong answer:**
  - "All my beans use constructor injection (TS:38-44, BC:15, TL:18, IC:27, DDL:28). There's no `@Autowired` on fields in main code. With a single constructor, Spring uses it automatically."
  - "Why: fields can be `final`, so the object is never half-built. Dependencies are explicit. Unit tests can just `new TransactionService(mockRepo, mockRepo, mockClient)` without Spring. A circular dependency fails fast at startup instead of hiding."
  - "`@SpringBootApplication` = `@Configuration` + `@EnableAutoConfiguration` + `@ComponentScan` from `com.jpmc.midascore` downward. That's why all my packages live under it."
  - "Default scope is singleton: one instance shared by all threads. My services are stateless (only final references to other singletons), so they're thread-safe. State lives in the DB and in per-transaction entities."
- **Avoid:** "Spring creates a new service per request." Saying field injection is "the same".
- **Likelihood:** High

### 7.2 Q: "How does `@Transactional` actually work?"
- **Follow-up ladder:** "So what's a proxy?" → "What if `process()` called another `@Transactional` method in the same class?" → "When does it roll back?" → "What does `readOnly` do?"
- **Strong answer:**
  - "Spring wraps `TransactionService` in a proxy (CGLIB subclass, since it's a class without an interface). The listener actually calls the proxy. The proxy asks `JpaTransactionManager` to begin a transaction: it gets a JDBC connection from Hikari, sets autocommit off, and binds an `EntityManager` to the thread (ThreadLocal). Then it calls my real method, and commits or rolls back."
  - "**Self-invocation pitfall:** calling `this.someTransactionalMethod()` from inside the class bypasses the proxy, so annotations on it are ignored. In my code `validate()` is private and runs inside `process()`'s transaction, which is what I want."
  - "**Rollback rules:** by default it rolls back on `RuntimeException` and `Error` only. Checked exceptions *commit*, unless you set `rollbackFor`. My `orElseThrow()` throws `NoSuchElementException` (unchecked), so it rolls back."
  - "**Rejections don't throw.** `process` returns `Optional.empty()`, so the transaction commits with nothing changed. That's deliberate: a rejection isn't a failure, and throwing would trigger Kafka retries for something that will never succeed."
  - "**Propagation:** default `REQUIRED` joins an existing transaction or creates one. `REQUIRES_NEW` suspends the outer one. I'd use that for, say, writing a rejection audit row that must survive even if the outer transaction rolls back."
- **Avoid:** "@Transactional locks the rows." "It makes the method synchronized."
- **Likelihood:** High

### 7.3 Q: "What does `@Transactional(readOnly = true)` on `balanceFor` do?"
- **Answer:** "It's a hint, not a guarantee. Spring tells Hibernate to use flush mode MANUAL and marks the session read-only, so Hibernate skips taking dirty-check snapshots and never flushes. That saves memory and CPU. Spring may also call `Connection.setReadOnly(true)`, and some setups use that to route to a read replica. It doesn't stop a native `UPDATE` from running. And on a single `findById` the benefit is small; the bigger value is clear intent."
- **Follow-up:** "Would you route reads to a replica?" → "Then `/balance` could be stale by replication lag. Fine for a display, not for a funds check."
- **Likelihood:** Medium

### 7.4 Q: "I don't see `userRepository.save(sender)` anywhere. How do the balances get saved?"
- **Why:** A classic "do you understand JPA or just call methods" question.
- **Follow-up ladder:** "What's the persistence context?" → "What's dirty checking?" → "When does flush happen?" → "What are the entity states?"
- **Strong answer:**
  - "`findById` inside the transaction returns a **managed** entity. Hibernate keeps it in the persistence context (first-level cache) along with a snapshot of its loaded state."
  - "`sender.debit(amount)` only changes the Java field. At flush time (just before commit, or before a query that needs fresh data), Hibernate compares each managed entity with its snapshot. That's **dirty checking**. It issues `UPDATE users SET name=?, balance=? WHERE id=?` for each changed one."
  - "`TransactionRecord` is new, so I do need `save`. `SimpleJpaRepository.save` checks `isNew` (id is null) → `em.persist`. With IDENTITY the INSERT runs immediately to get the generated id."
  - "Entity states: transient (new, unknown to JPA) → managed → detached (after the transaction or EntityManager closes) → removed."
- **Avoid:** "Hibernate saves automatically." Explain *when* and *how*.
- **Likelihood:** High

### 7.5 Q: "How many SQL statements does one successful transaction run?"
- **Answer (count them out loud):**
  1. `existsById(sender)`: a count/exists SELECT [Confirm exact SQL with `show-sql`]
  2. `existsById(recipient)`: same
  3. `findById(sender)`: SELECT. `existsById` doesn't put the entity in the persistence context, so this goes to the DB.
  4. `findById(recipient)`: SELECT
  5. INSERT into transactions (immediate, because of IDENTITY)
  6. UPDATE users (sender) at flush
  7. UPDATE users (recipient) at flush
  8. COMMIT
- "Seven statements. Removing `existsById` and rejecting on an empty `findById` saves two (W10). An atomic `UPDATE … WHERE balance >= ?` could cut it further."
- **Likelihood:** Medium (big signal if you nail it)

### 7.6 Q: "What's `open-in-view: false` and why did you set it?"
- **Answer:** "Open Session In View keeps the Hibernate session open for the whole HTTP request, including JSON rendering, so lazy loading 'just works' in controllers. The cost: a DB connection held for the full request, and hidden queries (N+1) firing during serialization. Spring Boot even logs a warning when it's on by default. With it off, my transactions end at the service boundary, and that's fine because `BalanceService` returns a `Balance` record DTO, not an entity."
- **Follow-up:** "So what would happen if a controller returned a `TransactionRecord`?" → "Jackson would touch the LAZY `sender` proxy after the session closed: `LazyInitializationException` (or it serializes the Hibernate proxy internals). That's why DTOs."
- **Likelihood:** Medium

### 7.7 Q: "What's LAZY vs EAGER? What's the N+1 problem? Is there any in your code?"
- **Answer:** "`@ManyToOne` defaults to EAGER. I set LAZY on `sender` and `recipient` (TR:25, 29), so loading a transaction gives proxies, and the user is fetched on first access. N+1: load N transactions with 1 query, then touch `getSender()` on each, which fires N more queries. My current code never loads transaction lists, so there's no N+1 today. A future 'history' endpoint would need `JOIN FETCH`, `@EntityGraph(attributePaths = {"sender","recipient"})`, or a DTO projection query."
- **Follow-up:** "Why not just make it EAGER?" → "EAGER always joins, even when unneeded, and EAGER on collections causes cartesian explosions."
- **Likelihood:** High

### 7.8 Q: "How does Spring Data turn `findByName` into SQL? You never wrote an implementation."
- **Answer:** "At startup, Spring Data creates a proxy implementing the repository interface. Methods inherited from `JpaRepository` delegate to `SimpleJpaRepository` (`findById` → `em.find`). For derived query methods, it parses the method name (`find…By` + property `Name`) into JPQL `select u from UserRecord u where u.name = ?1` and validates it at startup. A typo like `findByNmae` fails boot. Honestly, `findByName` is unused in my code (W21)."
- **Likelihood:** Medium

### 7.9 Q: "Explain Kafka basics using your project: topic, partition, offset, consumer group."
- **Follow-up ladder:** "How many partitions do you have and why?" → "What happens if you start a second instance?" → "What's a rebalance?" → "Where are offsets stored?"
- **Strong answer:**
  - "**Topic** `midas-transactions` is a named, append-only log. **Partitions** split it for parallelism. Mine has **1** (KC:17), which caps parallelism at 1 consumer."
  - "Each message in a partition has an **offset**, its sequential position. Ordering is guaranteed only *within* a partition."
  - "**Consumer group** `midas-core`: partitions are divided among the group's consumers, each partition going to exactly one consumer. Committed offsets are stored per group/partition in the internal `__consumer_offsets` topic."
  - "If I started a second instance, a **rebalance** would happen. With 1 partition, one instance stays idle as a hot standby. To scale I'd need more partitions."
  - "`auto-offset-reset: earliest` (core.yml:23) only applies when the group has *no* committed offset. Then it starts from the beginning instead of only new messages."
- **Avoid:** "Kafka deletes the message after it's consumed." Retention is time/size based, independent of consumption.
- **Likelihood:** High

### 7.10 Q: "What's your delivery guarantee? At-most-once, at-least-once, exactly-once?"
- **Follow-ups:** "Prove it — when exactly is the offset committed?" → "Can you get exactly-once with Kafka transactions?" → "Why doesn't that cover your DB?"
- **Strong answer:**
  - "**At-least-once.** Spring Kafka sets `enable.auto.commit=false` and commits offsets itself. The default ack mode is BATCH: after the listener has processed the records from a `poll()`, it commits."
  - "So the order is: DB commit (inside `process`) → listener returns → offset commit. A crash between the two causes redelivery → a duplicate transfer (W1)."
  - "If I committed the offset *before* processing, I'd get at-most-once, which can lose transfers. Worse for money."
  - "Kafka's exactly-once (idempotent producer + transactions) covers **Kafka → Kafka** (read-process-write within Kafka). My side effect is a **DB write**, outside Kafka's transaction. So the real fix is an idempotent consumer: a unique `transaction_id` in the DB, checked inside the same DB transaction. Optionally, store the offset in the DB in the same transaction."
- **Avoid:** Claiming exactly-once. Saying "Kafka guarantees no duplicates."
- **Likelihood:** High

### 7.11 Q: "What is `ErrorHandlingDeserializer` and why do you need it?"
- **Answer:** "Deserialization happens inside the consumer's `poll()`, *before* my listener. Without a wrapper, a malformed record throws in poll. The consumer can't get past it, so it re-polls the same offset forever. That's a **poison pill** that blocks the partition. `ErrorHandlingDeserializer` catches the failure from its delegate `JsonDeserializer` and returns a null value with the exception in a header. The container sees that and passes it to the error handler instead of calling my method. `DefaultErrorHandler` treats `DeserializationException` as **not retryable** by default, so it's logged and skipped right away; the 2 retries don't apply to it."
- **Follow-up:** "Did you test that?" → **Honest:** "No. There's no test that sends malformed JSON (see 11.3). I'd add one."
- **Likelihood:** High

### 7.12 Q: "Why `spring.json.use.type.headers: false` and a default type?"
- **Answer:** "By default, Spring's `JsonSerializer` adds a `__TypeId__` header with the producer's fully qualified class name, and `JsonDeserializer` uses it. That couples the producer and consumer to the same Java class names and packages. It also requires configuring `trusted.packages`, because deserializing arbitrary types from a header is a security risk. I ignore headers and always deserialize to `com.jpmc.midascore.foundation.Transaction` (core.yml:28-29). Any producer, like the console producer in my script, just sends plain JSON. The producer side has `add.type.headers: false` to match (core.yml:34)."
- **Likelihood:** Medium

### 7.13 Q: "Explain your `DefaultErrorHandler` config. What happens on an exception in `process()`?"
- **Answer:**
  - "`new DefaultErrorHandler(new FixedBackOff(500L, 2))` (KC:23). If the listener throws, the transaction has already rolled back. The handler **seeks** the consumer back to the failed offset so it's re-polled, waits 500ms, and retries, up to 2 more times (3 attempts in total)."
  - "After that, the default recoverer just logs the failed record, and the offset moves past it."
  - "Spring Boot auto-wires a `CommonErrorHandler` bean into the default listener container factory, which is why declaring the bean is enough. [Confirm: Boot 3.2 does this for `CommonErrorHandler` beans.]"
  - "Problem (W4): a valid transfer that failed because the DB blipped is lost after ~1s. Better: a `DeadLetterPublishingRecoverer` sending to `midas-transactions.DLT`, `ExponentialBackOffWithMaxRetries` for transient errors, and `addNotRetryableExceptions(...)` for validation-type errors."
- **Snippet (what I'd change it to):**
```java
@Bean
DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<Object, Object> template) {
    var recoverer = new DeadLetterPublishingRecoverer(template,
        (rec, ex) -> new TopicPartition(rec.topic() + ".DLT", rec.partition()));
    var backoff = new ExponentialBackOffWithMaxRetries(5);
    backoff.setInitialInterval(500); backoff.setMultiplier(2.0); backoff.setMaxInterval(10_000);
    var handler = new DefaultErrorHandler(recoverer, backoff);
    handler.addNotRetryableExceptions(IllegalArgumentException.class);
    return handler;
}
```
- **Likelihood:** High

### 7.14 Q: "What does the `NewTopic` bean do? Who creates the topic?"
- **Answer:** "Spring Boot auto-configures a `KafkaAdmin`. At startup it finds `NewTopic` beans and creates missing topics through the AdminClient: 1 partition, replication factor 1 (KC:17). If the topic already exists, it doesn't recreate it. It can *increase* partitions, never decrease them. In production, topics are usually created by infra (Terraform or a platform team), and auto-create is disabled on brokers."
- **Follow-up:** "Can you change 1 → 6 partitions later safely?" → "Kafka allows adding partitions, but the key→partition mapping (`hash(key) % N`) changes. Messages for the same key could temporarily be processed out of order across old and new partitions."
- **Likelihood:** Medium

### 7.15 Q: "How many threads consume messages? Can `process` run concurrently?"
- **Answer:** "One. `@KafkaListener` with default container concurrency 1 gives one consumer thread. That plus 1 partition means `process()` is effectively serial, which is the only reason there's no balance race today (W7). `/balance` requests run on Tomcat worker threads (default max 200) concurrently with that consumer thread, but they're read-only."
- **Likelihood:** High (bridges into Category 8)

### 7.16 Q: "How does `@Value("${incentive.api.url}")` resolve? And `${INCENTIVE_API_URL:http://…}`?"
- **Answer:** "Spring's `Environment` collects property sources in priority order: command-line args > OS env vars > `application.yml`, and more. `${INCENTIVE_API_URL:default}` in the yml means use the env var if it's set, otherwise the default. Relaxed binding also maps env vars like `SPRING_KAFKA_BOOTSTRAP_SERVERS` to `spring.kafka.bootstrap-servers`, so even keys without an explicit placeholder can be overridden. For grouped config, I'd prefer a type-safe `@ConfigurationProperties` record over scattered `@Value`s."
- **Likelihood:** Low-Medium

### 7.17 Q: "What does `@ConditionalOnProperty` do on `DemoDataLoader`?"
- **Answer:** "The bean is only registered if `midas.demo-data.enabled=true`. Tests set it to `false` (`@SpringBootTest(properties = "midas.demo-data.enabled=false")`) so seed users don't interfere with assertions. It's the same mechanism Spring Boot's auto-configuration uses everywhere (`@ConditionalOnClass`, `@ConditionalOnMissingBean`)."
- **Likelihood:** Medium

### 7.18 Q: "What's the bean lifecycle? Why does `SmartInitializingSingleton` run before the Kafka listener starts?"
- **Answer:** "Inside `refresh()`: bean definitions are loaded, then all non-lazy singletons are instantiated (constructor → injection → `@PostConstruct` → `afterPropertiesSet`). Once *all* of them exist, `afterSingletonsInstantiated()` is called on `SmartInitializingSingleton` beans. Then `finishRefresh()` starts `SmartLifecycle` beans. That's where the Kafka `MessageListenerContainer`s start polling and the embedded web server starts accepting traffic. `ApplicationRunner` and `CommandLineRunner` run even later, after refresh. So my seeding is guaranteed to finish before the first message or request."
- **Follow-up:** "Why not `@PostConstruct`?" → "It runs while other beans may not be ready yet. Calling a repository from it usually works, but there's no ordering guarantee relative to other components, and it's not meant for heavy work."
- **Likelihood:** Medium

### 7.19 Q: "How does Jackson handle your records and BigDecimal?"
- **Answer:** "Jackson 2.12+ maps records through their canonical constructor, using the component names as JSON properties. For a `BigDecimal` field, Jackson parses the JSON number text straight into a BigDecimal, with no detour through double, so `150.00` stays exact. When writing, `{"amount": 850.00}` keeps the scale. Unknown properties are ignored by Spring Boot's default ObjectMapper (`FAIL_ON_UNKNOWN_PROPERTIES=false`). The Kafka `JsonDeserializer` builds its own ObjectMapper unless one is injected [Confirm whether it shares Boot's config]."
- **Likelihood:** Low-Medium

### 7.20 Q: "What's HikariCP and how does it matter here?"
- **Answer:** "Spring Boot's default JDBC connection pool. Opening a DB connection is expensive (TCP + auth), so a pool keeps ~10 open (default `maximum-pool-size=10`) and lends them per transaction. Here, each `process()` holds one connection for the whole method, *including* the HTTP call (W2). One consumer thread barely matters today. With concurrency 10 and a slow incentive API, all 10 connections would sit waiting on HTTP, and `/balance` requests would block on `connectionTimeout` (30s default)."
- **Likelihood:** Medium

### 7.21 Q: "Your RestTemplate is a singleton used by many threads. Is that safe?"
- **Answer:** "Yes. `RestTemplate` is thread-safe once built. Its config is immutable, and each call gets its own request. The default request factory (`SimpleClientHttpRequestFactory`, plain `HttpURLConnection`) doesn't really do pooling. Under load I'd plug in Apache HttpClient 5 with a pooled connection manager (max connections per route) for keep-alive reuse."
- **Likelihood:** Low-Medium

---

## Category 8 — Concurrency

### 8.1 Q: "Can two transactions for the same sender corrupt the balance?"
- **Why:** The #1 concurrency question for any money project.
- **Follow-up ladder:** "Show me the interleaving." → "How do you fix it?" → "Optimistic or pessimistic, and why?" → "What about deadlocks?"
- **Strong answer:**
  - "Not today, because consumption is single-threaded (1 partition, concurrency 1). But the code itself isn't safe. Nothing like `@Version` or a lock exists."
  - "Interleaving with 2 threads, Alice has 100, both transfers are 80:"
```
T1: SELECT alice → 100          T2: SELECT alice → 100
T1: canAfford(80) ✓             T2: canAfford(80) ✓
T1: UPDATE balance = 20         T2: UPDATE balance = 20   (blocks on T1's row lock until T1 commits, then overwrites)
T1: COMMIT                      T2: COMMIT
→ Alice = 20, but 160 left her account. Lost update + double spend.
```
  - "That's a **lost update**. READ COMMITTED doesn't prevent it, because each transaction writes a value computed from a stale read."
- **Avoid:** "@Transactional prevents it."
- **Likelihood:** High

### 8.2 Q: "Optimistic vs pessimistic locking. Which would you use here?"
- **Answer:**
  - "**Pessimistic:** `@Lock(LockModeType.PESSIMISTIC_WRITE)` on a repository method → `SELECT … FOR UPDATE`. The second transaction *waits* until the first commits, then reads the fresh balance. Best when conflicts are likely, like a hot account (a merchant or a Blinkit store receiving many payments)."
  - "**Optimistic:** add `@Version Long version` to `UserRecord`. The UPDATE becomes `… WHERE id=? AND version=?`. If 0 rows are updated, Hibernate throws `OptimisticLockException` and I retry. Best when conflicts are rare; no locks are held."
  - "**Third option, often the best here:** an atomic conditional update, with no read-modify-write in Java at all:"
```java
@Modifying
@Query("update UserRecord u set u.balance = u.balance - :amt where u.id = :id and u.balance >= :amt")
int debitIfSufficient(@Param("id") Long id, @Param("amt") BigDecimal amt);
// returns 0 → insufficient funds (or no such user); 1 → debited
@Modifying
@Query("update UserRecord u set u.balance = u.balance + :amt where u.id = :id")
int credit(@Param("id") Long id, @Param("amt") BigDecimal amt);
```
  - "The DB does the check and the write in one statement under a row lock. Credits are commutative, so concurrent credits are safe."
  - "My pick: the conditional update for the hot path, plus `CHECK (balance >= 0)` as a safety net."
- **Follow-up:** "Optimistic retry inside a Kafka listener: where does the retry happen?" → "Either let the exception propagate so `DefaultErrorHandler` retries the whole message (the transaction rolled back, so that's clean), or use Spring Retry `@Retryable` around the transactional call (not inside it)."
- **Likelihood:** High

### 8.3 Q: "With pessimistic locking, can you deadlock?"
- **Answer:** "Yes. T1 transfers A→B and locks A then B; T2 transfers B→A and locks B then A. Each waits for the other. Fix: always lock in a consistent order, e.g. by ascending user id regardless of who is sender or recipient. Also keep transactions short (another reason to move the HTTP call out, W2) and set a lock timeout. Postgres detects deadlocks and aborts one transaction, which should then be retried."
```java
Long first = Math.min(tx.senderId(), tx.recipientId());
Long second = Math.max(tx.senderId(), tx.recipientId());
UserRecord a = userRepository.findByIdForUpdate(first).orElseThrow();
UserRecord b = userRepository.findByIdForUpdate(second).orElseThrow();
```
- **Likelihood:** Medium-High

### 8.4 Q: "If you increase partitions to scale, how do you keep each account consistent?"
- **Answer:** "Key messages by `senderId`. Kafka hashes the key to a partition, so all debits from one account are processed in order by one consumer, and the sender's debits never race. But a transfer also *credits* the recipient, and the recipient's own debits may be in another partition on another consumer. So rows can still be touched concurrently. Credits are additive, so an atomic `balance = balance + x` is safe. Debits still need the conditional update or a lock. Partitioning reduces contention; DB-level atomicity guarantees correctness."
- **Follow-up:** "Messages today have no key. What happens with 6 partitions?" → "Keyless records are spread across partitions (the sticky partitioner fills a batch per partition), so ordering per account is lost."
- **Likelihood:** High

### 8.5 Q: "How would the same message get processed twice? List the ways."
- **Answer:**
  1. "Consumer crashes after the DB commit, before the offset commit → redelivered."
  2. "**Rebalance:** processing is slower than `max.poll.interval.ms` (default 5 min). The broker thinks the consumer is dead and gives the partition to another one, which re-reads from the last committed offset while the first is still finishing."
  3. "Offset commit fails (broker unavailable) → re-read after restart."
  4. "The **producer** retries a send whose ack was lost → a duplicate *message* in the log (unless `enable.idempotence=true`, the default since Kafka 3.0 clients)."
  5. "`DefaultErrorHandler` retry after a partial failure. Here that's harmless, because the DB rolled back. But the incentive API was called again (W3)."
  6. "Upstream just publishes the same business transaction twice (a user double-taps)."
- "Only an **idempotency key** in the message, enforced by a DB unique constraint, covers all six."
- **Likelihood:** High

### 8.6 Q: "Implement idempotent processing."
- **Answer:**
```java
// Message gets a producer-generated id: record Transaction(String transactionId, Long senderId, ...)
// Entity: @Column(nullable=false, unique=true) private String externalId;

@Transactional
public Optional<TransactionRecord> process(Transaction tx) {
    if (transactionRepository.existsByExternalId(tx.transactionId())) {
        log.info("Duplicate {}, skipping", tx.transactionId());
        return Optional.empty();              // fast path
    }
    ... validate, debit, credit ...
    return Optional.of(transactionRepository.saveAndFlush(record)); // the UNIQUE constraint is the real guard
}
// If two copies race, one INSERT hits DataIntegrityViolationException → its transaction rolls back
// → catch it in the listener and treat it as "already processed" (ack, don't retry).
```
- "The `exists` check is just an optimization. The **unique constraint** is the guarantee, because check-then-insert has its own race. Also store rejected IDs, so a duplicate of a rejected transfer isn't re-evaluated later after the balance has changed."
- **Likelihood:** High

### 8.7 Q: "While a transfer is mid-flight, a client calls `/balance`. What do they see?"
- **Answer:** "Under READ COMMITTED they see the last *committed* balance, the value before the transfer. They never see the half-applied state (sender debited, recipient not yet credited), because both UPDATEs only become visible at COMMIT. That's the isolation part of ACID. In Postgres (MVCC), readers don't block on writers; they read the old row version. [Confirm H2's behavior: H2 2.x defaults to MVStore with MVCC-like row versioning.]"
- **Likelihood:** Medium

### 8.8 Q: "Are your entity objects shared between threads?"
- **Answer:** "No. Each transaction has its own EntityManager (persistence context), bound to the current thread. Two threads loading user 1 get two different Java objects. So `UserRecord` being mutable (`debit/credit`) is fine in Java terms. The conflict only shows up in the database, which is where locking has to happen."
- **Likelihood:** Medium

### 8.9 Q: "What about isolation levels? Would SERIALIZABLE fix it?"
- **Answer:** "Yes. SERIALIZABLE (Postgres uses SSI) would detect the read-write conflict and abort one transaction with a serialization failure, which you must retry. It's correct but costly under contention, and every caller needs retry logic. REPEATABLE READ in Postgres also catches *this* lost update ('could not serialize access due to concurrent update'). I'd still prefer the targeted fix (conditional update or `FOR UPDATE`) at READ COMMITTED; it's explicit and cheaper."
- **Follow-up:** "Name the anomalies." → dirty read, non-repeatable read, phantom read, lost update, write skew, and which level prevents which.
- **Likelihood:** Medium

### 8.10 Q: "How would you test for a race condition?"
- **Answer:**
```java
@Test
void concurrentDebitsNeverOverdraw() throws Exception {
    var alice = userRepository.save(new UserRecord("alice", new BigDecimal("100.00")));
    var bob   = userRepository.save(new UserRecord("bob",   new BigDecimal("0.00")));
    int threads = 10;
    var start = new CountDownLatch(1);
    var pool = Executors.newFixedThreadPool(threads);
    List<Future<?>> fs = new ArrayList<>();
    for (int i = 0; i < threads; i++) {
        fs.add(pool.submit(() -> { start.await();
            return transactionService.process(new Transaction(alice.getId(), bob.getId(), new BigDecimal("30.00"))); }));
    }
    start.countDown();                      // release all threads at once
    for (var f : fs) { try { f.get(); } catch (ExecutionException ignored) {} }
    var a = userRepository.findById(alice.getId()).orElseThrow().getBalance();
    var b = userRepository.findById(bob.getId()).orElseThrow().getBalance();
    assertThat(a).isGreaterThanOrEqualTo(BigDecimal.ZERO);
    assertThat(a.add(b)).isEqualByComparingTo("100.00");   // money conserved
}
```
- "Today this would likely **fail**, which is exactly the point. It's a good regression test once locking is added. Run it against Postgres (Testcontainers), since locking semantics differ from H2."
- **Likelihood:** Medium

---

## Category 9 — Failures & edge cases

### 9.1 Q: "The Incentive API is down. What happens?"
- **Answer:** "Connect is refused or times out at 2s, which throws `ResourceAccessException`, caught at IC:43. I log a WARN and return `Incentive.none()`. The transfer completes with incentive 0, which is the spec's choice: availability over the bonus. Tested by `IncentiveClientTest.serverErrorFallsBackToZero` (5xx), but a **timeout isn't tested**."
- **Follow-up ladder:** "The recipient just lost their incentive forever?" → "How would you make it eventually correct?" (W5: store `PENDING`, retry job / outbox) → "What if it's not down but just slow, at 4.9s each time?" (9.2)
- **Likelihood:** High

### 9.2 Q: "It's not down, it's slow — every call takes 4.9 seconds."
- **Answer:** "Worse than down. Each message takes ~5s, so throughput drops to ~0.2 msg/s, and consumer lag grows without bound. And each call holds a DB connection (W2). If processing a batch exceeds `max.poll.interval.ms`, I'd also get rebalances and duplicates. Fixes:"
  - "A **circuit breaker** (Resilience4j): after N failures or slow calls, open the circuit, skip the call instantly and use 0 (or `PENDING`), then probe half-open later."
  - "Tighter read timeout based on the API's SLO (p99 maybe 200ms, not 5s)."
  - "Move the call out of the DB transaction, or make incentives fully async."
  - "Bulkhead: cap concurrent outbound calls."
- **Likelihood:** High

### 9.3 Q: "The database goes down for 30 seconds."
- **Answer:** "`findById` throws (`CannotCreateTransactionException` / `JDBCConnectionException`). Nothing was committed. `DefaultErrorHandler` retries twice at 500ms, which is about 1 second, then **logs and skips**. So every transfer arriving in those 30s is **lost** (W4). Fixes: exponential backoff with longer retries for transient errors, or `DefaultErrorHandler` with a backoff that never gives up, or pause the container while a health check says the DB is down. Anything that does give up goes to a DLT for replay. Alert on DLT size and consumer lag."
- **Note:** With H2 in-memory the DB can't 'go down' separately from the app. Say this applies to the Postgres version.
- **Likelihood:** High

### 9.4 Q: "The app crashes mid-transaction. Walk through each crash point."
- **Answer:**
| Crash right after… | DB state | Kafka offset | Result |
|---|---|---|---|
| poll, before `process` | nothing | not committed | redelivered → processed once ✅ |
| `validate`/`findById` | nothing | not committed | redelivered ✅ |
| incentive HTTP call | nothing | not committed | redelivered; incentive API called twice (OK if it's pure) |
| debit/credit in memory (not flushed) | nothing (rolled back) | not committed | redelivered ✅ |
| DB COMMIT, before offset commit | **applied** | not committed | redelivered → **applied twice** ❌ (W1) |
| offset commit | applied | committed | ✅ |
- "Plus, with H2 in memory a crash wipes *everything*: balances and history. On restart, demo data is reseeded and the committed offsets mean old transfers are not replayed (W8)."
- **Likelihood:** High

### 9.5 Q: "Kafka broker goes down."
- **Answer:** "The consumer can't poll. The Kafka client keeps reconnecting (with `reconnect.backoff.ms`) and logs warnings, but the app stays up and `/balance` keeps working. Nothing is lost, because messages sit in the broker's log. When it comes back, the consumer resumes from the committed offset. *But* my compose setup has a single broker with replication factor 1. If that broker's disk is lost, the messages are gone. In production: 3+ brokers, `replication.factor=3`, `min.insync.replicas=2`, producer `acks=all`."
- **Likelihood:** Medium-High

### 9.6 Q: "Malformed JSON on the topic: `{"senderId": "abc"}` or just `hello`."
- **Answer:** "`JsonDeserializer` fails, and `ErrorHandlingDeserializer` captures it. The container routes it to the error handler, and since `DeserializationException` isn't retryable it's logged and skipped immediately. The partition isn't blocked. **Not covered by a test** (11.3). Improvement: DLT with the raw bytes, so someone can inspect it."
- **Likelihood:** High

### 9.7 Q: "Valid JSON but wrong data: missing amount, amount `0`, negative, sender = recipient, user 99, `"amount": "150.00"` as a string?"
- **Answer:**
  - "Missing field → null in the record → `validate` returns 'missing fields' (TS:78)."
  - "`0` or negative → `signum() <= 0` (TS:81). Sample message 6 (`-20.00`) tests this."
  - "Same user → TS:84. I compare with `Long.equals`, not `==`. `==` on `Long` compares references and only works by accident for values -128..127 (the Integer/Long cache)."
  - "Unknown user 99 → TS:87-91 (sample message 5)."
  - "String `"150.00"` → Jackson coerces numeric strings to BigDecimal by default, so it's accepted. [Confirm: default coercion allows it.]"
  - "All rejections: log + `Optional.empty()` + commit, no retry. That's correct, because retrying a business rejection never helps."
- **Likelihood:** High

### 9.8 Q: "What about edge amounts: `0.001`, `1e30`, or exactly the balance?"
- **Answer:**
  - "Exactly the balance → allowed (`compareTo >= 0`), tested by `spendingTheExactBalanceIsAllowed`."
  - "`0.001` → passes validation (positive), and the scale-3 amount meets a scale-2 column (W9) [Confirm H2 behavior]. I'd reject `scale() > 2`."
  - "`1e30` → no user can afford it, so it's rejected by `canAfford` before touching the DB. NUMERIC(19,2) overflow could only happen on a *credit* to a recipient whose balance is already near 10^17. Theoretical, but a DB error there would roll back and be retried and skipped."
  - "Very long decimals like `0.1000000000000000000001` → BigDecimal handles them; same scale issue."
- **Likelihood:** Medium

### 9.9 Q: "The Incentive API returns `{"amount": 1000000}`."
- **Answer:** "I'd credit it. I only guard against null and negative (IC:39). A buggy or compromised dependency could mint money. Add a business rule cap (`incentive <= amount * maxRate`), log or alert on anomalies, and keep incentives in a separate ledger account so they can be reconciled."
- **Likelihood:** Medium

### 9.10 Q: "A user is deleted between `validate()` and `findById()`."
- **Answer:** "`existsById` passes, then `findById(...).orElseThrow()` throws `NoSuchElementException`. The transaction rolls back and the error handler retries twice, still failing, then skips. The money is safe (nothing committed), but it's noisy, and it shows that `existsById` + `findById` is both redundant and racy (TOCTOU). Using `findById` once and rejecting on empty fixes both. Users aren't deleted anywhere today, though."
- **Likelihood:** Low-Medium

### 9.11 Q: "Messages arrive out of order. Does that matter?"
- **Answer:** "Within my single partition they're processed in log order. Out-of-order matters for balances: 'Bob receives 100' then 'Bob sends 80' succeeds, but in reverse order the send is rejected for insufficient funds. Kafka only guarantees order per partition, so the producer should key by account. Even then, cross-account causality (A pays B, then B pays C) spans partitions. Real systems accept that and let rejections be retried by the user, or use a per-account sequence number."
- **Likelihood:** Medium

### 9.12 Q: "The consumer falls behind. Lag is 2 million messages. What do you do?"
- **Answer:** "First, measure: `kafka-consumer-groups.sh --describe --group midas-core` shows lag per partition. Find the bottleneck. Likely the incentive call or the DB (check processing-time metrics). Short term: if there are more partitions than consumers, add instances. With 1 partition I can't, which is the core scaling limit. Medium term: more partitions keyed by sender, batch listener plus batched DB writes, async incentives. Make sure retention is longer than worst-case lag, or unprocessed messages get deleted."
- **Likelihood:** Medium

### 9.13 Q: "You restart midas-core. What state does it come back in?"
- **Answer:** (W8) "H2 is in-memory and `create-drop`, so the DB is empty, and DemoDataLoader reseeds the 5 users with *original* balances. Kafka still has the committed offset for group `midas-core`, so it resumes *after* the old messages. Result: balances silently reset, while Kafka believes those transfers were applied. `auto-offset-reset: earliest` doesn't help, because it only applies when no offset is committed. For the local demo I run `docker compose down -v` alongside. The real fix is a durable DB."
- **Likelihood:** Medium (a very sharp interviewer catch; volunteering it earns points)

### 9.14 Q: "What happens on shutdown (Ctrl+C / SIGTERM)?"
- **Answer:** "Spring closes the context and stops the listener containers. The consumer finishes the current record, commits offsets and leaves the group, which triggers a rebalance. Tomcat stops. In Boot 3.2 `server.shutdown` defaults to `immediate` (graceful only became the default in Boot 3.4 [Confirm]), so I'd set `server.shutdown=graceful` explicitly so in-flight HTTP requests finish. With `create-drop`, Hibernate drops the schema on close. In Kubernetes: handle SIGTERM, use a `preStop` delay, and give the container a long enough termination grace period."
- **Likelihood:** Low-Medium

### 9.15 Q: "What if the Kafka message value is `null` (a tombstone)?"
- **Answer:** "`validate()` handles `tx == null` (TS:78), so the service is defensive. Whether a null payload even reaches the listener depends on Spring's argument resolution: a non-`@Payload(required=false)` parameter may throw before my code runs, and the error handler then deals with it. [Confirm by testing: send a null-value record.]"
- **Likelihood:** Low

---

## Category 10 — Scaling to 100x / 1M users

### 10.1 Q: "We have 1 million users and 1,000 transfers per second. What breaks first?"
- **Why:** The core "scale it" question. They want a bottleneck-ordered answer, not a buzzword list.
- **Follow-up ladder:** "Fix that. Now what breaks?" → repeat 3–4 times.
- **Strong answer (in order):**
  1. **"Single partition / single consumer thread.** Throughput = 1 / per-message latency. With an incentive call of ~20ms plus ~5ms of DB work, that's about 40 msg/s. At 1,000/s, lag explodes. **Fix:** e.g., 24 partitions keyed by `senderId`, N instances in the consumer group, container concurrency."
  2. **"Concurrency correctness** shows up as soon as I parallelize (8.1). **Fix:** atomic conditional updates or locks, `CHECK (balance >= 0)`, idempotency key."
  3. **"Incentive API latency and availability:** 1,000 calls/s to a dependency. **Fix:** circuit breaker, connection pooling, tight timeouts. Better, make incentives async: commit the transfer, publish an event, and credit the incentive separately."
  4. **"H2 in-process:** can't share it between instances. **Fix:** Postgres with HikariCP sized correctly (pool size × instances < DB `max_connections`), possibly PgBouncer."
  5. **"DB write throughput:** 1,000 transfers/s = 3,000 row writes/s plus WAL. One good Postgres primary can handle that. Batch inserts (switch to SEQUENCE ids), short transactions, indexes kept minimal."
  6. **"Hot accounts** (one merchant receiving thousands/s) cause row-lock contention. **Fix:** sub-accounts or bucketed balances, or a ledger with async aggregation."
  7. **"Reads (`/balance`)** scale separately: read replicas or a cache (10.2)."
- **Avoid:** "Just add Redis and Kubernetes." That has no bottleneck reasoning.
- **Likelihood:** High

### 10.2 Q: "Would you cache balances in Redis?"
- **Follow-ups:** "Cache-aside or write-through?" → "TTL?" → "How do you invalidate?" → "What about stale reads?"
- **Strong answer:**
  - "Carefully. For **display** (app home screen), yes. For **funds checks**, never; those must read the DB under a lock."
  - "Pattern: cache-aside. `GET` checks Redis key `balance:{userId}`; on a miss, read the DB, `SET` with a TTL (e.g., 30–60s)."
  - "Invalidation: after the transfer **commits**, delete both users' keys (`@TransactionalEventListener(phase = AFTER_COMMIT)`). Deleting *before* commit risks repopulating the cache with the old value."
  - "The TTL is a safety net if an invalidation is lost. Remaining race: a reader fills the cache with the old value just before the delete. A short TTL bounds the damage, or version the value."
  - "Honestly, a PK lookup on Postgres is ~1ms, so I'd first try read replicas and only add Redis if DB read load is proven to be the problem."
- **Avoid:** Caching without an invalidation story. Caching values used for funds checks.
- **Likelihood:** High

### 10.3 Q: "Read replicas — any gotcha?"
- **Answer:** "Replication lag. A user transfers money, immediately opens the app, and the replica still shows the old balance. Mitigations: read-your-own-writes (route a user's reads to the primary for a few seconds after they write), show a 'processing' state, or accept a small lag for display. Funds checks always use the primary."
- **Likelihood:** Medium

### 10.4 Q: "How would you shard this database?"
- **Answer:** "Shard `users` by `user_id` (hash or range), with `transactions` stored by sender (or duplicated per participant). The hard part: a transfer between users on **different shards** is a distributed transaction. Options: 2PC (correct but slow and fragile), or a **saga**: debit on shard A with status PENDING, then credit on shard B, then mark COMPLETE, with a compensating credit back to A on failure. Both steps are idempotent and driven by Kafka events. At 1M users I wouldn't shard yet; one Postgres primary with vertical scaling and replicas handles it. Sharding is for much later."
- **Likelihood:** Medium

### 10.5 Q: "How do you scale the REST side? What do you need for horizontal scaling?"
- **Answer:** "Stateless instances behind a load balancer (round-robin or least-connections, health checks on `/actuator/health`). Midas will be stateless once the DB is external. There are no sessions and no in-memory caches holding user state. Then autoscale on CPU or request rate. For the Kafka side, autoscale consumers on **consumer lag** (e.g., KEDA), capped at the partition count."
- **Likelihood:** Medium-High

### 10.6 Q: "Your consumer processes one message at a time. How would you speed up a single consumer?"
- **Answer:** "Batch listener (`List<Transaction>`): fetch 500 records, load all involved users with one `findAllById`, apply them in memory in order, then write in a single transaction with JDBC batching (needs SEQUENCE ids instead of IDENTITY, 4.8). Plus make incentive calls concurrently with a bounded executor, or batch them if the API supports it. Trade-off: one bad record fails the batch; `BatchListenerFailedException` tells the error handler which index failed."
- **Likelihood:** Medium

### 10.7 Q: "Where's the single point of failure?"
- **Answer:** "Everything, honestly: one Kafka broker (RF=1, KC:17 and compose), one in-process DB, one app instance, one incentive API. Production: 3-broker Kafka with RF=3 and `min.insync.replicas=2`; Postgres primary plus a synchronous standby with automatic failover (Patroni or RDS Multi-AZ); multiple app instances across availability zones; circuit breaker plus fallback for the incentive API (already partially done)."
- **Likelihood:** Medium-High

### 10.8 Q: "Would a CDN help here?"
- **Answer:** "No. There's no static content, and balances are per-user, private and changing. A CDN helps for app assets and public, cacheable data (product images and catalog at Blinkit). I wouldn't put balance responses on a CDN."
- **Why it's here:** They check whether you blindly list buzzwords.
- **Likelihood:** Low

### 10.9 Q: "Back-of-envelope: storage for 1 year of transactions at 1,000/s average?"
- **Answer:** "1,000/s × 86,400 ≈ 86M rows/day, ≈ 31.5B/year. Each row is ~8+8+8+~10+~10+8 bytes of data plus ~25 bytes of row overhead, say ~100 bytes with indexes. ≈ 8.6 GB/day, ≈ 3 TB/year. So: partition the table by month (Postgres declarative partitioning), archive old partitions to cheap storage (S3/Parquet), and keep only recent months hot."
- **Likelihood:** Low-Medium

### 10.10 Q: "How would you use a queue more effectively for the incentive part?"
- **Answer:** "Decouple it. `TransactionService` commits the transfer and writes an **outbox** row (`IncentiveRequested{txId}`) in the same DB transaction. A relay publishes it to Kafka (polling or Debezium CDC). An `IncentiveWorker` consumes it, calls the API with retries and an idempotency key, and credits the recipient idempotently (unique on `txId`). The transfer path is now fast and never blocked by the incentive API, and incentives are eventually correct instead of silently zero."
- **Follow-up:** "Why the outbox — why not just `kafkaTemplate.send` after commit?" → "Dual-write problem. The DB commit succeeds and the app crashes before the send, so the event is lost. Or the send succeeds and the commit fails, giving a phantom event. The outbox makes the DB the single source of truth."
- **Likelihood:** Medium-High

---

## Category 11 — Testing, debugging, deployment

### 11.1 Q: "How did you test this? What's your test strategy?"
- **Follow-up ladder:** "Which of these are unit tests vs integration tests?" → "Why mock the IncentiveClient?" → "What's NOT tested?" → "How long does the suite take?"
- **Strong answer (16 tests, each layer with the right Spring test slice):**
  - "**Web layer:** `@WebMvcTest(BalanceController.class)` + `@MockBean BalanceService` loads only the MVC slice. Checks the JSON shape and a 400 for a missing param. Same for `IncentiveControllerTest` (≥100 → 5.00 on 250; 99.99 → 0)."
  - "**Persistence:** `@DataJpaTest` + `@Import(BalanceService)` uses an embedded H2, and each test is rolled back. Stored balance, and unknown user → 0."
  - "**HTTP client:** `@RestClientTest(IncentiveClient)` + `MockRestServiceServer` intercepts the RestTemplate. It verifies the request (URL, POST, JSON body fields), the parsed response, the 5xx fallback, and that a negative incentive is ignored. No real network."
  - "**Service (business rules):** `TransactionServiceTest`, 6 cases: valid transfer, incentive to the recipient only, insufficient funds (and `verify(never()).fetchIncentive` — no API call when rejected), exact balance, unknown users, zero/negative/self/null."
  - "**End-to-end:** `KafkaIntegrationTest` sends 3 messages through a real (embedded) Kafka broker, the middle one exceeding funds, and uses Awaitility to wait until 2 records exist. Then it checks sender 70.00 and recipient 32.00 (incentive mocked at 1.00)."
  - "CI runs `mvn -B verify` on every push to main and on every PR."
- **Avoid:** "100% coverage." I didn't measure it [Confirm: no JaCoCo configured].
- **Likelihood:** High

### 11.2 Q: "Why is `TransactionServiceTest` a `@SpringBootTest` with embedded Kafka? It doesn't test Kafka."
- **Why:** A sharp reviewer will notice (W22).
- **Answer:** "Fair point. It loads the full context, and because the context includes the `@KafkaListener`, it needs a broker, hence `@EmbeddedKafka`. `@DirtiesContext` then throws the context away. It's slower than necessary. Better options: a pure Mockito unit test (`new TransactionService(mockUserRepo, mockTxRepo, mockClient)`) for the rules; or `@DataJpaTest` + `@Import({TransactionService.class})` + `@MockBean IncentiveClient` for real DB behavior; or set `spring.kafka.listener.auto-startup=false`. I kept it as an integration-style test because it checks real JPA dirty checking and the transaction."
- **Follow-up:** "What does `@DirtiesContext` cost?" → "Spring caches application contexts across test classes with the same config. `@DirtiesContext` evicts it, so the next test class rebuilds a context. Needed here so each embedded broker and its bootstrap-servers property don't leak between classes."
- **Likelihood:** Medium

### 11.3 Q: "What's NOT tested?"
- **Answer (honest list):**
  - "Malformed or poison message handling. The README claims it, and there's no test. I'd send raw bytes or `"not json"` with a `KafkaTemplate<String,String>`, then a valid message, and assert the valid one is still processed."
  - "Incentive **timeout** (only 5xx is tested). I'd use `MockRestServiceServer` with a delayed response, or WireMock with a fixed delay."
  - "Concurrency and races (8.10)."
  - "Duplicate message → applied twice. This would currently *fail*, which documents the gap."
  - "DB failure → retry → skip behavior."
  - "The real Postgres dialect. Only H2 is used, and Testcontainers would fix that."
  - "The `DemoDataLoader` seeding and the README's expected-balance table. I verified those manually with the scripts."
  - "Non-numeric `userId` → 400."
- **Likelihood:** High

### 11.4 Q: "What's the difference between `@MockBean` and `@Mock`?"
- **Answer:** "`@Mock` (Mockito) creates a mock object with no Spring involvement; you inject it yourself. `@MockBean` (Spring Boot test) creates a Mockito mock **and replaces the bean of that type in the application context**, so everything that autowires `IncentiveClient` gets the mock. That's how `KafkaIntegrationTest` controls the incentive without a server. Cost: each distinct set of `@MockBean`s creates a different context-cache key, so more contexts are built. (In Boot 3.4+, `@MockBean` is deprecated in favor of `@MockitoBean`.)"
- **Likelihood:** Medium

### 11.5 Q: "How would you test against a real Postgres and real Kafka?"
- **Answer:** "Testcontainers. JUnit starts disposable Docker containers per test run:"
```java
@SpringBootTest
@Testcontainers
class TransferIT {
    @Container @ServiceConnection
    static PostgreSQLContainer<?> pg = new PostgreSQLContainer<>("postgres:16-alpine");
    @Container @ServiceConnection
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));
}
```
- "`@ServiceConnection` (Boot 3.1+) wires the datasource and bootstrap-servers automatically. Combined with Flyway, this tests the real migrations and real locking."
- **Likelihood:** Medium

### 11.6 Q: "A user says their balance is wrong. How do you debug it?"
- **Answer:**
  1. "Get the user id and the time window. Query `transactions` where they're sender or recipient (4.7), and recompute the expected balance from the history (4.6)."
  2. "Search the logs for that user's rejections (INFO 'Rejected …') and incentive fallbacks (WARN 'Incentive API call failed')."
  3. "Check for duplicates: identical (sender, recipient, amount) rows close together in time point to redelivery (W1)."
  4. "Check consumer lag: maybe the transfer just hasn't been processed yet."
  5. "Check whether a restart reset H2 (W8)."
- "Gaps I'd fix: no transaction ID or correlation ID in the logs, and rejections aren't persisted. Both would make this a 1-minute query."
- **Likelihood:** Medium-High

### 11.7 Q: "What logs and metrics would you add? What would you alert on?"
- **Answer:**
  - "Add Spring Boot **Actuator** + **Micrometer** → Prometheus → Grafana. Spring Kafka already publishes listener and consumer metrics through Micrometer once Actuator is present."
  - "Business metrics: `transactions.processed`, `transactions.rejected{reason}`, `incentive.fallback`, processing time histogram (p50/p95/p99), incentive API latency."
  - "Infra: **consumer lag** (the most important one), Hikari pool usage and pending threads, JVM heap and GC."
  - "Alerts: lag rising for 5+ minutes; fallback rate > X%; DLT non-empty; p99 processing time over the SLO; error rate."
  - "Logs: structured JSON with `transactionId`, `traceId` (Micrometer Tracing / OpenTelemetry) carried across Kafka headers and HTTP calls."
- **Likelihood:** Medium-High

### 11.8 Q: "How is this deployed?"
- **Answer (honest):** "It isn't deployed anywhere. You run it locally: Kafka in Docker Compose, and both apps as `java -jar` fat jars built by `spring-boot-maven-plugin`. CI only builds and tests. To deploy:"
```dockerfile
# multi-stage Dockerfile for midas-core
FROM eclipse-temurin:17-jdk AS build
WORKDIR /app
COPY . .
RUN ./mvnw -B -pl midas-core -am package -DskipTests   # (no mvnw in repo today; use a maven image)
FROM eclipse-temurin:17-jre
COPY --from=build /app/midas-core/target/midas-core-1.0.0.jar /app.jar
USER 1000
ENTRYPOINT ["java","-jar","/app.jar"]
```
- "Then add both apps to `docker-compose.yml` with `KAFKA_BOOTSTRAP_SERVERS=kafka:9092` and `INCENTIVE_API_URL=http://incentive-api:8080/incentive` (my env overrides already support this). For K8s: a Deployment with liveness and readiness probes on Actuator health, resource limits, config through env and Secrets, and a CI pipeline: build → test → image → push → deploy."
- **Gotcha to mention:** "Inside Docker, `KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092` (compose) would break containerized clients. They'd need an internal listener advertised as `kafka:9092`."
- **Likelihood:** Medium-High

### 11.9 Q: "What does `mvn verify` do? What's the Maven lifecycle?"
- **Answer:** "Phases run in order: validate → compile → test (Surefire runs unit tests) → package (jar, plus the Boot plugin repackages it into an executable fat jar) → verify (integration-test checks, Failsafe if configured) → install → deploy. `verify` runs everything up to and including verify. In a multi-module build, the parent builds modules in dependency order (the reactor). `-B` is batch mode, for clean CI logs."
- **Likelihood:** Low-Medium

### 11.10 Q: "Explain your CI pipeline. What would you add?"
- **Answer:** "`.github/workflows/ci.yml`: trigger on push to main and on PRs; ubuntu-latest; check out; Temurin JDK 17 with Maven dependency caching; `mvn -B verify`. Additions: JaCoCo coverage report, static analysis (SpotBugs/Checkstyle), dependency vulnerability scanning (Dependabot / OWASP dependency-check), build and push a Docker image on main, Testcontainers integration tests (Docker is available on GitHub runners), and branch protection that requires CI to pass."
- **Likelihood:** Medium

### 11.11 Q: "How do you check Kafka from the command line?"
- **Answer:**
```bash
# what scripts/send-transactions.sh does: pipe JSONL into the console producer inside the container
docker exec -i midas-kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic midas-transactions < file.jsonl
# read the topic
docker exec -it midas-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic midas-transactions --from-beginning
# group lag / offsets
docker exec -it midas-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group midas-core
# reset offsets (app stopped) to replay
... --reset-offsets --to-earliest --group midas-core --topic midas-transactions --execute
```
- **Likelihood:** Low-Medium

### 11.12 Q: "Tell me about a bug you found while testing."
- **Answer:** [Confirm — use a real one. True candidates from this codebase you could have hit:]
  - "Comparing BigDecimal with `equals` failed (`2.0` vs `2.00`), so I switched to `compareTo` / `isEqualByComparingTo`."
  - "Kafka consumer stuck on a bad message until I added `ErrorHandlingDeserializer`."
  - "`TransactionServiceTest` interfering through shared state, fixed with `deleteAll()` in `@BeforeEach` and `@DirtiesContext`."
  - "Async test flakiness, fixed with Awaitility."
- **Avoid:** Inventing one you can't explain in detail.
- **Likelihood:** High

*End of PART 2.*

---

# PART 3 — Question bank, categories 12–16

## Category 12 — Redesign, lessons, and interviewer pushback

### 12.1 Q: "If you had two more weeks — Phase 2 — what would you change, in priority order?"
- **Why:** Tests judgment. Can you rank by impact, or do you just list technologies?
- **Follow-up ladder:** "Why that first?" → "How long would it take?" → "How would you roll it out without downtime?"
- **Strong answer (ranked by risk to money):**
  1. **"Correctness of money:** idempotency key + unique constraint (W1), and an atomic conditional debit or row lock (W7) + `CHECK (balance >= 0)`. These prevent double-applies and overdrafts."
  2. **"Durability:** Postgres + Flyway, and drop `create-drop` (W8). Testcontainers for tests."
  3. **"No silent loss:** DLT + exponential backoff (W4). Persist rejections with a reason (W6)."
  4. **"Resilience:** move the incentive call out of the DB transaction (W2), add a circuit breaker, and make incentives async through an outbox (10.10)."
  5. **"Security:** Spring Security JWT on `/balance`, Kafka SASL + ACLs, H2 console off outside dev (W11–W13)."
  6. **"Operability:** Actuator/Micrometer, consumer-lag alerts, correlation IDs, a Dockerfile, deployment."
  7. **"Cleanup:** remove the unused validation dependency or actually use it, drop `findByName`, add `scale <= 2` validation (W9), return 404 for unknown users."
- **Avoid:** Starting with "rewrite it in microservices / Kubernetes" or "add Redis".
- **Likelihood:** High

### 12.2 Q: "What went wrong, or what would you do differently from day one?"
- **Strong answer (pick the true ones) [Confirm]:**
  - "I'd design the message contract first, *with a transaction ID*. Adding idempotency later means changing the producer contract."
  - "I'd keep transaction boundaries short from the start. I didn't think about the HTTP call holding a DB connection until later."
  - "I'd test the failure paths (poison message, timeout, duplicate) as early as the happy path. The README claims behavior that no test checks."
  - "I'd use Postgres via Testcontainers from the start, so H2-only behavior can't hide bugs."
- **Likelihood:** High

### 12.3 Q: "What did you learn from this project?"
- **Answer:** "Three concrete things. First: **delivery guarantees are about where you commit.** Offset after DB commit gives at-least-once, so the consumer has to be idempotent. Second: **money needs exact types and atomic updates.** BigDecimal and `compareTo`, and one transaction for debit + credit + audit. Third: **failure handling is most of the work.** Timeouts, fallbacks, poison pills and retries took more thought than the happy path."
- **Likelihood:** High

### 12.4 Pushback: "I don't agree with your fallback. Silently giving 0 incentive is wrong. Why not just fail the transaction?"
- **Strong answer:** "It's a business trade-off, and I'd ask the product owner. Failing blocks the user's payment because a *bonus* service is down, so the core flow depends on a non-critical one. Defaulting to 0 keeps payments available but underpays incentives. The best of both: don't decide the incentive synchronously at all. Commit the transfer, record the incentive as PENDING, and pay it when the API recovers. The user gets the payment now and the bonus soon after, and nothing is silently lost."
- **What they test:** Can you defend a decision without being defensive, and do you know a third option?
- **Likelihood:** Medium-High

### 12.5 Pushback: "Why Kafka at all? A REST POST /transfer would be simpler and give the user an instant answer."
- **Answer:** "Agreed it's simpler, and for a small app I might choose it. Kafka earns its place when producers must not depend on my availability, when traffic is bursty (buffering), when multiple consumers need the same event (notifications, analytics, fraud), or when replay matters. A common hybrid: `POST /transfers` validates, writes PENDING and an outbox row, and returns `202 Accepted` with a status URL. Processing then happens asynchronously. The user gets an instant acknowledgment and the system stays decoupled."
- **Likelihood:** Medium-High

### 12.6 Pushback: "Why not just use `synchronized` on `process()` to fix the race?"
- **Answer:** "It only works within one JVM. Two instances, or two consumer threads in two pods, aren't coordinated by a Java lock. It also serializes *all* transfers, even unrelated accounts. The lock has to live where the data is, in the database (row lock, conditional update or version check), or be a distributed lock. I'd avoid a distributed lock for money because of lease expiry edge cases."
- **Likelihood:** Medium-High

### 12.7 Pushback: "Why not Redis for balances? It's way faster than Postgres."
- **Answer:** "Redis is great for counters and caches, but balances are the source of truth for money. I need durable, ACID multi-row transactions with constraints and an audit trail. Redis persistence (RDB/AOF) can lose recent writes on a crash, depending on the fsync policy, and multi-key transactions have weaker guarantees. Postgres handles thousands of updates per second; the bottleneck is contention, not raw speed. So: Postgres as truth, Redis for a read cache at most."
- **Likelihood:** Medium

### 12.8 Pushback: "Isn't this over-engineered for a simulation? Two modules, CI, error handlers…"
- **Answer:** "The core is ~400 lines. The extras each have a reason. The stand-in API makes the repo runnable without the original JAR. CI proves the tests pass on a clean machine. The error handler prevents a single bad message from halting everything, which is one of the most common real Kafka incidents. I deliberately *didn't* add Redis, Docker images or K8s because nothing needed them."
- **Likelihood:** Low-Medium

### 12.9 Pushback: "Optimistic locking is always better because no locks are held. Why would you ever use pessimistic?"
- **Answer:** "Under low contention, yes. Under high contention (a hot merchant or store account), optimistic locking turns into a retry storm: most transactions fail and redo the work, including the external call. Pessimistic locking queues them instead, so each succeeds on the first try. And the conditional `UPDATE … WHERE balance >= x` is effectively a short pessimistic lock with no read-modify-write window. Pick based on contention, measured."
- **Likelihood:** Medium

### 12.10 Pushback: "You said at-least-once plus idempotency. Why not just turn on Kafka's exactly-once?"
- **Answer:** "Kafka EOS (transactional producer + `isolation.level=read_committed` + `sendOffsetsToTransaction`) makes 'consume → produce → commit offset' atomic *within Kafka*. My side effect is a Postgres write, which isn't part of Kafka's transaction. A crash after the DB commit but before the Kafka transaction commits still reprocesses. You'd need 2PC between Kafka and the DB (not supported or practical), or store offsets in the DB (doable, but you then manage offsets yourself). An idempotent consumer is simpler and the industry norm."
- **Likelihood:** Medium

### 12.11 Q: "If you rebuilt this today, what stack would you choose?"
- **Answer:** "Mostly the same. Spring Boot 3.x on Java 21 (virtual threads make blocking HTTP calls cheap), `RestClient` instead of RestTemplate, Postgres + Flyway, Kafka with keyed partitions, Resilience4j, Testcontainers, Actuator + OpenTelemetry. The design changes (idempotency, outbox, atomic updates) matter far more than the stack."
- **Likelihood:** Medium

---

## Category 13 — The Blinkit twist

> The interviewer will map your project onto Blinkit: **orders, inventory, payments, wallet/cashback, notifications, live tracking.** Key insight to say out loud: *"Midas is basically a wallet/payment-ledger service: debit one account, credit another, an external bonus, async events. The same patterns apply to Blinkit's inventory and payments."*

### 13.1 Q: "How would this design map to Blinkit's order flow?"
- **Answer:**
  - "Order placed → `OrderService` writes the order (PENDING) + an outbox event `order.created` in one DB transaction."
  - "Consumers in parallel, each idempotent on `orderId`: **inventory** reserves stock, **payment** charges the user, **notifications** send a push, **analytics** logs it."
  - "It's a **saga**. If payment fails, compensate: release the inventory reservation and mark the order cancelled. Midas's 'validate → apply atomically → audit' is one saga step."
  - "Key everything by `orderId` (order lifecycle events stay ordered), or by `storeId` for store-level streams."
- **Follow-ups:** "Orchestration or choreography?" → "Orchestrator (a central order-state machine) is easier to reason about and monitor at Blinkit's complexity; choreography (pure events) is looser but harder to trace."
- **Likelihood:** High

### 13.2 Q: "Inventory: 1 Maggi packet left in a dark store, two users check out at the same instant. How do you prevent overselling?"
- **Why:** It's **exactly** the same problem as Midas's double-spend (8.1). Make that connection explicitly.
- **Answer:**
  - "Same as my debit: never read-check-write in app code. One atomic statement:"
```sql
UPDATE store_inventory
SET available = available - :qty, reserved = reserved + :qty
WHERE store_id = :store AND sku_id = :sku AND available >= :qty;
-- rows affected = 1 → reserved; 0 → out of stock
```
  - "Reserve at checkout, **confirm** on payment success, **release** on failure or timeout (a TTL job scans expired reservations)."
  - "Hot SKUs during a sale: a Redis `DECRBY` with a Lua script as a fast gate, with the DB as the source of truth and reconciliation. Or pre-split stock into buckets."
  - "`CHECK (available >= 0)` as the last line of defense."
- **Likelihood:** High

### 13.3 Q: "Payments: the payment gateway calls our webhook twice for the same payment. What happens?"
- **Answer:** "Same as my duplicate Kafka message (W1/8.6). The webhook handler must be idempotent: unique constraint on `gateway_payment_id`, insert the payment event first, and if it's a duplicate, return 200 and do nothing. Also **verify the webhook signature** (HMAC with the shared secret) before trusting it, and don't rely on the webhook alone. Reconcile with the gateway's status API, because webhooks can be lost or delayed. Respond fast (enqueue, return 200) so the gateway doesn't time out and retry."
- **Follow-up:** "User clicks Pay twice?" → "Client sends an `Idempotency-Key` (UUID per checkout attempt). The server stores key → result and returns the same result on a replay."
- **Likelihood:** High

### 13.4 Q: "Blinkit gives cashback to the wallet after delivery. How is that like your incentive?"
- **Answer:** "Very similar, and I'd learn from my own shortcut. My incentive is fetched synchronously and silently becomes 0 on failure. For cashback: on `order.delivered`, a consumer computes cashback and writes an idempotent ledger entry (unique on `orderId + type`) crediting the wallet. If the rules service is down, the event is retried or sits in the DLT; it's never silently zero. Wallet balance = sum of ledger entries, cached as a column and updated in the same transaction (double-entry: debit 'cashback expense', credit 'user wallet')."
- **Likelihood:** Medium-High

### 13.5 Q: "How would you build live order tracking (rider location on a map)?"
- **Answer:**
  - "Rider app sends GPS every 3–5s → ingestion API → Kafka `rider.location`, keyed by `riderId`."
  - "A consumer updates the latest location in **Redis** (`GEOADD` / a hash with a TTL). History goes to a time-series or cold store if needed."
  - "Customers connect via **WebSocket** (or SSE) to a tracking gateway, subscribed to their order's rider. The gateway gets updates via Redis pub/sub or a Kafka consumer and pushes them. That's thousands of persistent connections per node, so it scales horizontally with sticky routing or a pub/sub fan-out."
  - "Fallback: polling `GET /orders/{id}/tracking` every few seconds, cached in Redis. Simpler, fine at low scale. Contrast: Midas has no real-time; clients would poll `/balance`."
  - "ETA: computed by a separate service from the route plus traffic; push only on meaningful change."
- **Likelihood:** Medium-High

### 13.6 Q: "Notifications: order placed, packed, out for delivery. How do you design that?"
- **Answer:** "Order-status events on Kafka → a notification service consumes them, looks up user preferences and device tokens, renders templates, and sends via FCM/APNs/SMS providers. Idempotent on `(orderId, status)` so a redelivered event doesn't double-notify. Retries with backoff per provider, a circuit breaker per provider, and a DLT. It's the same 'external API might be down' pattern as my IncentiveClient, but here we **retry** rather than default, because the notification matters."
- **Likelihood:** Medium

### 13.7 Q: "Blinkit's sale at 8 pm: 50x traffic in 10 minutes. What happens to a Midas-like system?"
- **Answer:** "Kafka absorbs the burst. Producers keep writing, and consumer lag grows instead of requests failing. That's the main reason to be event-driven. Then: autoscale consumers on lag (up to the partition count, so provision partitions ahead of time), pre-scale before known events, keep the hot path short (no sync external calls), cache catalog reads aggressively (CDN + Redis), rate-limit at the gateway, and use graceful degradation: turn off non-critical features (recommendations, cashback calculation) to protect checkout and payment."
- **Likelihood:** Medium-High

### 13.8 Q: "Order status should never go backwards (DELIVERED → PACKED). How do you enforce that with async events?"
- **Answer:** "A state machine with allowed transitions, enforced in the DB with a conditional update: `UPDATE orders SET status='DELIVERED', version=version+1 WHERE id=? AND status='OUT_FOR_DELIVERY'`. Zero rows means it's stale or out of order, so ignore and log it. Also key events by `orderId` so they're ordered within a partition, and carry a sequence number or timestamp."
- **Likelihood:** Medium

### 13.9 Q: "How would you model a wallet ledger for Blinkit money?"
- **Answer:**
```sql
accounts(id, user_id, type /*USER_WALLET, CASHBACK_POOL, ...*/, balance NUMERIC(19,2), version)
ledger_entries(id, txn_id, account_id, direction /*DEBIT|CREDIT*/, amount, created_at)
ledger_txns(id, idempotency_key UNIQUE, type, status, created_at)
-- invariant: for each txn, SUM(credits) = SUM(debits)
```
- "Every movement (top-up, payment, refund, cashback) is one `ledger_txn` with balanced entries, written in one DB transaction with the balance updates. Midas's `transactions` table is a simplified version of this."
- **Likelihood:** Medium

### 13.10 Q: "A refund comes in for an order that was paid with wallet + card. How do you keep things consistent?"
- **Answer:** "A refund saga: create a refund record (idempotent on `refundId`), then a wallet credit (ledger entry) and a card refund via the gateway API (async, webhook confirms). Each step is persisted and retried independently, with a status per leg (PENDING → SUCCESS/FAILED). A daily reconciliation job compares our ledger with gateway settlement reports. Never 'fire and forget' money, which is the lesson from my silent incentive fallback."
- **Likelihood:** Low-Medium

### 13.11 Q: "Why would Blinkit care about the BigDecimal / paise discussion?"
- **Answer:** "Prices, discounts, GST and cashback percentages all multiply and round. Pick one representation (integer paise or BigDecimal scale 2), one rounding rule (HALF_UP vs HALF_EVEN, as the finance team decides), and round at defined points (per line item vs per order). Otherwise totals differ by a paisa between services, and reconciliation fails at millions of orders a day."
- **Likelihood:** Low-Medium

---

## Category 14 — Live-coding asks from this project

> They usually ask for 10–20 lines. Talk while you type: the contract → the happy path → validation → errors → transactions → the test you'd write.

### 14.1 "Write a POST /transfers endpoint (synchronous version of your Kafka flow)."
```java
public record TransferRequest(@NotNull Long senderId, @NotNull Long recipientId,
                              @NotNull @Positive @Digits(integer = 17, fraction = 2) BigDecimal amount) {}

@RestController
@RequestMapping("/api/v1/transfers")
class TransferController {
    private final TransactionService service;
    TransferController(TransactionService service) { this.service = service; }

    @PostMapping
    ResponseEntity<?> transfer(@Valid @RequestBody TransferRequest req,
                               @RequestHeader("Idempotency-Key") String key) {
        return service.process(new Transaction(req.senderId(), req.recipientId(), req.amount()))
            .<ResponseEntity<?>>map(r -> ResponseEntity.status(HttpStatus.CREATED).body(Map.of("id", r.getId())))
            .orElseGet(() -> ResponseEntity.unprocessableEntity().body(Map.of("error", "transfer rejected")));
    }
}
```
- Talking points: `@Valid` triggers Bean Validation and gives 400 on failure (this finally uses the starter in the POM). 201 for created; 422 for a business rejection. The key would be passed to the service and stored with a unique constraint (8.6). In real code, `process` should return a reason, not just empty.
- **Likelihood:** High

### 14.2 "Write the JPA entity for a user with optimistic locking."
```java
@Entity @Table(name = "users")
public class UserRecord {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY) private Long id;
    @Column(nullable = false, unique = true) private String name;
    @Column(nullable = false, precision = 19, scale = 2) private BigDecimal balance;
    @Version private Long version;
    protected UserRecord() {}
    public void debit(BigDecimal amt) {
        if (balance.compareTo(amt) < 0) throw new IllegalStateException("insufficient funds");
        balance = balance.subtract(amt);
    }
    public void credit(BigDecimal amt) { balance = balance.add(amt); }
}
```
- **Likelihood:** Medium-High

### 14.3 "Write a repository method for a user's last N transactions, paginated."
```java
public interface TransactionRecordRepository extends JpaRepository<TransactionRecord, Long> {
    @Query("""
           select t from TransactionRecord t
           join fetch t.sender join fetch t.recipient
           where t.sender.id = :userId or t.recipient.id = :userId
           order by t.processedAt desc
           """)
    List<TransactionRecord> history(@Param("userId") Long userId, Pageable pageable);
}
// controller: repo.history(id, PageRequest.of(page, size)) → map to a DTO record (never return entities)
```
- Mention: `join fetch` avoids N+1. Using `Page<>` with fetch joins needs a separate `countQuery`. Prefer keyset pagination for deep pages.
- **Likelihood:** Medium-High

### 14.4 "Write a global exception handler."
```java
@RestControllerAdvice
class ApiExceptionHandler {
    @ExceptionHandler(UserNotFoundException.class)
    ProblemDetail notFound(UserNotFoundException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.getMessage());
    }
    @ExceptionHandler(MethodArgumentNotValidException.class)
    ProblemDetail invalid(MethodArgumentNotValidException e) {
        var pd = ProblemDetail.forStatus(HttpStatus.BAD_REQUEST);
        pd.setProperty("errors", e.getBindingResult().getFieldErrors().stream()
                .map(f -> f.getField() + ": " + f.getDefaultMessage()).toList());
        return pd;
    }
    @ExceptionHandler(Exception.class)
    ProblemDetail fallback(Exception e) {               // log it, don't leak the stack trace
        return ProblemDetail.forStatusAndDetail(HttpStatus.INTERNAL_SERVER_ERROR, "unexpected error");
    }
}
```
- **Likelihood:** Medium

### 14.5 "Write a Kafka consumer with manual acknowledgment."
```java
// application.yml: spring.kafka.listener.ack-mode: manual_immediate
@KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core")
public void onTransaction(ConsumerRecord<String, Transaction> rec, Acknowledgment ack) {
    try {
        transactionService.process(rec.value());   // DB transaction commits inside
        ack.acknowledge();                         // commit offset only after DB commit → at-least-once
    } catch (DataIntegrityViolationException dup) { // duplicate idempotency key → already processed
        ack.acknowledge();
    }
    // any other exception propagates → DefaultErrorHandler retries / DLT
}
```
- **Likelihood:** Medium-High

### 14.6 "Write a Kafka producer that publishes a transaction keyed by sender."
```java
@Service
class TransactionPublisher {
    private final KafkaTemplate<String, Transaction> template;
    TransactionPublisher(KafkaTemplate<String, Transaction> t) { this.template = t; }

    CompletableFuture<SendResult<String, Transaction>> publish(Transaction tx) {
        return template.send("midas-transactions", tx.senderId().toString(), tx)   // key → partition → per-sender order
                .whenComplete((res, ex) -> {
                    if (ex != null) log.error("send failed {}", tx, ex);
                    else log.debug("sent to p{} @{}", res.getRecordMetadata().partition(), res.getRecordMetadata().offset());
                });
    }
}
// producer config: acks=all, enable.idempotence=true (defaults in modern clients)
```
- Note: In Spring Kafka 3.x, `send` returns a `CompletableFuture` (2.x returned `ListenableFuture`).
- **Likelihood:** Medium

### 14.7 "Write the atomic debit/transfer without a race."
```java
@Transactional
public void transfer(long from, long to, BigDecimal amt) {
    if (userRepository.debitIfSufficient(from, amt) == 0)     // UPDATE ... WHERE balance >= :amt
        throw new InsufficientFundsException(from);
    if (userRepository.credit(to, amt) == 0)                  // UPDATE ... balance = balance + :amt
        throw new UserNotFoundException(to);                   // RuntimeException → whole tx rolls back
    transactionRepository.save(new TransactionRecord(userRepository.getReferenceById(from),
                                                     userRepository.getReferenceById(to), amt, BigDecimal.ZERO));
}
```
- `getReferenceById` returns a proxy without a SELECT, which is enough to set the FK. (Queries are in 8.2.)
- **Likelihood:** High

### 14.8 "Write a request-logging / timing filter (middleware)."
```java
@Component
class RequestTimingFilter extends OncePerRequestFilter {
    private static final Logger log = LoggerFactory.getLogger(RequestTimingFilter.class);
    @Override
    protected void doFilterInternal(HttpServletRequest req, HttpServletResponse res, FilterChain chain)
            throws ServletException, IOException {
        String cid = Optional.ofNullable(req.getHeader("X-Correlation-Id")).orElse(UUID.randomUUID().toString());
        MDC.put("cid", cid);
        res.setHeader("X-Correlation-Id", cid);
        long t0 = System.nanoTime();
        try { chain.doFilter(req, res); }
        finally {
            log.info("{} {} -> {} in {} ms", req.getMethod(), req.getRequestURI(), res.getStatus(),
                     (System.nanoTime() - t0) / 1_000_000);
            MDC.clear();     // threads are pooled — always clean up ThreadLocal state
        }
    }
}
```
- **Likelihood:** Medium

### 14.9 "Write a simple token-bucket rate limiter."
```java
class TokenBucket {
    private final long capacity; private final double refillPerNano;
    private double tokens; private long last = System.nanoTime();
    TokenBucket(long capacity, double perSecond) { this.capacity = capacity; this.tokens = capacity; this.refillPerNano = perSecond / 1e9; }
    synchronized boolean tryAcquire() {
        long now = System.nanoTime();
        tokens = Math.min(capacity, tokens + (now - last) * refillPerNano);
        last = now;
        if (tokens >= 1) { tokens -= 1; return true; }
        return false;
    }
}
// per user: ConcurrentHashMap<Long, TokenBucket>; across instances → Redis + Lua script
```
- **Likelihood:** Medium (popular "design + code" crossover)

### 14.10 "Write a circuit breaker around the incentive call (or use a library)."
```java
// Library (what I'd actually use): Resilience4j
@CircuitBreaker(name = "incentive", fallbackMethod = "noIncentive")
@TimeLimiter(name = "incentive")   // only for async return types; otherwise rely on HTTP timeouts
public Incentive fetchIncentive(Transaction tx) { return restTemplate.postForObject(url, tx, Incentive.class); }
private Incentive noIncentive(Transaction tx, Throwable t) { return Incentive.none(); }

// Hand-rolled concept:
enum State { CLOSED, OPEN, HALF_OPEN }
// CLOSED: count failures in a window; > threshold → OPEN (fail fast) for cooldown;
// after cooldown → HALF_OPEN: allow 1 probe; success → CLOSED, failure → OPEN.
```
- **Likelihood:** Medium

### 14.11 "Write a transactional outbox."
```java
@Entity class OutboxEvent { @Id @GeneratedValue Long id; String aggregateId; String type;
                            @Lob String payload; Instant createdAt; boolean published; }

@Transactional
public void process(Transaction tx) {
    ... debit / credit / save record ...
    outboxRepo.save(new OutboxEvent(record.getId().toString(), "IncentiveRequested", json(tx)));  // same DB tx
}

@Scheduled(fixedDelay = 500)
@Transactional
public void relay() {
    for (OutboxEvent e : outboxRepo.findTop100ByPublishedFalseOrderById()) {   // use FOR UPDATE SKIP LOCKED with >1 relay
        kafkaTemplate.send("incentive-requests", e.getAggregateId(), e.getPayload()).join();
        e.setPublished(true);
    }
}
// at-least-once publish → consumers must be idempotent on aggregateId
```
- **Likelihood:** Low-Medium

### 14.12 "Write a unit test for TransactionService using plain Mockito (no Spring)."
```java
@ExtendWith(MockitoExtension.class)
class TransactionServiceUnitTest {
    @Mock UserRepository users; @Mock TransactionRecordRepository txs; @Mock IncentiveClient incentives;
    @InjectMocks TransactionService service;

    @Test void rejectsInsufficientFunds() {
        var alice = new UserRecord("alice", new BigDecimal("10.00"));
        when(users.existsById(anyLong())).thenReturn(true);
        when(users.findById(1L)).thenReturn(Optional.of(alice));
        when(users.findById(2L)).thenReturn(Optional.of(new UserRecord("bob", BigDecimal.ZERO)));

        assertThat(service.process(new Transaction(1L, 2L, new BigDecimal("10.01")))).isEmpty();
        verifyNoInteractions(incentives, txs);
        assertThat(alice.getBalance()).isEqualByComparingTo("10.00");
    }
}
```
- **Likelihood:** Medium-High

### 14.13 "Write the SQL by hand" — see 4.2, 4.6, 4.7. Practice: top-K senders, users with no transactions (LEFT JOIN … IS NULL), second-highest balance, daily totals, duplicates detection:
```sql
-- second highest balance
SELECT MAX(balance) FROM users WHERE balance < (SELECT MAX(balance) FROM users);
-- probable duplicate transfers (same sender/recipient/amount within 1 minute)
SELECT a.id, b.id FROM transactions a JOIN transactions b
  ON a.sender_id = b.sender_id AND a.recipient_id = b.recipient_id AND a.amount = b.amount
 AND a.id < b.id AND b.processed_at - a.processed_at < INTERVAL '1' MINUTE;
-- Nth highest with window function
SELECT * FROM (SELECT u.*, DENSE_RANK() OVER (ORDER BY balance DESC) r FROM users u) x WHERE r = 3;
```
- **Likelihood:** Medium-High

### 14.14 DSA problems an interviewer may spin out of this project (Round 1 has DSA too)
| Problem (as they'd phrase it) | Pattern | Core idea |
|---|---|---|
| "Given a list of transfers, compute final balances." | HashMap | `map.merge(sender, -amt, BigDecimal::add)`; `merge(recipient, +amt, …)`. O(n) |
| "Process transfers in order, reject ones that would overdraw." | Simulation + HashMap | Same as `TransactionService`; O(n) |
| "Minimum number of transactions to settle debts" (Splitwise) | Net balances + backtracking/greedy | Net each person; match max creditor with max debtor (greedy is not always optimal; exact needs backtracking/bitmask DP) |
| "Top K users by total amount sent." | HashMap + min-heap of size K | O(n log k) |
| "Detect if money flows in a cycle A→B→C→A." | Graph cycle detection | DFS with colors (directed graph) or topological sort (Kahn's) |
| "Flag a user who sends > X in any 1-minute window." | Sliding window / deque per user | Monotonic time deque; pop entries older than 60s, keep a running sum |
| "Merge K sorted transaction streams by timestamp." | Min-heap | Like merging Kafka partitions by time; O(N log K) |
| "Design an LRU cache for balances." | HashMap + doubly linked list | O(1) get/put; or `LinkedHashMap(accessOrder=true)` + `removeEldestEntry` |
| "Find duplicate transactions." | HashSet of a composite key | `record Key(sender, recipient, amount, minuteBucket)` |
| "Rate limiter" | Token bucket / sliding log | 14.9 |
| "Two-sum: find two transactions summing to a target." | HashMap complement | O(n) |

- **LRU code (very common):**
```java
class LRUCache<K, V> extends LinkedHashMap<K, V> {
    private final int cap;
    LRUCache(int cap) { super(16, 0.75f, true); this.cap = cap; }   // accessOrder = true
    @Override protected boolean removeEldestEntry(Map.Entry<K, V> e) { return size() > cap; }
}
```
- **Likelihood:** Medium (they like DSA problems tied to the project)

---

## Category 15 — CS fundamentals the project touches (short spoken answers)

### DBMS

**15.1 "What is ACID? Map each letter to your code."** *(High)*
- **A**tomicity: debit + credit + audit row all commit or none (`@Transactional`, TS:49).
- **C**onsistency: constraints hold (NOT NULL, FKs, unique name). I'd add `CHECK (balance >= 0)`.
- **I**solation: concurrent transactions don't see each other's partial state (READ COMMITTED; `/balance` never sees a half-applied transfer).
- **D**urability: committed data survives a crash. **Not true for my H2 in-memory DB**, an honest caveat.

**15.2 "Isolation levels and anomalies."** *(High)*
- READ UNCOMMITTED: dirty reads possible. READ COMMITTED (default in PG/H2): no dirty reads, but non-repeatable reads and lost updates possible. REPEATABLE READ: same row reads stay stable (PG: snapshot; catches the lost update). SERIALIZABLE: as if run one at a time (PG: SSI, may abort; retry needed). Write skew needs SERIALIZABLE or explicit locks.

**15.3 "How does an index work? Why is a B+ tree used?"** *(High)*
- A balanced tree, high fan-out, so few levels (3–4 for millions of rows), giving O(log n) disk page reads. Leaves are linked, so range scans (`ORDER BY processed_at`) are fast. A hash index is O(1) for equality but can't do ranges. Clustered (PK order, e.g. InnoDB) vs secondary indexes (point to the PK/heap). Costs: extra writes and storage. Composite indexes follow leftmost-prefix order.

**15.4 "Normalization — is your schema normalized?"** *(Medium)*
- Yes, roughly 3NF. User data lives only in `users`; transactions reference users by FK. `balance` is technically **derived** data (it could be computed from the transactions), a deliberate denormalization for O(1) reads, kept correct by updating it in the same transaction.

**15.5 "Joins: inner vs left. Write one."** *(Medium)* See 4.7 / 14.13. The left join keeps all left rows with NULLs for non-matches; that's how you find users who never transacted.

**15.6 "Primary key vs unique key? Foreign key?"** *(Medium)*
- PK: unique + not null, one per table, the row identity (`id`). Unique: no duplicates and NULLs are allowed (`users.name`). FK: referential integrity, so `transactions.sender_id` must exist in `users`.

**15.7 "Optimistic vs pessimistic concurrency control?"** *(High)* See 8.2.

**15.8 "What's MVCC?"** *(Medium)* Writers create new row versions; readers see a snapshot as of their statement or transaction, so readers don't block writers and vice versa. Postgres does this (old versions are cleaned up by VACUUM). That's why `/balance` doesn't wait on an in-flight transfer.

**15.9 "SQL vs NoSQL / CAP theorem."** *(Medium-High)*
- CAP: during a network **P**artition you choose **C**onsistency (reject or block) or **A**vailability (serve possibly stale data). Money ledgers choose CP. A product catalog or a rider location can be AP.
- My project: Kafka with RF=1 is neither really. Single-node everything.

### OS / concurrency

**15.10 "Process vs thread? How many threads does your app run?"** *(High)*
- Process: its own address space. Threads share the heap within a process and have their own stacks. My app is 1 JVM process with: Tomcat worker threads (max 200 by default), 1 Kafka consumer thread (plus the client's internal network threads), the Hikari housekeeper thread, GC threads, and so on.

**15.11 "What's a deadlock? The 4 conditions?"** *(High)*
- Mutual exclusion, hold-and-wait, no preemption, circular wait. Break circular wait by locking in a global order (lock the lower user id first, 8.3). Databases detect cycles in the wait-for graph and abort a victim.

**15.12 "What's a race condition? Mutex vs semaphore?"** *(High)*
- A race condition is when the outcome depends on timing between threads. My example: read-check-write balance (8.1). Mutex: one holder at a time (`synchronized`, `ReentrantLock`). Semaphore: N permits (e.g., cap concurrent incentive calls at 20, a bulkhead).

**15.13 "Thread pools — why? What are virtual threads?"** *(Medium)*
- Creating OS threads is expensive, so pools reuse them and bound concurrency (Tomcat, Hikari). Java 21 virtual threads are cheap JVM-managed threads. A blocking call (like my RestTemplate POST) unmounts the virtual thread from its carrier, so you can have thousands of blocking calls without thousands of OS threads. With Java 21 + Boot 3.2: `spring.threads.virtual.enabled=true`.

**15.14 "What's ThreadLocal? Where does Spring use it?"** *(Medium)*
- Per-thread storage. Spring binds the current transaction and EntityManager to the thread (that's how repositories inside `process()` join the same transaction). The logging MDC is ThreadLocal too, so you must clear it in pooled threads (14.8). Caveat: an `@Async` call or another thread does *not* see the transaction.

### Networking

**15.15 "What happens when you run `curl http://localhost:33400/balance?userId=1`?"** *(High — the URL classic, using your own endpoint)*
1. curl parses the URL: scheme http, host localhost, port 33400, path + query.
2. DNS: `localhost` resolves via `/etc/hosts` to 127.0.0.1 (for a real domain: resolver cache → recursive resolver → root → TLD → authoritative).
3. TCP 3-way handshake (SYN, SYN-ACK, ACK) to 127.0.0.1:33400 over the loopback interface. (HTTPS would add a TLS handshake: certificate check, key exchange, session keys.)
4. curl sends `GET /balance?userId=1 HTTP/1.1` with a `Host` header.
5. Tomcat's acceptor gets the socket and a worker thread parses it → DispatcherServlet → controller → service → DB (2.3).
6. Response: `HTTP/1.1 200`, `Content-Type: application/json`, body `{"amount":850.00}`.
7. Keep-alive or close; the TCP FIN handshake.

**15.16 "TCP vs UDP? Which does Kafka use?"** *(Medium)* TCP: connection-oriented, ordered, reliable, with flow and congestion control. UDP: connectionless, no guarantees, low latency (video, DNS, games). Kafka, HTTP/1.1, HTTP/2 and Postgres all use TCP. (HTTP/3 runs on QUIC over UDP.)

**15.17 "HTTP methods, idempotency, status codes."** *(High)* GET (safe, idempotent), PUT/DELETE (idempotent), POST (not idempotent, hence idempotency keys), PATCH (not necessarily). Codes: 200/201/202/204, 400/401/403/404/409/422/429, 500/502/503/504. Which of these my API uses: 5.2.

**15.18 "HTTPS — how does TLS work, briefly?"** *(Medium)* The client hello lists ciphers. The server sends its certificate, and the client verifies the CA chain and hostname. They do an (EC)DHE key exchange to get shared session keys (forward secrecy), then use symmetric encryption (AES-GCM) for the data. My services use plain HTTP, and I'd add TLS/mTLS (6.12).

**15.19 "What's a connection timeout vs a read timeout?"** *(Medium — directly in IC:30-31)* Connect timeout: the maximum time to establish TCP (2s). If the host is down or blackholed, it fails fast. Read (socket) timeout: the maximum time waiting for data on an established connection (5s). If the server is stuck, it fails. There's no *total* request timeout in RestTemplate. A slow drip of bytes can exceed 5s overall, since each read resets the timer.

**15.20 "Load balancer L4 vs L7?"** *(Low-Medium)* L4 routes TCP connections by IP/port (fast, content-blind). L7 understands HTTP: path routing, headers, TLS termination, retries.

### Caching

**15.21 "Cache strategies and eviction."** *(Medium-High)* Cache-aside (my 10.2 choice), read-through, write-through (write cache + DB synchronously), write-back (write to cache, flush later; risky for money). Eviction: LRU, LFU, TTL. Problems: stale data, thundering herd/stampede (fix with a lock or request coalescing, or jittered TTLs), cache penetration (cache negatives or use a Bloom filter).

### OOP / Java

**15.22 "Where is OOP in your code? Encapsulation?"** *(High)*
- `UserRecord` encapsulates its balance: no public setter, only `debit`, `credit` and `canAfford`. The rule lives with the data (a rich domain model). JPA constructors are `protected` so callers can't create invalid empty objects.
- Abstraction: repository interfaces hide JPA. `IncentiveClient` hides HTTP.
- Polymorphism: Spring proxies (CGLIB subclasses) wrap my service. `DefaultErrorHandler` is a strategy implementing `CommonErrorHandler`.
- Composition over inheritance: the service *has* repositories and a client.

**15.23 "SOLID — any examples or violations?"** *(Medium)*
- **S:** Listener only receives, service decides, client calls HTTP. Good. *Violation:* `TransactionService` does validation + orchestration + persistence. Could extract a `TransactionValidator`.
- **O/L:** Not much inheritance here.
- **I:** Small repository interfaces.
- **D:** Constructor-injected abstractions. Though `IncentiveClient` is a concrete class, not an interface (`@MockBean` still works).

**15.24 "Why are records immutable and why does that matter?"** *(Medium)* Fields are final, there are no setters, and `equals/hashCode` are value-based. Safe to share across threads, safe as map keys, and a DTO can't be modified after validation. `BigDecimal` and `Long` are immutable too, so `debit()` *reassigns* `balance = balance.subtract(…)`. Calling `balance.subtract(x)` alone would do nothing, a classic bug.

**15.25 "`==` vs `equals` — where does it matter in your code?"** *(High)*
- `tx.senderId().equals(tx.recipientId())` (TS:84). These are `Long` objects. `==` compares references and only *appears* to work for -128..127 because of `Long.valueOf` caching. User 1000 sending to user 1000 would pass a `==` check as "different".
- `BigDecimal.equals` compares value **and scale** (`2.0` ≠ `2.00`), so I use `compareTo` (UR:44) and `signum()` (TS:81).

**15.26 "equals/hashCode on JPA entities — did you implement them? Should you?"** *(Medium)*
- No. Entities use identity equality from `Object`. That's fine since I never put them in hash sets across sessions. If you implement them, don't use the generated `id` naively: it's null before persist, so the hashCode changes after save and breaks HashSets. Use a natural/business key, or id-based equality with a constant hashCode.

**15.27 "How does a HashMap work?"** *(High — they'll pivot to it from 14.14)* An array of buckets; index = `hash(key) & (n-1)`. Collisions are chained in a list, which becomes a red-black tree beyond 8 entries per bucket (Java 8+). It resizes ×2 at the load factor 0.75. Average O(1), worst O(log n). Keys must have consistent `equals`/`hashCode` and be immutable.

**15.28 "Java memory: stack vs heap, GC."** *(Medium)* Stack: per-thread frames and local variables/references. Heap: objects (entities, BigDecimals), shared. G1 (the default) is a generational, region-based collector: young-generation collections are frequent and cheap, mixed collections handle old regions. The persistence context holding many entities in one big transaction is a classic heap issue in batch jobs.

**15.29 "Checked vs unchecked exceptions — and why it matters for @Transactional."** *(Medium)* Checked must be declared or caught (`IOException`). Unchecked extend `RuntimeException`. Spring rolls back by default only on unchecked (7.2). Spring wraps checked JDBC exceptions into an unchecked `DataAccessException` hierarchy.

---

## Category 16 — Code-specific "why did you write it this way?"

### 16.1 "TS:84 — why `.equals` and not `==` for the IDs?" *(High)*
- See 15.25. Long caching from -128 to 127 makes `==` look correct in tests with small IDs. That's exactly the kind of bug tests with ids 1 and 2 would never catch.

### 16.2 "UR:44 — `balance.compareTo(amount) >= 0`. Why not `equals` or `>`?" *(High)*
- You can't use `>` on objects in Java. `equals` is scale-sensitive. `compareTo` compares numeric value only. `>= 0` allows spending the exact balance (tested by `spendingTheExactBalanceIsAllowed`).

### 16.3 "TS:77 — why does `validate` return `Optional<String>` instead of throwing exceptions?" *(Medium-High)*
- "Rejections are an expected business outcome, not an error. Throwing would roll back (fine) but also propagate to the Kafka error handler, which would **retry** a message that can never succeed, then skip it. Returning a reason keeps control flow explicit and lets me log *why*."
- Pushback: "An `Optional<String>` is a bit stringly-typed." → "Agreed. I'd use a `sealed interface ValidationResult` or an enum `RejectionReason`, so callers can count reasons in metrics or persist them."

### 16.4 "TS:57-58 — `orElseThrow()` after you already validated existence. Why both?" *(Medium)*
- Honest: "Redundant. It's a defensive guard, but it doubles the queries (W10) and has a TOCTOU gap (9.10). I'd fetch once and reject on empty."

### 16.5 "TS:66 — why is the incentive fetched *after* the funds check, not before?" *(Medium-High)*
- "So a rejected transaction never calls the external API. That saves latency and load. `insufficientFundsIsRejected` verifies it with `verify(incentiveClient, never())`. The downside is it's inside the transaction (W2)."

### 16.6 "TS:69 — why does the recipient get `amount + incentive` but the sender is only debited `amount`?" *(Medium)*
- "That's the spec: the incentive is a bonus paid by the system, not by the sender. So total money in the system grows by the incentive, which is why my reconciliation invariant is `sum(balances) = initial + sum(incentives)` (4.6)."

### 16.7 "TS:50 — `process` returns `Optional<TransactionRecord>` but the listener ignores it. Why return it?" *(Low-Medium)*
- "For testability: the tests assert `isPresent()` / `isEmpty()`. It's also useful for a future synchronous REST caller. The listener has nothing to do with it."

### 16.8 "TL:24 — why `log.debug` for received and `log.info` for rejections?" *(Low)*
- "Per-message 'received' logs at INFO would flood logs at volume. Rejections are rarer and actionable. In production I'd count both with metrics rather than rely on logs, and not log raw amounts (W14)."

### 16.9 "IC:39 — why check `incentive == null || incentive.amount() == null || signum() < 0`?" *(Medium)*
- "`postForObject` returns null on an empty body. The JSON might lack `amount`. A negative incentive would *debit* the recipient, which is never acceptable. Each one is tested or explainable (`negativeIncentiveIsIgnored`). Missing: an upper bound (9.9)."

### 16.10 "IC:43 — why catch `RestClientException` and not `Exception`?" *(Medium)*
- "That's the root of RestTemplate's failures: connect or read timeouts (`ResourceAccessException`), 4xx/5xx (`HttpClientErrorException` / `HttpServerErrorException`), and unreadable bodies. Catching `Exception` would also swallow programming bugs like an NPE in my own code and hide them. Narrow catches keep real bugs loud."

### 16.11 "IC:29-32 — why build the RestTemplate in the constructor instead of a `@Bean`?" *(Low-Medium)*
- "It's the only consumer, and the timeouts belong to this client. Using the injected `RestTemplateBuilder` keeps Boot's customizations and lets `@RestClientTest` bind `MockRestServiceServer` to it. If several clients needed it, I'd make a named `@Bean`. (In Boot 3.4+, `setConnectTimeout` / `setReadTimeout` are deprecated in favor of `connectTimeout()` / `readTimeout()` [Confirm].)"

### 16.12 "KC:23 — what exactly does `new FixedBackOff(500L, 2)` mean?" *(High)*
- "500ms interval, maximum **2 retries**, so **3 attempts total**. Then the default recoverer logs and the offset moves on. Not 2 attempts, a common mix-up."

### 16.13 "KC:17 — why 1 partition and replicas 1?" *(High)*
- "Local single-broker setup: you can't have more replicas than brokers. One partition also gives total ordering and makes serial processing safe given my missing locks. That's the honest reason it 'works'. For scale: more partitions + keys + locking (8.4)."

### 16.14 "core.yml:37 — what does `DB_CLOSE_DELAY=-1` do?" *(Medium)*
- "By default an H2 in-memory DB is destroyed when its *last connection* closes. If the pool briefly has zero connections, the data vanishes. `-1` keeps it alive as long as the JVM runs."

### 16.15 "core.yml:42-43 — `create-drop` and `open-in-view: false`?" *(High)* See 4.9 and 7.6.

### 16.16 "core.yml:23 — why `auto-offset-reset: earliest`?" *(Medium)*
- "When the group has no committed offset (first start), read from the beginning, so messages published *before* the service started aren't missed. `latest` would skip them. It doesn't apply after offsets exist (W8)."

### 16.17 "Why port 33400 and the package name `foundation`?" *(Low)*
- [Confirm: these come from the Forage simulation's spec/skeleton. Say so honestly: "the simulation specified the port and the test harness expected it".]

### 16.18 "UR:26 / TR:42 — why `protected` no-arg constructors?" *(Medium)*
- "JPA/Hibernate needs a no-arg constructor to instantiate entities via reflection and to create proxy subclasses. `protected` satisfies that while preventing application code from creating a half-initialized entity. Callers must use the constructor that sets the required fields."

### 16.19 "TR:51 — `processedAt = Instant.now()` in the constructor. Any problem?" *(Medium)*
- "It's hard to test deterministically and uses the app server's clock. Better: inject a `java.time.Clock` (fixed in tests), or use `@CreationTimestamp` or a DB default `now()`. Also, *processed* time isn't *transaction* time. The producer's event timestamp should travel in the message too. I used `Instant` (UTC, unambiguous) rather than `LocalDateTime` (no zone)."

### 16.20 "UR:23 — why `precision = 19, scale = 2`?" *(Medium)*
- "Scale 2 = paise/cents. Precision 19 is the total number of digits, so 17 before the decimal point. 19 matches the digit count of `Long.MAX_VALUE` (~9.2×10^18), and it's a common convention. Without them, Hibernate's defaults for BigDecimal are `NUMERIC(38,2)` [Confirm]; being explicit documents intent."

### 16.21 "TR:25 — `@ManyToOne(fetch = LAZY, optional = false)` — what does `optional=false` do?" *(Medium)*
- "It tells Hibernate the association is never null. The column is NOT NULL, and Hibernate can safely create a lazy proxy without checking whether the FK is null. (With `optional=true`, some to-one cases force eager loading.)"

### 16.22 "UR:59 — `toString` includes id, name and balance. Why override it, and why not include relations?" *(Low-Medium)*
- "For readable logs (DDL logs 'Seeded UserRecord{…}'). Never include lazy associations in `toString`: it triggers loading, possibly outside a session (`LazyInitializationException`), or infinite recursion in bidirectional mappings. `TransactionRecord` has no `toString`, partly for that reason. (Balance in logs is a PII concern, W14.)"

### 16.23 "Incentive.java:8 — `Incentive.none()` returns `BigDecimal.ZERO` (scale 0). Does that matter?" *(Low-Medium)*
- "Not for arithmetic: `amount.add(ZERO)` keeps the amount's scale. It's persisted into a scale-2 column, so it becomes `0.00`. Comparisons use `compareTo`. A static factory gives intent-revealing naming (a Null Object for 'no incentive')."

### 16.24 "IAC:33 — why `HALF_EVEN`? And why `BigDecimal.ZERO.setScale(2)` without a rounding mode at IAC:31?" *(Medium)*
- "HALF_EVEN (banker's rounding) rounds .5 to the nearest even digit, so rounding errors average out over many transactions instead of always rounding up. `setScale(2)` without a mode is safe for zero because no digits are dropped. It throws `ArithmeticException` only if rounding would actually be needed."

### 16.25 "IAC:18-19 — why `new BigDecimal("100.00")` and not `new BigDecimal(100.00)` or `valueOf`?" *(Medium-High)*
- "The String constructor is exact. `new BigDecimal(0.02)` (double) gives `0.0200000000000000004163…` because the double is already imprecise. `BigDecimal.valueOf(0.02)` is also fine (it goes through `Double.toString`). Same idea in DDL:38-42 with the seed balances."

### 16.26 "IAC:21-25 — why are the records nested inside the controller?" *(Low)*
- "The stand-in is tiny and they're only its wire format. Nesting keeps them private to the module. Deliberately *not* shared with midas-core: two services shouldn't share DTO classes, only a contract."

### 16.27 "DDL:34 — `if (userRepository.count() > 0) return;` — why?" *(Low-Medium)*
- "Idempotent seeding. If the DB already has users (e.g., a persistent DB in the future), don't insert duplicates. And with `unique` on `name`, duplicates would actually *crash* startup."

### 16.28 "DDL:43 — `saveAll(users).forEach(u -> log.info(...))`?" *(Low)*
- "`saveAll` returns the saved entities with generated IDs, so the log shows the assigned id 1–5. That's what the scripts and README rely on. Relying on IDENTITY producing 1..5 is a bit fragile, honestly."

### 16.29 "core.yml:28-29, 34 — the `spring.json.*` properties?" *(Medium)* See 7.12.

### 16.30 "Why did you add `spring-boot-starter-validation` if you never use it?" *(Medium)*
- Honest: "It's a leftover. I planned to use Bean Validation on the DTO and ended up validating manually in the service. I'd either annotate the `Transaction` record + `@Valid` or remove the dependency. Unused dependencies add attack surface and confusion." **Don't** claim you use it.

### 16.31 "URepo:10 — `findByName` is never called. Why is it there?" *(Low)*
- Honest: "Unused. Probably from an earlier version or the starter [Confirm]. I'd delete it, or use it if an admin lookup endpoint is added."

### 16.32 Tests — "KafkaIntegrationTest:28 — what's `bootstrapServersProperty`?" *(Medium)*
- "`@EmbeddedKafka` starts an in-JVM broker on a random port and writes its address into `spring.kafka.bootstrap-servers`, so the app's auto-configured consumer and `KafkaTemplate` connect to it automatically. No hard-coded ports, no Docker."

### 16.33 "KafkaIntegrationTest:50 — why wait for `count() == 2` and then check balances?" *(Medium)*
- "The DB is updated asynchronously on the listener thread. Waiting for the 2nd *record* means both valid messages were processed. The 500.00 message sits between them, so if it had blocked the partition, the third would never land. Then balances: sender 100−10−20 = 70; recipient 0 + (10+1) + (20+1) = 32, with the incentive mocked at 1.00."
- Follow-up: "How do you know the rejected one was processed and not still pending?" → "Ordering in one partition. The 3rd message only completes after the 2nd was handled."

### 16.34 "TransactionServiceTest:43-44 — why `deleteAll()` in `@BeforeEach` instead of `@Transactional` on the test?" *(Medium)*
- "With `@SpringBootTest`, a `@Transactional` test would wrap the service call in the *test's* transaction and roll back. That hides real commit and flush behavior. Since the context is shared across tests in the class, I clean up explicitly. Order matters: delete `transactions` first because of the FK to `users`."

### 16.35 "IncentiveClientTest:21 — what does `@RestClientTest(value=..., properties=...)` give you?" *(Low-Medium)*
- "A slice with Jackson + `RestTemplateBuilder` + `MockRestServiceServer` auto-bound to builders, only my `IncentiveClient` bean, and a property override for the URL. The test asserts on the outgoing request (method, URL, JSON fields) as well as the response handling."

### 16.36 "docker-compose.yml — explain `KAFKA_ADVERTISED_LISTENERS` and `CONTROLLER_QUORUM_VOTERS`." *(Medium)*
- "Advertised listeners are what the broker tells clients to connect to after bootstrap. `localhost:9092` works for apps on the host but breaks for other containers (they'd need `kafka:9092`, 11.8). `CONTROLLER_QUORUM_VOTERS=1@localhost:9093` is the KRaft Raft quorum, a single voter (this node). RF=1 for the internal offsets and transaction-state topics, because there's only one broker. `GROUP_INITIAL_REBALANCE_DELAY_MS=0` makes consumers join instantly in dev (the default 3s delay waits for more members)."

### 16.37 "scripts/send-transactions.sh — what does `set -euo pipefail` do? And `grep -c .`?" *(Low)*
- "`-e` exits on any failing command, `-u` errors on unset variables, `pipefail` makes a pipeline fail if any stage fails. Safer scripts. `grep -c .` counts non-empty lines, i.e. the number of messages. `cd "$(dirname "$0")"` makes the default file path work from any directory."

### 16.38 "ci.yml — why `cache: maven` and `-B`?" *(Low)* `setup-java` caches `~/.m2` keyed on the pom hashes, so dependencies aren't re-downloaded every run. `-B` is batch mode: no interactive prompts or download-progress spam.

### 16.39 "pom.xml — why `<relativePath/>` in the parent and what does the Boot parent give you?" *(Low-Medium)*
- "Empty `relativePath` tells Maven not to look for the parent POM in `../`; fetch it from the repository. The Boot parent gives dependency management (a curated, compatible version set, so no versions in my child POMs), plugin config (`spring-boot-maven-plugin` repackaging to a fat jar), the Java version property, and resource filtering."

### 16.40 "Which Java 17 features did you use?" *(Medium)*
- "Records (`Transaction`, `Incentive`, `Balance`, the incentive-api DTOs), `List.of` (immutable factory, Java 9), no-arg `Optional.orElseThrow()` (Java 10), and lambdas and method references (`map(UserRecord::getBalance)`). Text blocks in my snippets; not in the repo. Sealed classes or pattern-matching `switch` would suit a `ValidationResult` (16.3)."

### 16.41 "BS:23-26 — explain that Optional chain."
- "`findById` → `Optional<UserRecord>` → `.map(UserRecord::getBalance)` → `Optional<BigDecimal>` → `.map(Balance::new)` (constructor reference) → `.orElseGet(() -> new Balance(ZERO))`. `orElseGet` builds the fallback lazily, only when it's empty; `orElse` would construct it every time. Trivial here, but the right habit." *(Medium)*

*End of PART 3.*

---

# PART 4 — Coverage check + gap-fill questions

Legend: ✅ covered (question numbers listed) · ➕ was uncovered, new question added below in **Category 17**.

## 4.1 Files (0.1)

| # | File | Covered by |
|---|---|---|
| 1 | README.md | ✅ 1.1, 1.2, 1.7, 0.6 table; ➕ 17.3 (expected-balance table) |
| 2 | pom.xml (parent) | ✅ 16.39, 3.10, 11.9; ➕ 17.4 (versions) |
| 3 | midas-core/pom.xml | ✅ 16.30, 11.9, 3.11 |
| 4 | incentive-api/pom.xml | ✅ 2.5; ➕ 17.2 |
| 5 | docker-compose.yml | ✅ 3.9, 16.36, 11.8, 9.5 |
| 6 | .github/workflows/ci.yml | ✅ 11.10, 16.38 |
| 7 | .gitignore | ➕ 17.1 |
| 8 | midas-core application.yml | ✅ 16.14–16.16, 7.12, 7.16, 7.6, 4.9, 6.7; ➕ 17.5, 17.6 |
| 9 | incentive-api application.yml | ➕ 17.2 |
| 10 | scripts/send-transactions.sh | ✅ 11.11, 16.37 |
| 11 | scripts/check-balances.sh | ➕ 17.3 |
| 12 | scripts/sample-transactions.jsonl | ✅ 9.7; ➕ 17.3 |
| 13 | MidasCoreApplication.java | ✅ 7.1 |
| 14 | TransactionListener (TL) | ✅ 1.2, 7.15, 14.5, 16.8 |
| 15 | TransactionService (TS) | ✅ 1.2, 4.5, 7.2, 7.5, 8.1, 16.1–16.7 |
| 16 | BalanceService (BS) | ✅ 2.3, 7.3, 16.41 |
| 17 | BalanceController (BC) | ✅ 2.3, 5.1–5.3, 6.1 |
| 18 | IncentiveClient (IC) | ✅ 2.4, 3.7, 9.1, 9.2, 16.9–16.11 |
| 19 | KafkaConfig (KC) | ✅ 7.13, 7.14, 16.12, 16.13 |
| 20 | DemoDataLoader (DDL) | ✅ 3.12, 7.17, 7.18, 16.27, 16.28 |
| 21 | UserRecord (UR) | ✅ 4.1, 14.2, 15.22, 16.2, 16.18, 16.20, 16.22 |
| 22 | TransactionRecord (TR) | ✅ 4.1, 4.4, 7.7, 16.19, 16.21 |
| 23 | foundation/Transaction.java | ✅ 3.8, 7.12; ➕ 17.9 (message contract) |
| 24 | foundation/Incentive.java | ✅ 16.23 |
| 25 | foundation/Balance.java | ✅ 5.8; ➕ 17.10 |
| 26 | UserRepository | ✅ 7.8, 16.31 |
| 27 | TransactionRecordRepository | ✅ 14.3, 4.7 |
| 28 | IncentiveApiApplication.java | ✅ 2.5; ➕ 17.2 |
| 29 | IncentiveController (IAC) | ✅ 2.5, 5.9, 16.24–16.26 |
| 30 | TransactionServiceTest | ✅ 11.1, 11.2, 16.34 |
| 31 | KafkaIntegrationTest | ✅ 11.1, 16.32, 16.33, 3.11 |
| 32 | IncentiveClientTest | ✅ 11.1, 16.35 |
| 33 | BalanceControllerTest / BalanceServiceTest | ✅ 11.1, 5.2; ➕ 17.7 (test slices) |
| 34 | IncentiveControllerTest | ✅ 11.1; ➕ 17.7 |

## 4.2 Routes / entry points (0.2)

| Route | Covered by |
|---|---|
| `GET /balance?userId=` | ✅ 2.3, 5.1, 5.2, 5.3, 5.8, 6.1, 6.11, 15.15 |
| `POST /incentive` | ✅ 2.5, 5.6, 5.9, 16.24 |
| `/h2-console` | ✅ 6.7, W13 |
| Kafka topic `midas-transactions` / group `midas-core` | ✅ 7.9, 7.10, 6.6, 11.11 |
| Outbound `POST ${incentive.api.url}` | ✅ 2.4, 6.12, 9.1, 9.2 |
| Startup hook `afterSingletonsInstantiated` | ✅ 3.12, 7.18 |

## 4.3 DB models (0.3)

| Item | Covered by |
|---|---|
| `users` table, fields, unique name | ✅ 4.1, 4.2, 4.10 |
| `transactions` table, LAZY FKs | ✅ 4.1, 4.2, 4.4, 16.21 |
| Indexes (present + missing) | ✅ 4.3, 15.3 |
| No `CHECK`, no `@Version` | ✅ 8.2, 14.2 |
| IDENTITY generation | ✅ 4.8 |
| `ddl-auto: create-drop`, no migrations | ✅ 4.9, 9.13 |
| `NUMERIC(19,2)` | ✅ 3.6, 16.20 |
| `processed_at` Instant | ✅ 16.19 |

## 4.4 Libraries / services (0.4)

| Library | Covered by |
|---|---|
| Spring Boot 3.2.5 + managed versions | ✅ 16.39; ➕ 17.4 |
| Web MVC / Tomcat | ✅ 2.3, 15.10, 15.15 |
| Spring Data JPA / Hibernate | ✅ 3.5, 7.4–7.8 |
| H2 | ✅ 3.2, 16.14 |
| Spring for Apache Kafka | ✅ 7.9–7.15 |
| ErrorHandlingDeserializer + JsonDeserializer | ✅ 7.11, 7.12 |
| Kafka broker 3.7 KRaft | ✅ 3.9, 16.36 |
| RestTemplate / RestTemplateBuilder | ✅ 3.7, 7.21, 16.11 |
| Jackson | ✅ 7.19 |
| SLF4J / Logback | ✅ 16.8; ➕ 17.5 |
| spring-boot-starter-validation (unused) | ✅ 5.4, 16.30 |
| JUnit 5 / Mockito / MockMvc / MockRestServiceServer / @DataJpaTest / @EmbeddedKafka / Awaitility | ✅ 11.1, 11.4, 3.11, 16.32, 16.35; ➕ 17.7 |
| GitHub Actions / Temurin 17 | ✅ 11.10, 16.38 |

## 4.5 Config / env variables (0.5)

| Key | Covered by |
|---|---|
| `server.port` 33400 / 8080 | ✅ 16.17; ➕ 17.2 |
| `general.kafka-topic` | ✅ 7.14, 7.16 |
| `incentive.api.url` / `INCENTIVE_API_URL` | ✅ 7.16, 6.12 |
| `midas.demo-data.enabled` | ✅ 7.17 |
| `spring.kafka.bootstrap-servers` / `KAFKA_BOOTSTRAP_SERVERS` | ✅ 7.16, 16.32 |
| `consumer.group-id` | ✅ 7.9 |
| `auto-offset-reset: earliest` | ✅ 16.16, 9.13 |
| Consumer deserializers / delegate | ✅ 7.11 |
| `spring.json.value.default.type`, `use.type.headers` | ✅ 7.12 |
| Producer serializers, `add.type.headers` | ✅ 7.12; ➕ 17.6 |
| Datasource URL / `sa` / empty password | ✅ 16.14, 6.7, 6.9 |
| `ddl-auto` | ✅ 4.9 |
| `open-in-view` | ✅ 7.6 |
| `h2.console.*` | ✅ 6.7 |
| `logging.level.com.jpmc.midascore` | ➕ 17.5 |
| Hard-coded timeouts 2s/5s | ✅ 2.4, 15.19 |
| Hard-coded backoff 500ms × 2 | ✅ 16.12 |
| Hard-coded partitions/replicas 1/1 | ✅ 16.13 |
| Hard-coded threshold 100.00 / rate 0.02 | ✅ 16.25, 2.5 |
| Compose KRaft env vars | ✅ 16.36 |
| Script `TOPIC` / `$1` | ✅ 16.37 |

## 4.6 README claims (0.6)

| Claim | Covered by |
|---|---|
| R1 overview | ✅ 1.1 |
| R2 Forage, Dec 2025 | ✅ 1.3, 1.8 |
| R3 badges (Java 17, Boot 3.2.5, Kafka 3.7, 16 tests) | ✅ 11.1; ➕ 17.4 |
| R4 malformed message skipped | ✅ 7.11, 9.6, 11.3 |
| R5 four rejection rules | ✅ 9.7, 16.1–16.3 |
| R6 incentive fallback 0 | ✅ 9.1, 12.4 |
| R7 single @Transactional unit | ✅ 4.5, 7.2, 2.4 (W2) |
| R8 expected balances table | ➕ 17.3 |
| R9 `/balance` example | ✅ 2.3 |
| R10 H2 console | ✅ 6.7 |
| R11 config table | ✅ 7.16 |
| R12 tests need no Docker | ✅ 16.32, 11.5 |
| R13 BigDecimal, "no rounding errors" | ✅ 3.6, 4.12, 9.8 |
| R14 2s/5s timeouts | ✅ 2.4, 15.19 |
| R15 2 retries then skip | ✅ 7.13, 9.3, 16.12 |
| R16 env overrides | ✅ 7.16 |
| R17 next steps | ✅ 12.1 |
| R18 stand-in Incentive API | ✅ 2.5 |

## 4.7 Weak spots (Step 2)

| W | Covered by | W | Covered by |
|---|---|---|---|
| W1 idempotency | 7.10, 8.5, 8.6, 9.4 | W14 PII in logs | 6.10, 16.8 |
| W2 HTTP inside tx | 2.4, 9.2, 7.20 | W15 no rate limit/validation | 5.4, 6.11 |
| W3 retry re-calls API | 8.5 | W16 1 partition/1 replica | 16.13, 10.7 |
| W4 skip loses transfers | 7.13, 9.3 | W17 blocking per-message | 10.1, 10.6 |
| W5 silent fallback | 9.1, 12.4, 13.4 | W18 single instance/no cache | 10.2, 10.5 |
| W6 rejections not stored | 12.1, 11.6 | W19 IDENTITY no batching | 4.8 |
| W7 no locking | 8.1–8.3 | W20 unknown user 200 | 5.3 |
| W8 restart inconsistency | 9.13 | W21 unused code | 16.30, 16.31, 17.6 |
| W9 scale not validated | 4.12, 9.8 | W22 heavy service test | 11.2 |
| W10 redundant queries | 7.5, 16.4 | W23 no error body/versioning | 5.5, 5.7 |
| W11 no auth / IDOR | 6.1 | W24 no Docker/metrics | 11.7, 11.8 |
| W12 Kafka unauthenticated | 6.6 | W25 no git commits | 1.8 |
| W13 H2 console exposed | 6.7 | | |

**Result:** every inventory item now maps to at least one question.

---

## Category 17 — Gap-fill questions (items that had no dedicated question)

### 17.1 Q: "What's in your `.gitignore`? Have you ever committed something you shouldn't?"
- **Why:** Tests repo hygiene and secrets awareness. It also lets them ask about your git history.
- **Follow-up ladder:** "How would you remove a committed secret?" → "Is deleting it in a new commit enough?"
- **Strong answer:**
  - "It ignores build output (`target/`, `*.class`), logs, and IDE folders (`.idea/`, `*.iml`, `.vscode/`, `.DS_Store`). So only source, config and scripts are committed, and the jars are rebuilt by CI."
  - "There are no secrets to leak. The only credential is H2's `sa` with an empty password, and external URLs come from env overrides."
  - "If a secret were committed: **rotate it first**. Deleting it in a new commit isn't enough because it stays in history. Then rewrite history (`git filter-repo` or BFG) and force-push. Prevent it in future with a pre-commit hook or gitleaks in CI, and a `.env` file that is gitignored."
- **Avoid:** "I'd just delete the file."
- **Likelihood:** Low

### 17.2 Q: "How do the two services find each other locally? What if port 8080 is already taken?"
- **Why:** Checks you understand the incentive-api module and its config (`server.port: 8080`), not just midas-core.
- **Follow-ups:** "How would this work in Docker or Kubernetes?" → "What's service discovery?"
- **Strong answer:**
  - "incentive-api is its own Boot app (`IncentiveApiApplication`) on port 8080. midas-core is on 33400. midas-core calls `incentive.api.url`, which defaults to `http://localhost:8080/incentive` (core.yml:9)."
  - "If 8080 is taken, start the stand-in with `--server.port=8081` (or `SERVER_PORT=8081`) and point midas at it with `INCENTIVE_API_URL=http://localhost:8081/incentive`. No code change. That's why the URL is externalized."
  - "In Docker Compose, the services resolve each other by service name (`http://incentive-api:8080`). In Kubernetes, a `Service` gives a stable DNS name. With Eureka/Consul, clients look up instances dynamically. Today it's static config."
- **Avoid:** Not knowing that 8080 is the Spring Boot default and is set explicitly in the incentive yml.
- **Likelihood:** Low-Medium

### 17.3 Q: "Your README shows Bob ending at 577.75. Walk me through how the sample batch gets there."
- **Why:** Proves you understand your own rules end to end: the threshold, rejections, and the incentive going to the recipient only. The README, `sample-transactions.jsonl` and `check-balances.sh` all tie together here.
- **Follow-ups:** "Why was message 3 rejected?" → "Which messages called the incentive API?" → "How did you verify it — automated or manual?"
- **Strong answer (trace aloud):**

| # | Message | Outcome | Balances after |
|---|---|---|---|
| start | — | — | alice 1000, bob 500, carol 250, dave 0, erin 2000 |
| 1 | 1→2, 150.00 | ✅ ≥100, so incentive 2% = 3.00 | alice 850, bob 653 |
| 2 | 2→3, 75.25 | ✅ below 100, so incentive 0 | bob 577.75, carol 325.25 |
| 3 | 4→1, 10.00 | ❌ dave has 0.00, insufficient funds (no incentive call) | — |
| 4 | 5→4, 300.00 | ✅ incentive 6.00 | erin 1700, dave 306 |
| 5 | 3→99, 5.00 | ❌ unknown recipient 99 | — |
| 6 | 1→5, −20.00 | ❌ amount must be positive | — |

  - "Final: 850 / 577.75 / 325.25 / 306 / 1700, which matches the README. Total = 3759.00 = 3750.00 initial + 9.00 incentives. That's my conservation invariant (4.6)."
  - "Only messages 1, 2 and 4 called the incentive API. Message 3 is rejected before the call (16.5)."
  - "Verification: manual. `check-balances.sh` (curl for users 1–5), then `send-transactions.sh`, then `check-balances.sh` again. The automated equivalent is `KafkaIntegrationTest`, which uses its own data and a mocked incentive. I'd add a test that replays the sample file and asserts the README table, so the docs can't drift."
- **Avoid:** Fumbling the arithmetic. Practise this once out loud.
- **Likelihood:** Medium

### 17.4 Q: "What versions are you on? What changed going from Spring Boot 2 to 3?"
- **Why:** The README badges invite this. It also tests whether you understand the `jakarta.*` imports in your own entities.
- **Follow-ups:** "How do you find the exact Hibernate or Kafka client version?" → "How would you upgrade safely?"
- **Strong answer:**
  - "Java 17 and Spring Boot 3.2.5 (parent POM). Everything else is managed by Boot's BOM, so child POMs have no versions. To see the resolved versions: `mvn dependency:tree`. [Confirm the exact Spring Kafka, kafka-clients, Hibernate and H2 versions.] The Kafka broker image is 3.7.0, and that's what the badge refers to."
  - "Boot 3 changes: Java 17 is the minimum; Java EE moved to **Jakarta EE 9+**, so imports went from `javax.persistence` to `jakarta.persistence` (see UR and TR); Hibernate 6; Spring Framework 6 (`ProblemDetail`, `RestClient` in 6.1); native-image (AOT) support; Micrometer Observation for tracing."
  - "Safe upgrade: bump the parent one minor version at a time, read the release notes and deprecations, run `spring-boot-properties-migrator` to flag renamed properties, and run the full test suite in CI."
- **Avoid:** Claiming the Kafka *client* is 3.7 without checking.
- **Likelihood:** Low-Medium

### 17.5 Q: "How is logging set up? Why `log.info("Rejected {}: {}", …)` and not string concatenation?"
- **Why:** Covers SLF4J/Logback and the `logging.level` config (core.yml:49-51), and checks basic performance awareness.
- **Follow-ups:** "What log levels exist and when do you use each?" → "How would you change a level in production without redeploying?" → "Structured logging?"
- **Strong answer:**
  - "SLF4J is the API (a facade); Logback is the implementation that `spring-boot-starter-logging` brings in. Every class has `private static final Logger log = LoggerFactory.getLogger(...)`."
  - "`{}` placeholders are only formatted if that level is enabled. With concatenation, `"Received " + tx` builds the string, calling `toString()`, even when DEBUG is off. That matters on a per-message path like TL:24."
  - "Levels: ERROR (needs a human), WARN (degraded but working, like my incentive fallback at IC:44), INFO (business events, like rejections), DEBUG/TRACE (diagnostics). `logging.level.com.jpmc.midascore: INFO` sets my package; framework packages use Boot's defaults."
  - "Changing it at runtime: Actuator's `/actuator/loggers` endpoint (secured). For production: JSON logs (Boot 3.4+ has built-in structured logging [Confirm]; otherwise logstash-logback-encoder) with a correlation id in the MDC (14.8), shipped to ELK or Loki."
- **Avoid:** "I use System.out.println."
- **Likelihood:** Low-Medium

### 17.6 Q: "midas-core only consumes. Why does its config have producer serializers?"
- **Why:** A sharp reader of core.yml:30-34 will ask this.
- **Strong answer:** "Main code doesn't produce anything. The producer config is used by the tests: `KafkaIntegrationTest` autowires Boot's `KafkaTemplate`, which sends `Transaction` objects as plain JSON (`add.type.headers: false`, to match the consumer that ignores type headers). Strictly, it belongs in `src/test/resources/application.yml` or the test's `properties`. It'll be needed in main code once I publish result or DLT events (7.13, 10.10)."
- **Avoid:** Inventing a producer that doesn't exist.
- **Likelihood:** Low

### 17.7 Q: "You use `@WebMvcTest`, `@DataJpaTest`, `@RestClientTest` and `@SpringBootTest`. What's the difference, and why `@MockBean BalanceService` in the controller test?"
- **Why:** Covers BalanceControllerTest, BalanceServiceTest and IncentiveControllerTest in depth.
- **Follow-ups:** "Does `@DataJpaTest` use your application.yml datasource?" → "Do `@DataJpaTest` tests commit?"
- **Strong answer:**
  - "`@WebMvcTest(BalanceController.class)` loads only the MVC slice: controllers, Jackson, filters, `MockMvc`. No services, repositories, DB or Kafka. The controller needs a `BalanceService`, so I provide a `@MockBean`, otherwise the context fails to start. It's fast and isolates HTTP concerns (status codes, JSON shape, a 400 for a missing param)."
  - "`@DataJpaTest` loads JPA only: entities, repositories, an EntityManager. It **replaces the datasource with an embedded DB** by default and makes each test `@Transactional` with **rollback**, so tests don't leak data. I `@Import(BalanceService.class)` because services aren't in the slice."
  - "`@RestClientTest` gives Jackson, RestTemplateBuilder and `MockRestServiceServer`."
  - "`@SpringBootTest` loads the whole application (needed for the Kafka end-to-end test)."
  - "IncentiveControllerTest asserts `jsonPath("$.amount").value(5.00)`. JsonPath compares the parsed JSON number to the expected number, so `5.00` vs `5.0` works. A small gotcha: the response serializes as `5.00`, but `value(0)` for the zero case also passes."
- **Likelihood:** Medium

### 17.8 Q: "Your `IncentiveApiApplication` is basically empty. What would a *real* incentive service need?"
- **Strong answer:** "Rules stored in a DB or config (campaigns, date ranges, per-user caps), not hard-coded `THRESHOLD`/`RATE`. Idempotency on a transaction ID, because payouts have side effects. Auth for service-to-service calls (mTLS or client credentials). An audit trail of each decision. Rate limits. Probably an async API (event in, event out), so payments don't depend on it synchronously (10.10)."
- **Likelihood:** Low

### 17.9 Q: "Your Kafka message is just `{senderId, recipientId, amount}`. What's missing from that contract?"
- **Why:** `Transaction.java` is the system's most important interface, and it lacks the fields needed for the fixes discussed earlier.
- **Follow-ups:** "How do you evolve the schema without breaking consumers?" → "What's a schema registry?"
- **Strong answer:**
  - "Missing: `transactionId` (idempotency, W1), `occurredAt` (event time vs `processedAt`), `currency`, `initiatedBy` (authorization and audit, 6.6), and a `schemaVersion`. A message key (`senderId`) is missing too, which matters for ordering (8.4)."
  - "Evolution: only add optional fields with defaults, and never rename or remove. Jackson ignoring unknown fields gives forward compatibility. For strictness, use Avro or Protobuf with Confluent Schema Registry and enforce BACKWARD compatibility, so a producer can't publish a breaking schema."
- **Likelihood:** Medium

### 17.10 Q: "Why wrap the balance in a `Balance` record instead of returning a bare `BigDecimal`?"
- **Strong answer:** "A JSON **object** (`{"amount": 850.00}`) can be extended later (`currency`, `asOf`, `pending`) without breaking clients. A bare number can't. It also matched the simulation's expected response shape. It's a DTO, so the response stays decoupled from the `UserRecord` entity and lazy loading or extra fields can't leak."
- **Likelihood:** Low-Medium

*End of PART 4. Document complete (Parts 1–4).*

---

# Gap-check additions

> Second pass by a **tougher interviewer who has read the whole bank above**. Every question below is **new**: not in Categories 1–17. They include follow-ups to the bank's own answers, odd "what if"s, line-level code questions, versions, config values, commit history, README claims, and questions triggered when *you* mention a related technology (Redis, Docker, Kafka internals, WebSockets, Kubernetes, Postgres, JWT, CDC, gRPC, virtual threads).
> Numbering: **GC.n**. Path aliases as defined at the top. Format is the same as the rest of the bank.

### Highest-value gap additions (learn these first)
GC.1 (AI use), GC.4 (README badge honesty), GC.29 (`max.poll.records` × slow API = rebalance storm), GC.37 (blocking retries), GC.49 (returning an entity outside the tx), GC.52 (non-RestClient exception), GC.58 (timed-out-but-paid incentive), GC.84 (`"currency":"USD"` silently ignored), GC.111 (`@Modifying` stale persistence context), GC.112 (catching the duplicate-key exception from inside a rolled-back tx).

---

## A. Commit history, authorship & README claims

### GC.1 Q: "Did you use AI tools (ChatGPT/Copilot/Claude) to write this? How much?"
- **Why they ask:** Very common now. They test honesty and whether you can own code you didn't type.
- **Follow-up ladder:** "Which parts?" → "Change something live: add a rejection reason to the DB." → "Explain a line I pick at random."
- **Strong answer:**
  - [Confirm. Answer truthfully.] "I used AI for X (e.g., boilerplate, test scaffolding, README wording). I designed and verified the logic myself, and I can explain every line."
  - "My proof is that I can change it: add a `status` column for rejections, or swap in a conditional UPDATE, right now."
  - "Where I verified things myself: I traced the README sample batch by hand, and I checked the retry semantics of `FixedBackOff`."
- **Avoid:** Flat denial if it isn't true. Saying "AI wrote it, I just ran it".
- **Likelihood:** High

### GC.2 Q: "If I run `git blame` / `git log`, what will I see?"
- **Why:** Your local repo has **zero commits**. Interviewers who open GitHub notice a single "initial commit" dump.
- **Follow-up ladder:** "Why one giant commit?" → "How do you normally commit?" → "What's a good commit message?"
- **Strong answer:**
  - [Confirm and fix before the interview.] "The simulation work happened in Forage's environment. I rebuilt it into this repo later, so the history is short. My normal practice is small commits with one logical change each, imperative messages ('Add DLT for failed transactions'), and feature branches with PRs so CI runs."
- **Avoid:** Making up a history you can't show.
- **Likelihood:** Medium

### GC.3 Q: "README says 'completed December 2025'. What exactly did you complete then, and what came later?"
- **Why:** Tests consistency between your resume, README and story.
- **Follow-up ladder:** "What did the Forage version look like?" → "What did you change after, and why?"
- **Strong answer:** [Confirm.] "The Forage tasks (Kafka listener, H2 persistence, incentive call, balance endpoint) were done in Dec 2025. Afterwards I added: the stand-in incentive API, error handling, timeouts, CI, and the README." Keep the list identical to your answer to 1.3.
- **Avoid:** Different dates on your resume and in the README.
- **Likelihood:** Medium

### GC.4 Q: "Your README badge says '16 tests passing'. Is that live?"
- **Why:** README:7 is a **static** shields.io badge. A sharp interviewer will notice it can never go red.
- **Follow-up ladder:** "So it could be lying?" → "How do you make it honest?"
- **Strong answer:**
  - "It's static text, so it's true as of when I wrote it, but it won't update. The honest version is the GitHub Actions workflow status badge (`…/actions/workflows/ci.yml/badge.svg`), which reflects the latest run on main. For a test count or coverage, I'd publish JaCoCo to Codecov and use their badge."
- **Avoid:** Claiming it's automatic.
- **Likelihood:** Medium

### GC.5 Q: "Your README says 'no rounding errors'. Prove it, or retract it."
- **Why:** README:131 is an absolute claim. The bank lists it as an overclaim (W9), but never as a direct challenge.
- **Follow-up ladder:** "What about 1/3?" → "What about `divide` without a rounding mode?"
- **Strong answer:** "I'd soften it to 'no binary floating-point errors'. BigDecimal is exact for add, subtract and multiply, but division can have a non-terminating result. `BigDecimal.ONE.divide(new BigDecimal(3))` throws `ArithmeticException`, so you must pick a scale and a rounding mode. Any percentage calculation, like the 2% incentive, involves a rounding *decision* (HALF_EVEN in IAC:33). The claim should be 'deterministic, explicit rounding', not 'no rounding'."
- **Avoid:** Defending the absolute.
- **Likelihood:** Medium

### GC.6 Q: "Your mermaid diagram — what's missing from it?"
- **Why:** Tests whether you can critique your own docs.
- **Follow-up ladder:** "Where would the DLT go?" → "Where's the H2 console?"
- **Strong answer:** "It shows the happy path only. Missing: the error handler and retry path, where skipped messages go (nowhere today), the H2 console as a second entry point, the fallback branch when the incentive API fails, and that the producer is outside my system (who authenticates it?). An honest diagram shows failure paths too."
- **Avoid:** "It's complete."
- **Likelihood:** Low

### GC.7 Q: "The README's 'Running it locally' says `mvn verify` first, then Kafka. What if I start midas-core *before* Kafka is up?"
- **Why:** An ordering and robustness what-if taken straight from README:73-89.
- **Follow-up ladder:** "Does `/balance` still work?" → "Does the topic get created?" → "How would you add a readiness check?"
- **Strong answer:**
  - "The app still starts. `KafkaAdmin` fails to reach the broker and logs an error (by default it isn't fatal) [Confirm in Boot 3.2], so the `NewTopic` isn't created at startup. The listener container keeps retrying the connection in the background. `/balance` works. When Kafka comes up, the consumer connects. Whether the topic then exists depends on broker auto-creation (`auto.create.topics.enable`, true by default in Apache Kafka) [Confirm]."
  - "Better: an Actuator readiness indicator that checks Kafka, and `depends_on` with `condition: service_healthy` in compose."
- **Avoid:** "It crashes." Check first.
- **Likelihood:** Low-Medium

### GC.8 Q: "Your README shows `docker compose down -v`. Which volumes does `-v` remove? You didn't define any."
- **Why:** A line-level config question on README:107 vs docker-compose.yml.
- **Follow-up ladder:** "So is Kafka data persisted at all?" → "How do you persist it?"
- **Strong answer:** "There are no named volumes. `-v` would only remove anonymous volumes the image declares [Confirm whether `apache/kafka` declares any]. Either way, `docker compose down` removes the container, and the Kafka log dirs live in its filesystem, so topic data is gone after `down` even without `-v`. To persist across restarts, add a named volume mounted at the log dir (`KAFKA_LOG_DIRS`). For this demo, losing data is actually fine, since H2 is in-memory too (it keeps both sides consistent, 9.13)."
- **Avoid:** Pretending `-v` is needed.
- **Likelihood:** Low

---

## B. Build, Maven & versions

### GC.9 Q: "Your groupId is `com.jpmc`. You don't work at JPMorgan. Why?"
- **Why:** Brand and namespace hygiene. Maven groupIds should be a domain you control.
- **Follow-up ladder:** "What's the convention?" → "Would you rename it?"
- **Strong answer:** "It came from the Forage simulation's package naming [Confirm]. The convention is reverse-DNS of a domain you own, e.g. `io.github.<username>`. Publishing under `com.jpmc` would be misleading, so for a portfolio repo I'd rename the groupId and packages and keep a README note that it's based on the JPMC Forage simulation."
- **Avoid:** Implying any affiliation with JPMC.
- **Likelihood:** Medium

### GC.10 Q: "Why version `1.0.0` and not `1.0.0-SNAPSHOT`?"
- **Why:** Maven semantics check (pom.xml:16).
- **Follow-up ladder:** "What happens if you `deploy` 1.0.0 twice?"
- **Strong answer:** "SNAPSHOT means 'in development, mutable'. Repositories accept re-deploys and consumers re-resolve the latest build. A release version like `1.0.0` should be immutable, and most repositories reject a second deploy. Since this is still evolving, `1.0.0-SNAPSHOT` is more correct. Releases would be cut with the release plugin or by tagging in CI."
- **Avoid:** "No difference."
- **Likelihood:** Low

### GC.11 Q: "Where's the version for `awaitility`, `h2` or `spring-kafka`? You didn't specify any."
- **Why:** Checks that you understand BOM / dependency management (midas-core/pom.xml).
- **Follow-up ladder:** "How would you override one?" → "What's the risk of overriding?"
- **Strong answer:** "The Spring Boot parent imports `spring-boot-dependencies`, a BOM that pins compatible versions for hundreds of libraries, including H2, Awaitility, spring-kafka and kafka-clients. To override: set the property (e.g., `<h2.version>`) or add `<dependencyManagement>`. Risk: breaking compatibility that Boot tested. Do it only for CVE fixes."
- **Avoid:** "Maven picks the latest automatically." It doesn't.
- **Likelihood:** Medium

### GC.12 Q: "Why is H2 `runtime` scope and not `compile`?"
- **Why:** Line-level detail (midas-core/pom.xml:37).
- **Follow-up ladder:** "What scopes exist?" → "Which scope for Lombok?"
- **Strong answer:** "My code never imports H2 classes. It talks JDBC/JPA, and the driver is loaded at runtime. `runtime` keeps it off the compile classpath, so nobody accidentally codes against H2-specific APIs, and swapping to Postgres is just a dependency change. Scopes: compile (default), provided (supplied by the container, e.g. Lombok or the servlet API), runtime, test, plus system and import (for BOMs)."
- **Avoid:** Not knowing scopes.
- **Likelihood:** Low-Medium

### GC.13 Q: "What's inside `midas-core-1.0.0.jar`? How does `java -jar` start Spring?"
- **Why:** README:84 runs the fat jar. This tests the "magic" behind it.
- **Follow-up ladder:** "Why can't a normal jar contain jars?" → "What are layered jars?"
- **Strong answer:** "`spring-boot-maven-plugin` repackages it. `BOOT-INF/classes` holds my code, `BOOT-INF/lib` holds all dependency jars, and the Spring Boot loader classes sit at the root. The manifest's `Main-Class` is the Boot `JarLauncher` (in Boot 3.2 it's `org.springframework.boot.loader.launch.JarLauncher`) and `Start-Class` is `MidasCoreApplication`. The standard JVM classloader can't read nested jars, so Boot's launcher provides a classloader that can. Layered jars split dependencies from app code so Docker layer caching works (only the app layer changes per build)."
- **Avoid:** "It's just a jar."
- **Likelihood:** Low-Medium

### GC.14 Q: "Spring Boot 3.2 — is it still supported?"
- **Why:** Version awareness. Boot 3.2's open-source support has already ended [Confirm exact date: ~Nov 2024].
- **Follow-up ladder:** "How do you track that?" → "Why does it matter?"
- **Strong answer:** "Boot minor versions get about a year of open-source support. 3.2 is past it, so it gets no more free CVE patches. I'd upgrade to the current 3.x line (check spring.io's support timeline), run the properties migrator, and fix deprecations, e.g. the `RestTemplateBuilder.setConnectTimeout` → `connectTimeout` rename in 3.4 [Confirm]. In a company, Dependabot or Renovate opens these PRs automatically."
- **Avoid:** "Latest is always risky, I prefer old."
- **Likelihood:** Low-Medium

### GC.15 Q: "Java 17 — why not 21? What would 21 give this project?"
- **Why:** The README badge says Java 17. The bank (12.11) mentions 21 in passing; this asks about it directly.
- **Follow-up ladder:** "What are virtual threads?" → GC.110 "pinning?"
- **Strong answer:** "17 was the baseline for Boot 3 and an LTS release. 21 (also LTS) gives virtual threads (cheap blocking I/O, useful for the RestTemplate call), record patterns, pattern-matching `switch` (a clean `ValidationResult`), sequenced collections, and generational ZGC. Boot 3.2+ supports `spring.threads.virtual.enabled=true`. I'd move to 21 or later."
- **Avoid:** "No difference."
- **Likelihood:** Medium

### GC.16 Q: "There's no Maven wrapper (`mvnw`). Why does that matter?"
- **Why:** Reproducibility. README:71 says "Maven 3.9+".
- **Follow-up ladder:** "How does CI get Maven?"
- **Strong answer:** "Without the wrapper, every developer's Maven version can differ. The wrapper pins one version and downloads it on first run (`./mvnw verify`). CI currently uses the runner's preinstalled Maven. I'd add it with `mvn wrapper:wrapper` and use `./mvnw` in CI and Dockerfiles."
- **Avoid:** Not knowing what mvnw is.
- **Likelihood:** Low

### GC.17 Q: "`KafkaIntegrationTest` runs in Surefire's `test` phase. Should it?"
- **Why:** Tests Maven test-phase knowledge (Surefire vs Failsafe).
- **Follow-up ladder:** "What's Failsafe?" → "Why separate them?"
- **Strong answer:** "Surefire runs `*Test` classes in `test`. Failsafe runs `*IT` classes in `integration-test`/`verify`, and guarantees `post-integration-test` cleanup even when tests fail. Splitting them gives a fast unit feedback loop (`mvn test`) and slower integration tests in `verify`. My E2E test is quick because the broker is embedded, so it's OK in Surefire. With Testcontainers I'd rename it to `*IT` and add Failsafe."
- **Avoid:** "All tests are the same."
- **Likelihood:** Low

---

## C. docker-compose & scripts

### GC.18 Q: "Your compose file has no healthcheck. What does that cost you?"
- **Why:** docker-compose.yml line-level review.
- **Follow-up ladder:** "What would the healthcheck command be?" → "`depends_on` conditions?"
- **Strong answer:** "Compose only knows the container *started*, not that Kafka is *ready*. If I add the apps to compose, they'd race the broker (GC.7). Add:
  `healthcheck: test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092"]`, with interval and retries. Then use `depends_on: kafka: condition: service_healthy` on the app services."
- **Avoid:** "depends_on waits for readiness." Plain `depends_on` only waits for start.
- **Likelihood:** Low-Medium

### GC.19 Q: "`container_name: midas-kafka` — what problem can that cause?"
- **Why:** Small config detail.
- **Follow-up ladder:** "Why does your script need it?"
- **Strong answer:** "Container names are global on the Docker host. A second copy of the project, or a parallel CI job, would conflict. I set it because `send-transactions.sh` does `docker exec midas-kafka`. A more robust option is `docker compose exec kafka …`, which resolves by service name within the project."
- **Avoid:** Not knowing why it's set.
- **Likelihood:** Low

### GC.20 Q: "Where's `CLUSTER_ID` in your KRaft config? Doesn't KRaft need a formatted storage dir?"
- **Why:** KRaft depth. Storage must be formatted with a cluster id.
- **Follow-up ladder:** "What happens on a multi-node cluster?"
- **Strong answer:** "KRaft needs `kafka-storage.sh format --cluster-id …` before first start. The official `apache/kafka` image's entrypoint formats storage automatically and uses a default cluster id if `CLUSTER_ID` isn't set [Confirm]. That's fine for a single dev node. In a multi-node cluster, every node must be formatted with the *same* id, generated once with `kafka-storage.sh random-uuid`."
- **Avoid:** Bluffing about KRaft internals. Say [Confirm] if unsure.
- **Likelihood:** Low

### GC.21 Q: "Your image tag is `apache/kafka:3.7.0`. Is that fully pinned?"
- **Why:** Supply-chain awareness.
- **Follow-up ladder:** "Same question for `actions/checkout@v4`?" (GC.26)
- **Strong answer:** "A tag is mutable: someone can re-push `3.7.0`. Fully pinned means a digest, `apache/kafka:3.7.0@sha256:…`. It's better than `latest`, but production manifests should use digests, plus image scanning (Trivy) in CI."
- **Avoid:** "Tags never change."
- **Likelihood:** Low

### GC.22 Q: "What if the JSONL file has a blank line or a trailing empty line? What gets published?"
- **Why:** An unusual what-if on scripts/send-transactions.sh and the jsonl file.
- **Follow-up ladder:** "How does the consumer react?" → "Does `grep -c .` match what was sent?"
- **Strong answer:** "The console producer sends each line as a record, so a blank line may become an **empty-value** record [Confirm]. On the consumer, `JsonDeserializer` fails on empty input (or returns null) [Confirm]. Either way, `ErrorHandlingDeserializer` + the error handler skip it, or `validate` rejects a null. Meanwhile `grep -c .` counts only non-empty lines, so the script's 'Sent N messages' count would be lower than what Kafka actually received."
- **Avoid:** Assuming blank lines are ignored.
- **Likelihood:** Low

### GC.23 Q: "`check-balances.sh`: what happens if midas-core isn't running?"
- **Why:** A line-level shell detail (`set -euo pipefail` + `curl -s`).
- **Follow-up ladder:** "How would you make it fail clearly?"
- **Strong answer:** "`printf 'user 1: '` prints, then `curl -s` fails with exit code 7 (connection refused). `-s` hides the error message, and `set -e` aborts the script, so you see 'user 1: ' and nothing else, with no explanation. Better: `curl -fsS` (`-f` fails on HTTP errors, `-S` shows errors even in silent mode), a trap or explicit message, and making the host and port configurable instead of hardcoding `localhost:33400`."
- **Avoid:** "It prints 0."
- **Likelihood:** Low

---

## D. CI

### GC.24 Q: "Your CI runs on `pull_request` from forks. Any security concern?"
- **Why:** GitHub Actions security basics.
- **Follow-up ladder:** "What is `pull_request_target`?" → "`permissions:`?"
- **Strong answer:** "With `pull_request`, fork PRs run with a read-only token and no secrets, so running their `mvn verify` is relatively safe. There are no secrets in my workflow anyway. `pull_request_target` runs with base-repo privileges and is dangerous if it checks out PR code. I'd add `permissions: contents: read` explicitly (least privilege)."
- **Avoid:** Not knowing the difference.
- **Likelihood:** Low

### GC.25 Q: "CI is green but a test is flaky — say `KafkaIntegrationTest` fails 1 in 20 runs. What do you do?"
- **Why:** Realistic engineering judgment for async tests.
- **Follow-up ladder:** "How do you find the cause?" → "Would you add retries?"
- **Strong answer:** "Don't just re-run. Reproduce by looping it locally (`mvn -Dtest=KafkaIntegrationTest -Dsurefire.rerunFailingTestsCount=0` in a loop). Look at the timing: slow consumer startup or a partition-assignment delay versus Awaitility's 20s, and an embedded broker port clash. Fixes: wait for partition assignment before sending (`ContainerTestUtils.waitForAssignment`), isolate the topic and group per test, and avoid shared state. Quarantine it with a ticket only as a last resort; blind retries hide real race bugs."
- **Avoid:** "Just add @RepeatedTest / retry."
- **Likelihood:** Medium

### GC.26 Q: "`actions/checkout@v4` and `setup-java@v4` — pinned by tag. Should they be pinned by SHA?"
- **Why:** Supply-chain detail (ci.yml:12-13).
- **Follow-up ladder:** "What's the trade-off?"
- **Strong answer:** "Tags can be moved. A compromised action could change what `v4` points to (this has happened with third-party actions). Pinning a full commit SHA is immutable, and Dependabot can still bump it. For first-party `actions/*` many teams accept tags; for third-party actions, pin the SHA."
- **Avoid:** "Doesn't matter."
- **Likelihood:** Low

### GC.27 Q: "CI uses `ubuntu-latest` and Temurin `'17'`. Is the build reproducible?"
- **Why:** Reproducibility follow-up.
- **Follow-up ladder:** "What could change between two runs of the same commit?"
- **Strong answer:** "Not perfectly. `ubuntu-latest` moves to new Ubuntu versions, `'17'` resolves to the newest 17.0.x patch, and Maven on the runner can change. Dependencies are pinned by the Boot BOM. For stronger reproducibility: pin the OS image (`ubuntu-24.04`), the JDK patch, use the Maven wrapper, and build inside a pinned container image."
- **Avoid:** "Yes, fully reproducible."
- **Likelihood:** Low

---

## E. application.yml line-by-line

### GC.28 Q: "What if `general.kafka-topic` is missing from config?"
- **Why:** What-if on the property used in TL:22, KC:16 and the tests.
- **Follow-up ladder:** "How do you fail fast with a nice message?" → "`@ConfigurationProperties` + `@Validated`?"
- **Strong answer:** "Startup fails: `Could not resolve placeholder 'general.kafka-topic'`, both when resolving the `@KafkaListener` topics expression and for the `@Value` in KC. That's actually good, because it fails fast. A cleaner way is `@ConfigurationProperties(prefix = "midas")` with a record plus `@Validated @NotBlank`, which gives one typed config object, IDE autocomplete (with the config processor) and clear errors. The `general.` prefix is a non-standard namespace from the simulation [Confirm]; I'd rename it to `midas.kafka.topic`."
- **Avoid:** "It uses a default."
- **Likelihood:** Low-Medium

### GC.29 Q: "Default `max.poll.records` is 500 and `max.poll.interval.ms` is 5 minutes. With your 5s read timeout, what happens when the incentive API is slow?"
- **Why:** A sharp follow-up to 9.2. It's arithmetic on defaults you didn't override in core.yml.
- **Follow-up ladder:** "What's the difference from `session.timeout.ms`?" (GC.30) → "How do you fix it?"
- **Strong answer:**
  - "One poll can return up to 500 records, processed serially. In the worst case each takes about 7s (2s connect + 5s read), so 500 × 7s = 3,500s, far beyond the 300s `max.poll.interval.ms`. The consumer is considered dead, the group **rebalances**, and the partition is reassigned. When my thread finally tries to commit, it fails (`CommitFailedException`), the batch is re-read, and transfers already committed to the DB get **applied again**. That's a rebalance-and-duplicates storm."
  - "Fixes: lower `max.poll.records` (e.g., 10–50), tighter timeouts, a circuit breaker so slow calls fail fast, and making incentives async. Idempotency (8.6) makes the duplicates harmless."
- **Avoid:** "Heartbeats keep it alive." The background heartbeat doesn't cover `max.poll.interval.ms`.
- **Likelihood:** Medium (a very strong signal if you volunteer it)

### GC.30 Q: "`session.timeout.ms` vs `max.poll.interval.ms` vs `heartbeat.interval.ms`?"
- **Why:** Kafka consumer liveness, a follow-up to GC.29.
- **Follow-up ladder:** "Which one detects a crashed process vs a stuck processor?"
- **Strong answer:** "A background heartbeat thread sends heartbeats every `heartbeat.interval.ms` (3s default). If the coordinator gets none within `session.timeout.ms` (45s default in Kafka 3.x), the consumer is dead (process crashed or network lost). Separately, if the app doesn't call `poll()` within `max.poll.interval.ms` (5 min), the client leaves the group because it's 'alive but stuck'. The first detects a crashed or disconnected consumer; the second detects a stuck or slow processing loop."
- **Avoid:** Mixing them up.
- **Likelihood:** Medium

### GC.31 Q: "Why is `spring.deserializer.value.delegate.class` under `consumer.properties` and not a normal Spring key?"
- **Why:** Line-level (core.yml:26-27).
- **Follow-up ladder:** "How else could you configure it?"
- **Strong answer:** "Boot exposes the common consumer settings as typed keys (`group-id`, `auto-offset-reset`, deserializers). Anything else is passed straight into the Kafka client's config map via `properties.*`. `ErrorHandlingDeserializer` reads its delegate class from that map at `configure()` time. Alternative: define a `ConsumerFactory` bean in Java and construct `new ErrorHandlingDeserializer<>(new JsonDeserializer<>(Transaction.class))`, which is type-safe and refactor-friendly."
- **Avoid:** "It's a typo / Spring property."
- **Likelihood:** Low

### GC.32 Q: "Your consumer uses `StringDeserializer` for keys. What *are* the keys today?"
- **Why:** Config vs reality (core.yml:24).
- **Follow-up ladder:** "What would you key by?" (bank 8.4) → "What's the partitioner algorithm?"
- **Strong answer:** "Null. Neither the console producer in the script nor the test's `KafkaTemplate.send(topic, value)` sets a key. The deserializer is ready for string keys. With a key, the default partitioner does `murmur2(keyBytes) % numPartitions`. Without one, the sticky partitioner fills a batch for one partition, then switches."
- **Avoid:** Claiming you key by user.
- **Likelihood:** Low-Medium

### GC.33 Q: "H2 console: README says JDBC URL `jdbc:h2:mem:midas`, but your app uses `jdbc:h2:mem:midas;DB_CLOSE_DELAY=-1`. Does the console see the same DB?"
- **Why:** A sharp line-level check (README:106 vs core.yml:37).
- **Follow-up ladder:** "What if you typed `jdbc:h2:mem:midas2`?" → "Could another JVM connect?"
- **Strong answer:** "Yes. In-memory databases are identified by name *within one JVM*, and the console runs inside the app's JVM. `DB_CLOSE_DELAY` is a lifetime setting, not part of the identity. A different name silently creates a new, empty DB, which is a classic confusing moment. Another JVM can't connect to `mem:` at all; you'd need H2 server mode or a file DB."
- **Avoid:** "Different URL, so a different DB."
- **Likelihood:** Low

### GC.34 Q: "What exactly does `ddl-auto: create-drop` generate for `Instant processedAt` and `String name`?"
- **Why:** Type-mapping detail (TR:40, UR:21).
- **Follow-up ladder:** "Precision loss?" → "Why not `LocalDateTime`?"
- **Strong answer:** "`String` becomes `VARCHAR(255)` by default (set `length` if needed). `Instant` maps to a timestamp type: in Hibernate 6.x, a UTC timestamp, likely `TIMESTAMP(6) WITH TIME ZONE` on H2 [Confirm with show-sql]. Precision 6 means microseconds, while `Instant.now()` can have nanoseconds on Linux, so the value read back can differ in the last digits from the one in memory. That matters if you compare them in tests. `LocalDateTime` has no zone, so it's ambiguous across servers. Prefer `Instant` or `OffsetDateTime`."
- **Avoid:** Guessing exact DDL without [Confirm].
- **Likelihood:** Low

### GC.35 Q: "`password:` is empty in YAML. Is that `null` or `""`? Does it matter?"
- **Why:** A YAML subtlety on core.yml:39.
- **Follow-up ladder:** "How do you set a real password safely?"
- **Strong answer:** "In YAML, `password:` with nothing after it is null. Spring binds it to an empty or absent password, which H2's `sa` user accepts. In real setups: `password: ${DB_PASSWORD}` with no default, so startup fails if it's missing rather than silently connecting with an empty password."
- **Avoid:** Overthinking it. Just know the safe pattern.
- **Likelihood:** Low

---

## F. Startup, listener, Kafka config, seeding (MidasCoreApplication, TL, KC, DDL)

### GC.36 Q: "What does `SpringApplication.run(MidasCoreApplication.class, args)` actually do, step by step?"
- **Why:** The entry-point file has no question yet.
- **Follow-up ladder:** "How does auto-configuration know what to configure?" → "How do you see which auto-configs applied?"
- **Strong answer:** "It deduces the app type (servlet, because Spring MVC is on the classpath) and loads initializers and listeners. It prepares the `Environment` (args, env vars, application.yml). It creates an `AnnotationConfigServletWebServerApplicationContext`. Then **refresh**: component scan from `com.jpmc.midascore`, auto-configuration classes listed in `META-INF/spring/…AutoConfiguration.imports` evaluated by their `@Conditional`s (DataSource, JPA, Kafka, Jackson, MVC), singletons created, `SmartInitializingSingleton` (my seeding), then lifecycle start (Tomcat and Kafka containers). Finally runners, then `ApplicationReadyEvent`. To see which auto-configs matched and why: run with `--debug` for the condition evaluation report."
- **Avoid:** "It starts Tomcat." That's only one step.
- **Likelihood:** Medium

### GC.37 Q: "During `FixedBackOff`'s 500ms waits, what is the consumer doing? Do other messages get processed?"
- **Why:** Blocking vs non-blocking retries, a follow-up to 7.13 and 16.12.
- **Follow-up ladder:** "What if the backoff were 5 minutes?" → "What's `@RetryableTopic`?"
- **Strong answer:**
  - "These are **blocking retries**. The failed record is re-sought, and nothing after it in that partition is processed until the retries finish. That's how ordering is preserved. For 500ms × 2 it's harmless."
  - "With long backoffs, you'd block the partition for that long and risk `max.poll.interval.ms`. Spring Kafka handles long backoffs by pausing the consumer while still polling [Confirm the version-specific behavior]."
  - "**Non-blocking retries:** `@RetryableTopic` sends the failed record to `topic-retry-1`, `-retry-2`… with delays, then `-dlt`, so the main partition keeps flowing. The trade-off is that **ordering is lost** for that record. For account balances that may be unacceptable (a later debit could run before an earlier credit), so blocking retries plus a DLT is often right for money."
- **Avoid:** "Retries happen in the background."
- **Likelihood:** Medium-High

### GC.38 Q: "After 2 retries, where is the skipped record logged, and what does the log contain?"
- **Why:** README:27 says "logged and skipped". Prove you know where and what.
- **Follow-up ladder:** "Could you replay it from the log?" → "How would you alert on it?"
- **Strong answer:** "The `DefaultErrorHandler`'s default recoverer logs at ERROR with the record (topic-partition@offset) and the exception, through Spring Kafka's logger, not mine [Confirm the exact format]. You can't replay from a log line reliably: the payload may be truncated or absent, and logs rotate. That's why a DLT is better. It keeps the original bytes plus exception headers, and you alert on DLT growth."
- **Avoid:** "TransactionService logs it."
- **Likelihood:** Medium

### GC.39 Q: "`@KafkaListener(topics=…, groupId=…)` — the group id is in the yml too. Why both? Which wins?"
- **Why:** Line-level redundancy (TL:22 vs core.yml:22).
- **Follow-up ladder:** "What does the `id` attribute do?" → "`idIsGroup`?"
- **Strong answer:** "The annotation's `groupId` overrides the consumer factory's `group.id`. Here they're the same value (`${spring.kafka.consumer.group-id}`), so it's redundant but explicit. Gotcha: if you set `id = "txListener"` without `groupId`, Spring uses the **id as the group id** by default (`idIsGroup = true`), which could silently change your group and reset offsets. Setting `id` is useful for controlling the listener at runtime (GC.40)."
- **Avoid:** "The yml wins."
- **Likelihood:** Low-Medium

### GC.40 Q: "How would you pause consumption at runtime, say during a DB migration, without stopping the app?"
- **Why:** An operational what-if.
- **Follow-up ladder:** "What happens to lag?" → "Does pausing trigger a rebalance?"
- **Strong answer:** "Give the listener an `id`, then use `KafkaListenerEndpointRegistry.getListenerContainer("txListener").pause()` / `.resume()`, exposed via an admin endpoint or Actuator. Pause keeps the consumer in the group (it keeps polling but gets no records), so there's **no rebalance**. `stop()` leaves the group and does trigger one. Lag grows while paused and drains after resume."
- **Avoid:** "Kill the app."
- **Likelihood:** Low-Medium

### GC.41 Q: "Your listener takes a `Transaction` parameter. How does Spring know to give you the payload and not the `ConsumerRecord`?"
- **Why:** Framework internals of TL:23.
- **Follow-up ladder:** "How would you get the partition and offset for logging?"
- **Strong answer:** "Spring Kafka wraps the method in a `MessagingMessageListenerAdapter`, which converts the `ConsumerRecord` into a Spring `Message` and uses argument resolvers. An un-annotated parameter is the payload; you can also declare `ConsumerRecord<K,V>`, `Acknowledgment`, `@Header(KafkaHeaders.RECEIVED_PARTITION) int p`, `@Header(KafkaHeaders.OFFSET) long o`. I'd add partition and offset to my logs (MDC) so a log line can be tied to an exact record, which my current `log.debug("Received {}")` can't do."
- **Avoid:** "Kafka gives you the object directly."
- **Likelihood:** Low-Medium

### GC.42 Q: "What retention does your topic have? What happens to messages after it?"
- **Why:** KC:17 sets only partitions and replicas, not topic configs.
- **Follow-up ladder:** "What if lag exceeds retention?" → "What's log compaction?" (GC.95)
- **Strong answer:** "The broker default: `log.retention.hours=168` (7 days), deleted by whole segment. If my consumer is down or behind longer than that, unread messages are **deleted**, and on resume the committed offset is out of range, so `auto-offset-reset: earliest` kicks in and I silently skip the lost data. For a financial stream, set retention explicitly (`TopicBuilder.config(TopicConfig.RETENTION_MS_CONFIG, …)`) and alert on lag well before it gets close."
- **Avoid:** "Kafka keeps messages forever."
- **Likelihood:** Medium

### GC.43 Q: "Another team (fraud detection) wants the same transaction events. What changes in your system?"
- **Why:** A Kafka fan-out what-if.
- **Follow-up ladder:** "Do they slow you down?" → "Can they replay from the start?"
- **Strong answer:** "Nothing on my side. They use their own consumer group, and each group has independent offsets, so both get every message. They don't affect my lag. They can start from `earliest` within retention. The real issues are contract and schema governance (17.9) and ACLs (6.6) for their principal. That decoupling is a key advantage over a direct REST call."
- **Avoid:** "They'd steal my messages." That only happens within the same group.
- **Likelihood:** Medium

### GC.44 Q: "Someone deletes and recreates the topic while midas-core is running. What happens?"
- **Why:** An unusual operational scenario.
- **Follow-up ladder:** "What about the committed offset?"
- **Strong answer:** "The consumer gets errors (unknown topic or partition), then sees the new topic in metadata. The committed offset (say 500) is now beyond the new log's end, so `OffsetOutOfRange` triggers the reset policy `earliest` and it starts from 0 of the new topic. No old messages are replayed, because they're gone. In production, topic deletion is disabled or locked down with ACLs, and topics are managed as code."
- **Avoid:** Guessing confidently. Reason it out.
- **Likelihood:** Low

### GC.45 Q: "Is `DemoDataLoader`'s seeding atomic? What if insert #3 fails?"
- **Why:** A follow-up to 16.27 and 16.28.
- **Follow-up ladder:** "Where's the transaction? You didn't annotate anything."
- **Strong answer:** "`SimpleJpaRepository.saveAll` is itself `@Transactional`, so all 5 inserts are one transaction: either all are seeded or none. But `count()` runs in a *separate* read-only transaction, so check-then-insert isn't atomic. Two instances starting together (with a shared DB) could both see 0, and then one fails on the unique `name` constraint, crashing startup. Fix: Flyway seed migrations, or `INSERT … ON CONFLICT DO NOTHING`."
- **Avoid:** "There's no transaction."
- **Likelihood:** Low-Medium

### GC.46 Q: "What if `midas.demo-data.enabled` is set to `TRUE` or `yes`?"
- **Why:** `@ConditionalOnProperty(havingValue = "true")` semantics, a line-level detail.
- **Follow-up ladder:** "And if the property is missing entirely?"
- **Strong answer:** "`havingValue` is compared case-insensitively [Confirm], so `TRUE` works but `yes` doesn't. If the property is missing, the bean is **not** created, because `matchIfMissing` defaults to false. My yml sets `true`, so it's on by default, and the tests turn it off."
- **Avoid:** Assuming any truthy string works.
- **Likelihood:** Low

---

## G. TransactionService, line by line

### GC.47 Q: "Why is the validation order nulls → amount → self-transfer → existence?"
- **Why:** TS:77-93 design reasoning.
- **Follow-up ladder:** "Would you reorder for performance?"
- **Strong answer:** "Cheapest checks first. Null and amount checks are pure in-memory work, and the self-transfer check needs no DB. Only then two DB lookups. Bad input is rejected without touching the DB, and nulls are checked before anything dereferences them (avoiding an NPE on `tx.amount().signum()`). The funds check comes after loading (it needs the balance), and the expensive HTTP call comes last (16.5)."
- **Avoid:** "No particular reason."
- **Likelihood:** Medium

### GC.48 Q: "`orElseThrow()` with no argument — what does the exception look like in your logs?"
- **Why:** Line-level, TS:57-58.
- **Follow-up ladder:** "How would you improve it?"
- **Strong answer:** "`NoSuchElementException: No value present`, with no user id and no context. It's nearly useless at 3 am. Better: `orElseThrow(() -> new UserNotFoundException(id))`, or remove the redundancy (16.4) and reject with a reason."
- **Avoid:** Defending the bare call.
- **Likelihood:** Low-Medium

### GC.49 Q: "`process` returns a `TransactionRecord` entity to callers outside the transaction. Can a caller safely call `record.getSender().getName()`?"
- **Why:** A sharp JPA subtlety on TS:71-74 and TR:25 (LAZY).
- **Follow-up ladder:** "What if it had been loaded with `findById` on the record instead?" → "Why do DTOs avoid this?"
- **Strong answer:** "Here, yes. `sender` was set to the **real `UserRecord` instance** loaded by `findById` in the same persistence context, not a lazy proxy, so it works even after the transaction ends. The entity is detached but fully initialized. If a caller later loaded a `TransactionRecord` fresh (`transactionRepository.findById`), `sender` would be an **uninitialized proxy**. Touching it outside a transaction (with `open-in-view: false`) throws `LazyInitializationException`. Returning entities across layers is fragile; I'd return a DTO."
- **Avoid:** A confident "always fails" or "always works". It depends on how it was loaded.
- **Likelihood:** Medium

### GC.50 Q: "What if `process` were `final`, `private` or `static`? Would `@Transactional` still work?"
- **Why:** Proxy limits, a follow-up to 7.2.
- **Follow-up ladder:** "What would you see?"
- **Strong answer:** "No. A CGLIB proxy works by subclassing and overriding. A `final` method can't be overridden, so the call runs on the proxy instance itself, whose injected fields are null, giving an **NPE** with no transaction. Private and static methods aren't intercepted either: the annotation is silently ignored. Spring 6 also intercepts protected and package-private methods with CGLIB proxies, but public is the safe convention."
- **Avoid:** "Spring handles it."
- **Likelihood:** Medium

### GC.51 Q: "There's no `try/catch` anywhere in `process`. Intentional?"
- **Why:** An error-handling philosophy check.
- **Follow-up ladder:** "What would a catch-all here break?"
- **Strong answer:** "Yes. Expected outcomes (rejections) are return values. Unexpected failures (DB down, a bug) should propagate, so the transaction rolls back and the Kafka error handler can retry or DLT the message. A catch-all that logs and returns would **commit** a partial state, or at least swallow the failure, and Kafka would ack a message that never applied. The only catch is in `IncentiveClient`, where a failure has a defined business fallback."
- **Avoid:** "I forgot."
- **Likelihood:** Medium

### GC.52 Q: "What if `IncentiveClient` throws something that's *not* a `RestClientException`, like `IllegalArgumentException` for a malformed URL, or a Jackson exception?"
- **Why:** A what-if on IC:43's narrow catch combined with TS.
- **Follow-up ladder:** "Is retrying useful then?" → "How do you classify exceptions?"
- **Strong answer:** "It propagates out of `process` → rollback → `DefaultErrorHandler` retries twice (each retry fails the same way) → skip. So a **config bug** (a bad `INCENTIVE_API_URL`) silently drops *every* valid transfer after a few seconds each. Fixes: validate the URL at startup (fail fast), mark such exceptions non-retryable and route them to a DLT, and alert. Body parsing errors are wrapped as `RestClientException` (so they fall back to 0), but URI syntax problems may not be [Confirm]."
- **Avoid:** "It's caught."
- **Likelihood:** Medium

### GC.53 Q: "Two different users each send 100 to the same recipient at the same time. Any problem for the recipient row?"
- **Why:** A concurrency variant the bank didn't cover: a credit-credit race.
- **Follow-up ladder:** "Why are credits easier than debits?"
- **Strong answer:** "With the read-modify-write pattern (load, `credit()`, flush), yes: lost update. Both read 500 and both write 600, so one credit vanishes. It's the same bug as 8.1, just on the recipient. With an atomic `UPDATE … SET balance = balance + :x`, no: the DB serializes the two updates on the row lock, and addition commutes. Credits don't need a pre-check, so the atomic form is always safe."
- **Avoid:** "Only debits race."
- **Likelihood:** Medium

### GC.54 Q: "How would you support blocked, frozen or KYC-pending accounts?"
- **Why:** A Blinkit/fintech business extension.
- **Follow-up ladder:** "Where does the check go?" → "Race between freeze and transfer?"
- **Strong answer:** "Add `status` (ACTIVE/FROZEN/CLOSED) to `users`. Validation rejects if either side isn't ACTIVE. To avoid a race where a freeze lands mid-transfer, include the status in the atomic update (`… WHERE id = ? AND status = 'ACTIVE' AND balance >= ?`), or lock the row. Record a rejection reason like `SENDER_FROZEN`. Freezes come from a compliance service as events."
- **Avoid:** Checking status only in Java without considering the race.
- **Likelihood:** Low-Medium

### GC.55 Q: "Name things: `UserRecord` isn't a Java `record`, but `Transaction` is. Isn't that confusing?"
- **Why:** Naming nitpick, likely from a senior reviewer.
- **Follow-up ladder:** "What would you rename them to?"
- **Strong answer:** "Fair. 'Record' here means a database record, which predates Java records, and the names came from the simulation [Confirm]. With Java records in the same codebase, it reads as ambiguous. I'd use `UserEntity` / `TransactionEntity` (or `Account` / `Transfer` for domain names) and keep DTOs as `TransactionMessage` / `BalanceResponse`. The `tx` vs `transaction` variable naming in TS is inconsistent too."
- **Avoid:** Getting defensive over names.
- **Likelihood:** Low-Medium

---

## H. IncentiveClient & incentive-api

### GC.56 Q: "What headers does your `postForObject` send? What Content-Type and Accept?"
- **Why:** HTTP details of IC:38.
- **Follow-up ladder:** "How would you add an auth header or idempotency key?"
- **Strong answer:** "RestTemplate picks the Jackson converter for the `Transaction` record, so `Content-Type: application/json`. `Accept` lists the media types the converters can read for `Incentive` (JSON). To add headers: build an `HttpEntity<>(body, headers)` and use `exchange`/`postForObject` with it, or register a `ClientHttpRequestInterceptor` on the builder for cross-cutting headers (auth token, `X-Correlation-Id`, `Idempotency-Key`)."
- **Avoid:** "No headers."
- **Likelihood:** Low-Medium

### GC.57 Q: "The incentive API returns 200 with an HTML error page (a proxy or gateway page). What happens?"
- **Why:** An unusual integration failure.
- **Follow-up ladder:** "What about `{"amount":"abc"}`?" → "What about 5 decimal places?"
- **Strong answer:** "No converter reads `text/html` into `Incentive`, so RestTemplate throws `UnknownContentTypeException` (a `RestClientException`). It's caught and falls back to 0 with a WARN. `{"amount":"abc"}` gives a JSON parse failure, wrapped in `RestClientException`, also 0. `{"amount": 3.14159}` is accepted, and that scale-5 value is credited and persisted into a scale-2 column (the W9 issue again) [Confirm DB rounding]. I'd normalize with `setScale(2, HALF_EVEN)` in the client."
- **Avoid:** "It crashes."
- **Likelihood:** Low-Medium

### GC.58 Q: "The incentive API *succeeded* on its side — it recorded a payout — but your 5s read timeout fired first. Now what?"
- **Why:** A classic distributed-systems ambiguity: a timeout is not a failure.
- **Follow-up ladder:** "How do you reconcile?" → "What's the general name for this problem?"
- **Strong answer:** "We credit 0, while they believe they paid an incentive. The systems now **disagree**, and nobody knows. A timeout means *unknown outcome*, not failure. Fixes: send an idempotency key (the transaction id) so we can safely retry or query 'what did you decide for tx X?' later. Persist `incentive_status = UNKNOWN`, and have a reconciliation job ask the API or compare against their report. This is the same problem as payment gateways timing out (13.3). The general pattern is an idempotent retry plus a status query."
- **Avoid:** "Timeout means it failed."
- **Likelihood:** Medium

### GC.59 Q: "Why no retry on the incentive call before falling back to 0?"
- **Why:** A design decision follow-up to 9.1.
- **Follow-up ladder:** "When *would* you retry?" → "Retry storms?"
- **Strong answer:** "Retrying inside the DB transaction would hold the connection even longer (W2), and the API's idempotency is unknown (GC.58). A single attempt with a fallback keeps the worst case bounded at about 7s. If the call moved outside the transaction, I'd do 1–2 retries with exponential backoff and **jitter** (randomized delays, so thousands of clients don't retry in sync and hammer a recovering service: a retry storm), only for idempotent or safe errors (connect failures, 503), with a circuit breaker on top."
- **Avoid:** "Retries are always good."
- **Likelihood:** Medium

### GC.60 Q: "Why are the timeouts hard-coded at IC:30-31 instead of configured?"
- **Why:** A config-hygiene nitpick.
- **Follow-up ladder:** "How would you pick the values?"
- **Strong answer:** "No good reason. They should be properties (`incentive.api.connect-timeout: 2s`, `read-timeout`), bound into `Duration` via `@ConfigurationProperties`, so ops can tune them per environment without a rebuild. Values come from the dependency's SLO: a read timeout a bit above its p99 latency, and a connect timeout short (hundreds of ms within a data center)."
- **Avoid:** "They never need changing."
- **Likelihood:** Low-Medium

### GC.61 Q: "IC:44 logs `e.getMessage()` only, no stack trace. Deliberate?"
- **Why:** A logging decision on a specific line.
- **Follow-up ladder:** "When do you want the stack?"
- **Strong answer:** "Yes. It's an expected, handled failure, so a one-line WARN keeps logs readable when the API is down and every message would otherwise dump a stack trace. The downside is losing the cause chain for unusual errors. A compromise: log the exception class plus message, and the full stack at DEBUG; or rate-limit the logs and rely on a metric (`incentive.fallback` count) to alert."
- **Avoid:** "I forgot the exception argument."
- **Likelihood:** Low

### GC.62 Q: "Your stand-in returns 200 with 0 when `amount` is null. Shouldn't that be a 400?"
- **Why:** API design on IAC:30.
- **Follow-up ladder:** "How would you validate in Spring?"
- **Strong answer:** "Yes. Null input is a client error, so 400 is more correct and would surface caller bugs. I kept it lenient so midas never sees an error from the stand-in, but that hides problems. Better: `@Valid @RequestBody` with `@NotNull @Positive` on the request record, so it returns 400 with a ProblemDetail."
- **Avoid:** Defending a 200 for invalid input.
- **Likelihood:** Low-Medium

### GC.63 Q: "Your threshold is `>= 100.00`. Where's the test for exactly 100.00?"
- **Why:** A missing **boundary test**. `IncentiveControllerTest` checks 250.00 and 99.99 only.
- **Follow-up ladder:** "What's boundary value analysis?" → "What about 100.004?"
- **Strong answer:** "Good catch, it's missing. Boundary value analysis says test just below, at, and just above the edge: 99.99 → 0, **100.00 → 2.00**, 100.01 → 2.00. Plus 100.004, which is ≥ 100 → `2.00008` → HALF_EVEN → 2.00. And null and negative. A parameterized test (`@ParameterizedTest @CsvSource`) covers it in a few lines."
- **Avoid:** "99.99 is enough."
- **Likelihood:** Medium

### GC.64 Q: "`150.00 × 0.02` — what scale is the result before `setScale(2)`?"
- **Why:** A BigDecimal arithmetic detail (IAC:33).
- **Follow-up ladder:** "And for `add`?" → "And for `divide`?"
- **Strong answer:** "For `multiply`, scale = sum of the scales = 2 + 2 = 4, so `3.0000`. `setScale(2, HALF_EVEN)` gives `3.00`. For `add`/`subtract`, scale = max of the scales. `divide` needs an explicit scale and rounding mode (or a `MathContext`), or it throws on non-terminating results (GC.5)."
- **Avoid:** "Always 2."
- **Likelihood:** Low-Medium

---

## I. Entities, records, repositories, Balance API

### GC.65 Q: "`UserRecord.debit()` doesn't check anything. Couldn't someone push a balance negative?"
- **Why:** An encapsulation hole in UR:35-41. The bank shows a fix in 14.2 but never asks this.
- **Follow-up ladder:** "And `credit(-50)`?" → "Where should invariants live?"
- **Strong answer:** "Yes. The funds rule lives in the service (`canAfford` then `debit`). Any other caller of `debit` skips it, and `credit(negative)` is effectively a hidden debit. The entity should **protect its own invariants**: `debit` throws if `amount <= 0` or the balance is insufficient, and `credit` throws if `amount <= 0`. The DB adds `CHECK (balance >= 0)`. Then the service can't forget."
- **Avoid:** "The service always checks."
- **Likelihood:** Medium

### GC.66 Q: "Why is the table named `users` and not `user`? And `transactions`, not `transaction`?"
- **Why:** A reserved-words trap.
- **Follow-up ladder:** "How do you escape it if you must?"
- **Strong answer:** "`USER` is a reserved word in Postgres (and SQL generally, e.g. `CURRENT_USER`) and in H2, so `select * from user` fails or returns the wrong thing. `TRANSACTION` is a keyword in several databases. Plural names avoid that. If forced, quote identifiers (`@Table(name = "\"user\"")`) or set `hibernate.globally_quoted_identifiers`, but that's painful."
- **Avoid:** "Just style."
- **Likelihood:** Low-Medium

### GC.67 Q: "`JpaRepository` vs `CrudRepository` vs `ListCrudRepository` — why `JpaRepository`?"
- **Why:** A Spring Data API choice (URepo:8, TRRepo:6).
- **Follow-up ladder:** "Do you use anything JPA-specific?"
- **Strong answer:** "`CrudRepository` is the basic CRUD with `Iterable` returns. `ListCrudRepository` (Spring Data 3) returns `List`s. `JpaRepository` adds paging and sorting, `flush`, `saveAndFlush`, `getReferenceById`, batch deletes and query by example. I only use `findById`, `existsById`, `save`, `saveAll`, `count` and `deleteAll`, so `ListCrudRepository` would suffice. `JpaRepository` is just the common default, and I'll need `saveAndFlush` and `getReferenceById` for the idempotency and atomic-update designs."
- **Avoid:** Not knowing the hierarchy.
- **Likelihood:** Low

### GC.68 Q: "Unknown user returns `{"amount":0}`, but known users return `{"amount":850.00}`. Spot the inconsistency."
- **Why:** BS:26 uses `BigDecimal.ZERO` (scale 0), while the DB returns scale 2.
- **Follow-up ladder:** "Does the client care?" → "How do you make output consistent?"
- **Strong answer:** "The JSON number format differs: `0` vs `850.00`. A strict client or a snapshot test could break, and it's a tell that the unknown-user path is special-cased. Fix: always `setScale(2)` in the DTO, or a Jackson config or serializer for money, or better, return 404 for unknown users (5.3)."
- **Avoid:** "JSON numbers are equal."
- **Likelihood:** Low

### GC.69 Q: "What does `/balance?userId=99999999999999999999` return? `userId=-1`? `userId=1&userId=2`?"
- **Why:** BC:20 edge cases.
- **Follow-up ladder:** "How would you validate path or query params?"
- **Strong answer:** "Beyond `Long.MAX_VALUE`: conversion fails with `MethodArgumentTypeMismatchException`, giving 400. `-1`: valid Long, not found, so 200 with 0 (it should be a 400 or 404). Repeated params: Spring binds a single `Long` from the first value [Confirm]. Fix: `@Validated` on the controller and `@Positive` on the param, giving 400 via a ConstraintViolation handler."
- **Avoid:** "500."
- **Likelihood:** Low-Medium

### GC.70 Q: "The controller returns a `Balance` record, not `ResponseEntity<Balance>`. When would you use `ResponseEntity`?"
- **Why:** An API style question on BC:20.
- **Follow-up ladder:** "How do you return 404 without `ResponseEntity`?"
- **Strong answer:** "Returning the body directly means 200 with that body, which is clean when the status is always 200. Use `ResponseEntity` when the status or headers vary (201 + `Location`, 404, `ETag`/`Cache-Control`). Or keep returning the body and throw exceptions mapped by `@RestControllerAdvice` or `@ResponseStatus`. For a 404, throw `UserNotFoundException` (14.4)."
- **Avoid:** "ResponseEntity is required."
- **Likelihood:** Low-Medium

### GC.71 Q: "Is `BalanceService` worth existing? It's one line. Isn't that over-layering?"
- **Why:** Pushback on 2.2.
- **Follow-up ladder:** "When would you skip the service layer?"
- **Strong answer:** "Today it's thin, but it owns the transaction boundary (`readOnly`) and the 'unknown user → 0' policy, and it's where caching (10.2) or authorization checks (6.1) would go. Controllers shouldn't call repositories directly in a layered design, because the controller would end up holding business rules. For a truly trivial CRUD app, I'd accept collapsing it. Consistency across the codebase matters too."
- **Avoid:** "Layers are always needed."
- **Likelihood:** Low-Medium

### GC.72 Q: "Add a `GET /users/{id}/transactions` endpoint — what's the risk with your current entity design?"
- **Why:** A follow-up to 4.4 and 7.7.
- **Follow-up ladder:** "How do you return sender names efficiently?"
- **Strong answer:** "Returning `TransactionRecord`s would serialize LAZY `sender`/`recipient` proxies after the transaction closes (`open-in-view: false`), giving `LazyInitializationException`, or N+1 if open-in-view were on. Also a JSON cycle risk if a back-reference is added later. Do it with a DTO projection query: `select new com.…TxView(t.id, s.name, r.name, t.amount, t.processedAt) from TransactionRecord t join t.sender s join t.recipient r where …`, with keyset pagination."
- **Avoid:** Returning entities.
- **Likelihood:** Medium

### GC.73 Q: "You use `Optional` as a return type. Would you ever use it for a field or parameter?"
- **Why:** Java idiom; your code uses `Optional` for returns (TS:50, 77).
- **Follow-up ladder:** "`Optional.get()` vs `orElseThrow()`?"
- **Strong answer:** "No. `Optional` is designed for return values that may be absent. It isn't `Serializable`, it adds an allocation, and an `Optional` parameter forces callers to wrap. For fields, use null plus documentation, or a better model. Prefer `orElseThrow()` over `get()`, since it states intent (`get` was effectively renamed in Java 10)."
- **Avoid:** "Optional everywhere."
- **Likelihood:** Low

### GC.74 Q: "No `@Version`, no `createdAt` or `updatedAt` on `users`. How do you know when a balance last changed?"
- **Why:** Auditing gap in UR.
- **Follow-up ladder:** "Spring Data auditing?" → "Hibernate Envers?"
- **Strong answer:** "You can't, except by scanning transactions. Add `@Version` (also for locking) and `@CreatedDate`/`@LastModifiedDate` with `@EnableJpaAuditing`. For full history, Hibernate Envers writes revision tables, or treat the ledger entries as the history (13.9)."
- **Avoid:** "Not needed."
- **Likelihood:** Low

---

## J. Tests, line by line

### GC.75 Q: "In `KafkaIntegrationTest` you `send` three messages without waiting. How do you know they land in order?"
- **Why:** Producer ordering subtlety (KafkaIntegrationTest:46-48).
- **Follow-up ladder:** "When can a producer reorder?" → "What does idempotence change?"
- **Strong answer:** "Same producer, same partition (there's only 1), so records are appended in send order. Reordering can only happen when a batch fails and is retried while a later batch succeeds, which requires `max.in.flight.requests.per.connection > 1` **without** idempotence. With idempotence on (the default in modern clients [Confirm in the resolved kafka-clients version]), the broker uses sequence numbers to keep order even with retries."
- **Avoid:** "Kafka always guarantees global order."
- **Likelihood:** Medium

### GC.76 Q: "Your E2E test sends before the consumer is assigned the partition. Could it miss messages?"
- **Why:** Async test timing.
- **Follow-up ladder:** "How do you wait for assignment?"
- **Strong answer:** "It won't miss them, because `auto-offset-reset: earliest` makes a new group read from the start, so early messages are still consumed once assignment happens. The only effect is extra latency, which Awaitility's 20s covers. The deterministic version is `ContainerTestUtils.waitForAssignment(container, partitions)` via `KafkaListenerEndpointRegistry` before sending. With `latest`, it *could* miss messages, which is a nice demonstration of why that setting matters."
- **Avoid:** "It's guaranteed by luck."
- **Likelihood:** Low-Medium

### GC.77 Q: "`@MockBean` stubs set in `@BeforeEach` — do they leak between tests?"
- **Why:** Mockito + Spring test lifecycle (TransactionServiceTest:47, 63).
- **Follow-up ladder:** "What about the context cache?"
- **Strong answer:** "No. `@MockBean` mocks are reset **after each test** by default (`MockReset.AFTER`), so stubbing like `incentiveIsCreditedToRecipientOnly`'s 5.25 doesn't leak. `@BeforeEach` re-stubs the default `Incentive.none()`. The mock *bean* itself stays in the cached context; only its stubbing and invocations reset."
- **Avoid:** "Each test gets a new context."
- **Likelihood:** Low-Medium

### GC.78 Q: "Why `isEqualByComparingTo("70.00")` and not `isEqualTo(new BigDecimal("70.00"))`?"
- **Why:** AssertJ + BigDecimal (KafkaIntegrationTest:53).
- **Follow-up ladder:** "What would fail?"
- **Strong answer:** "`isEqualTo` uses `equals`, which is scale-sensitive. If the DB returned `70.0` or arithmetic produced `70.0000`, it would fail even though the value is right. `isEqualByComparingTo` uses `compareTo`. It's the test-side mirror of 16.2."
- **Avoid:** Treating them as the same.
- **Likelihood:** Low-Medium

### GC.79 Q: "Your tests live in `com.jpmc.midascore`, not mirrored sub-packages. How does `@SpringBootTest` find your configuration?"
- **Why:** Test configuration discovery.
- **Follow-up ladder:** "What if a test were in `com.other`?"
- **Strong answer:** "`@SpringBootTest` and the slice annotations search **upward** from the test's package for a `@SpringBootConfiguration` (my `@SpringBootApplication` class). Tests in `com.jpmc.midascore` find `MidasCoreApplication` directly. Mirrored sub-packages would also work, by walking up. A test in an unrelated package fails ('Unable to find a @SpringBootConfiguration') unless you specify `classes = …`. Mirroring packages is the convention, and it lets tests access package-private members."
- **Avoid:** "It scans everything."
- **Likelihood:** Low

### GC.80 Q: "How many Spring contexts and embedded brokers does `mvn verify` start? How would you speed the suite up?"
- **Why:** Test-suite performance, a follow-up to 11.2.
- **Follow-up ladder:** "What's the context cache key?"
- **Strong answer:** "Roughly: 2 full `@SpringBootTest` contexts, each with its own embedded broker (`@DirtiesContext` prevents reuse), plus separate slice contexts for `@WebMvcTest` ×2 (two modules), `@DataJpaTest` and `@RestClientTest`. The cache key includes the configuration, properties, `@MockBean` set and active profiles, so different `@MockBean`s mean different contexts. Speed-ups: make `TransactionServiceTest` a Mockito unit test, share one embedded broker (or a Testcontainers singleton), remove `@DirtiesContext` where it isn't needed, and use the same `@MockBean` set across classes."
- **Avoid:** Not knowing context caching exists.
- **Likelihood:** Low

### GC.81 Q: "How would you test that the README's expected-balance table stays true?"
- **Why:** Docs-as-tests, a follow-up to 17.3.
- **Follow-up ladder:** "Where does the incentive come from in that test?"
- **Strong answer:** "Add an E2E test that seeds the same five users, publishes each line of `scripts/sample-transactions.jsonl` via `KafkaTemplate<String,String>`, and uses the *real* incentive rule: start `incentive-api` with a `MockRestServiceServer` that applies the 2% rule, or WireMock. Then assert the five balances from the README. Now the docs break the build if they drift."
- **Avoid:** "I checked manually, that's enough."
- **Likelihood:** Low

---

## K. Unusual "what if" scenarios

### GC.82 Q: "A message has `"amount": 1E+2` (scientific notation). What happens?"
- **Why:** BigDecimal + Jackson edge case.
- **Follow-up ladder:** "What would the JSON response show?"
- **Strong answer:** "Jackson parses it as `BigDecimal` `1E+2` (unscaled 1, scale -2). `signum`, `compareTo` and arithmetic all work, and `compareTo(100.00)` is 0, so it earns the incentive. In the DB it's stored as `100.00` (scale 2). In memory, `toString` prints `1E+2`, so logs look odd. If serialized directly, Jackson writes `1E+2` unless `WRITE_BIGDECIMAL_AS_PLAIN` is on [Confirm]. Normalize on input: `setScale(2, UNNECESSARY)`."
- **Avoid:** "It fails to parse."
- **Likelihood:** Low

### GC.83 Q: "What if `senderId` is sent as a string `"1"` or a decimal `1.0`?"
- **Why:** Jackson coercion for `Long`.
- **Follow-up ladder:** "How do you make parsing strict?"
- **Strong answer:** "By default Jackson coerces numeric strings to `Long`, so `"1"` works. `1.0` is accepted as `1` (float-to-int truncation is allowed by default) and `1.5` would truncate to 1 [Confirm current Jackson defaults]. Silently truncating an ID is dangerous. Make it strict with `DeserializationFeature.ACCEPT_FLOAT_AS_INT=false` and coercion config disabling string-to-number, on the Kafka deserializer's ObjectMapper."
- **Avoid:** "It errors out."
- **Likelihood:** Low

### GC.84 Q: "A producer adds `"currency": "USD"` to the message. What does your system do?"
- **Why:** Silent semantic corruption: unknown fields are ignored.
- **Follow-up ladder:** "How would you prevent it?" → "Schema evolution rules?"
- **Strong answer:** "Jackson ignores the unknown field (`FAIL_ON_UNKNOWN_PROPERTIES` is off by default in Boot's mapper, and I assume the same for the Kafka `JsonDeserializer` [Confirm]). So 100 USD is processed as **100 of whatever my implicit currency is**. That's a silent financial error. Fixes: make `currency` a required field now (reject if it's missing or unsupported), use a schema registry with compatibility rules (17.9), and consider failing on unknown properties for money messages."
- **Avoid:** "It's fine, it's ignored."
- **Likelihood:** Medium

### GC.85 Q: "The server's clock jumps back an hour (NTP correction). What breaks?"
- **Why:** Time correctness (TR:51 `Instant.now()`).
- **Follow-up ladder:** "Monotonic vs wall clock?"
- **Strong answer:** "`processedAt` values go non-monotonic. 'Last 10 transactions ordered by `processed_at`' shows them out of order, and time-window reports double-count or miss rows. Balances aren't affected. Mitigations: order by `id` (or a sequence) for 'newest' where it matters, use the event time from the producer for business time, and use `System.nanoTime()` (monotonic) for measuring durations, never `Instant` differences. DST doesn't matter, because `Instant` is UTC."
- **Avoid:** "Clocks don't change."
- **Likelihood:** Low

### GC.86 Q: "Disk fills up on the Kafka broker. What happens to producers, your consumer and your DB?"
- **Why:** An infrastructure failure what-if.
- **Follow-up ladder:** "How do you prevent it?"
- **Strong answer:** "The broker can't append, so it marks the log dir offline or shuts down. Producers get errors or timeouts (and buffer, then fail). My consumer can't fetch or commit, so processing stops, but nothing is corrupted. DB state stays consistent with the last processed offset. When it recovers, it resumes from the committed offset (maybe reprocessing a few messages, so idempotency matters). Prevention: retention by size and time, disk usage alerts at 70–80%, and quotas."
- **Avoid:** "Kafka drops old messages to make room automatically." Only retention does that, on its schedule.
- **Likelihood:** Low

### GC.87 Q: "A user sends 100 transfers of 1.00 each, versus one of 100.00. Anything exploitable?"
- **Why:** Business-logic abuse around the incentive threshold.
- **Follow-up ladder:** "What about the reverse: 100.00 back and forth between two accounts?"
- **Strong answer:** "Splitting earns *no* incentive (each is under 100), so no exploit there. The real exploit is the reverse: **ping-pong**. A sends 100 to B (B earns 2), B sends 102 back to A (A earns 2.04), and so on. Two colluding accounts mint unlimited incentive money. Fixes: incentive caps per user per day, no incentive for transfers between linked accounts, a velocity rule ('more than N transfers between the same pair'), fraud scoring before payout, and paying incentives with a delay so they can be clawed back."
- **Avoid:** Missing the ping-pong loop. It's the interesting part.
- **Likelihood:** Medium (great business-sense signal)

### GC.88 Q: "What if the same user id appears as both sender and recipient but in different number types, `1` and `1.0`?"
- **Why:** A follow-up to GC.83 combined with the self-transfer check (TS:84).
- **Follow-up ladder:** "Does `Long.equals` save you?"
- **Strong answer:** "If Jackson coerces `1.0` into `Long 1`, both become `Long(1)` and `equals` correctly rejects it as a self-transfer. If it rejected floats, it would be a deserialization error, so the message is skipped. Either outcome is safe. The unsafe case is truncation like `1.9` → 1 (GC.83)."
- **Avoid:** Overthinking. Reason from the coercion rules.
- **Likelihood:** Low

### GC.89 Q: "The JVM runs out of memory mid-processing. Walk me through the state."
- **Why:** A crash variant beyond 9.4.
- **Follow-up ladder:** "How would you detect and diagnose it?"
- **Strong answer:** "An OOM can kill the thread or the process. If the process dies before the DB commit, the transaction rolls back, the offset isn't committed, and the message is redelivered (safe). If it dies after the commit but before the offset commit, it's a duplicate (W1). With H2 in-memory, everything is gone anyway. Diagnose with `-XX:+HeapDumpOnOutOfMemoryError`, JVM heap metrics, and `-XX:+ExitOnOutOfMemoryError` so the orchestrator restarts a clean process instead of leaving a zombie."
- **Avoid:** "Java handles it."
- **Likelihood:** Low

---

## L. If you mention a related technology: the follow-ups you'll get

### Kafka internals (if you say "Kafka" confidently)

### GC.90 Q: "What's a leader, a follower and the ISR? What does `acks=all` really guarantee?"
- **Why:** You'll name `acks=all` and `min.insync.replicas` (9.5). Expect them to drill in.
- **Follow-up ladder:** "What if the ISR shrinks to 1?" → "Unclean leader election?"
- **Strong answer:** "Each partition has one leader handling reads and writes, and followers replicate from it. The ISR (in-sync replicas) are the followers caught up within `replica.lag.time.max.ms`. `acks=all` means the leader acknowledges only after **all current ISR members** have the write. With `min.insync.replicas=2`, if the ISR drops below 2, producers get `NotEnoughReplicas`. That's availability traded for durability. Unclean leader election (off by default) would let an out-of-sync replica become leader and lose data."
- **Avoid:** "`acks=all` means every replica, always."
- **Likelihood:** Medium

### GC.91 Q: "How does the idempotent producer work?"
- **Why:** You mentioned `enable.idempotence` (8.5).
- **Follow-up ladder:** "Does it dedupe across producer restarts?" → "Then what does?"
- **Strong answer:** "The broker assigns the producer a **producer ID**. Each batch carries a per-partition **sequence number**, and the broker rejects duplicates or out-of-order sequences. That removes duplicates from *producer retries*. It does **not** dedupe across producer restarts (a new PID; the transactional id gives continuity) or business-level duplicates. That's still my consumer's job (8.6)."
- **Avoid:** "It makes everything exactly-once."
- **Likelihood:** Medium

### GC.92 Q: "Why is Kafka so fast?"
- **Why:** A classic once you mention throughput.
- **Follow-up ladder:** "What's zero-copy?"
- **Strong answer:** "Append-only, **sequential disk I/O**. It leans on the OS **page cache** instead of its own cache. **Zero-copy** (`sendfile`) moves bytes from page cache to the socket without passing through user space. Batching and compression (lz4/zstd) per batch. Partitions spread load across brokers and consumers. Consumers track their own offsets, so the broker does no per-message acknowledgement bookkeeping."
- **Avoid:** "It's in-memory." It isn't.
- **Likelihood:** Medium

### GC.93 Q: "What happens during a rebalance? What are eager vs cooperative rebalancing and static membership?"
- **Why:** You mention rebalances in 8.5 and GC.29.
- **Follow-up ladder:** "How do rolling deploys interact with rebalances?"
- **Strong answer:** "Eager: all consumers revoke **all** partitions, then a new assignment is made, so the whole group stops. Cooperative (incremental, e.g. `CooperativeStickyAssignor`) moves only the partitions that need to move, over a couple of rounds, while others keep consuming. **Static membership** (`group.instance.id`) lets a restarting pod rejoin as the same member within `session.timeout.ms` without triggering a rebalance. That's great for rolling deploys in Kubernetes."
- **Avoid:** Vague "it redistributes partitions".
- **Likelihood:** Low-Medium

### GC.94 Q: "How do you choose the number of partitions?"
- **Why:** Follow-up to "add partitions" (10.1).
- **Follow-up ladder:** "Downside of too many?"
- **Strong answer:** "Partitions ≥ max consumer parallelism needed = target throughput ÷ per-consumer throughput, with headroom, since increasing later breaks key → partition mapping (7.14). E.g., 1,000 msg/s at ~100 msg/s per consumer gives 10, so pick 12–24. Too many means more open files, longer leader elections and failovers, more memory in clients, and slower rebalances."
- **Avoid:** "As many as possible."
- **Likelihood:** Medium

### GC.95 Q: "What's log compaction and would you use it here?"
- **Why:** A topic-config follow-up to GC.42.
- **Follow-up ladder:** "What's a tombstone?"
- **Strong answer:** "Compaction keeps at least the **latest value per key** rather than deleting by time. It's good for changelog or state topics (e.g., `user-balance` keyed by userId, so a new consumer can rebuild current balances). A tombstone (null value) deletes a key. Not for `midas-transactions`, since every transfer is a distinct event that matters. Compaction would drop history."
- **Avoid:** Suggesting compaction for the transaction event stream.
- **Likelihood:** Low-Medium

### GC.96 Q: "Kafka vs RabbitMQ vs Redis Streams vs SQS — one line each, when?"
- **Why:** Comparison drill after 3.1.
- **Follow-up ladder:** "What would you pick for Blinkit notifications?"
- **Strong answer:** "Kafka: high-throughput durable log, replay, ordering per partition, many consumer groups (events and pipelines). RabbitMQ: flexible routing, per-message ack and TTLs, work queues, lower throughput, no replay. Redis Streams: lightweight streams with consumer groups, in-memory first, good for small or medium workloads already on Redis. SQS: managed, effortless scaling, at-least-once, FIFO with limits, no replay. For notifications: Kafka for the events, possibly feeding SQS or RabbitMQ work queues per channel."
- **Avoid:** Picking one as "best".
- **Likelihood:** Medium

### Redis (if you say "cache it in Redis")

### GC.97 Q: "Redis is single-threaded — how is it fast? What data structures would you use here?"
- **Why:** Redis basics once you mention it (10.2, 13.2, 13.5).
- **Follow-up ladder:** "When is single-threading a problem?"
- **Strong answer:** "Commands execute on one thread: in-memory, no locks, event-loop I/O multiplexing (6.0+ can use I/O threads for networking). Each command is atomic. A slow command (`KEYS *`, big `SMEMBERS`) blocks everyone. Structures: STRING with TTL for `balance:{id}`; HASH for an order snapshot; ZSET for leaderboards or time-ordered history; GEO for rider locations (13.5); a Lua script for atomic inventory decrements (13.2); INCR for counters and rate limits."
- **Avoid:** "Redis is multi-threaded" (for command execution), or "just a key-value store".
- **Likelihood:** Medium

### GC.98 Q: "Redis persistence — RDB vs AOF? Can you lose data?"
- **Why:** A follow-up to 12.7 ("Redis can lose writes").
- **Follow-up ladder:** "What `appendfsync` would you choose?"
- **Strong answer:** "RDB: periodic point-in-time snapshots, compact, fast restarts, but you lose everything since the last snapshot. AOF: logs every write, with `appendfsync always` (safest, slowest), `everysec` (default, so up to ~1s of loss) or `no`. Both can be combined. Replication is async, so failover can lose acknowledged writes. That's why it's a cache, not the money ledger."
- **Avoid:** "Redis is durable like Postgres."
- **Likelihood:** Low-Medium

### GC.99 Q: "Your cache key expires and 10,000 requests hit the DB at once. What's that called and how do you fix it?"
- **Why:** Cache stampede, a follow-up to 10.2 and 15.21.
- **Follow-up ladder:** "What about keys that never exist (cache penetration)?"
- **Strong answer:** "A cache stampede (thundering herd). Fixes: a per-key lock or single-flight (`SET lock:key NX PX 5000`) so one request recomputes while others wait or serve stale data; jittered TTLs so keys don't expire together; early probabilistic refresh. Penetration (lookups for non-existent IDs every time): cache the negative result briefly, or use a Bloom filter in front."
- **Avoid:** "Increase the TTL."
- **Likelihood:** Medium

### GC.100 Q: "You said 'distributed lock with Redis'. Is a Redis lock safe for money?"
- **Why:** A trap. Bank 12.6 warns against it; they'll push.
- **Follow-up ladder:** "What's a fencing token?"
- **Strong answer:** "Not by itself. `SET key value NX PX ttl` locks can be lost: the holder pauses (GC or network) past the TTL, the lock expires, a second client takes it, and now two holders write. Redlock has debated failure modes too. Safe designs use **fencing tokens**: an increasing number with each lock grant, and the storage rejects writes with an older token. Simpler: do the lock in the database that holds the money (row lock or conditional update)."
- **Avoid:** "Redis lock = safe."
- **Likelihood:** Low-Medium

### GC.101 Q: "How does Redis Cluster shard keys? What happens to a multi-key operation?"
- **Why:** Scaling follow-up.
- **Follow-up ladder:** "What are hash tags?"
- **Strong answer:** "16,384 hash slots, `CRC16(key) mod 16384`, slots spread across masters. Multi-key commands or Lua scripts must touch keys in the same slot, or you get a `CROSSSLOT` error. Use **hash tags**, `{user:42}:balance` and `{user:42}:limits`, so only the braces part is hashed and related keys land together."
- **Avoid:** Not knowing about slots.
- **Likelihood:** Low

### Docker & Kubernetes (if you say "I'd containerize it")

### GC.102 Q: "Image vs container? Docker vs a VM?"
- **Why:** Basics once you say "Dockerfile" (11.8).
- **Follow-up ladder:** "What isolates containers?"
- **Strong answer:** "An image is an immutable, layered filesystem plus metadata. A container is a running instance with a writable layer on top. Containers share the host **kernel**, isolated by namespaces (pid, net, mount…) and limited by cgroups (CPU and memory). VMs virtualize hardware with their own kernel, which is heavier but gives stronger isolation."
- **Avoid:** "Containers are lightweight VMs" with nothing more.
- **Likelihood:** Medium

### GC.103 Q: "Inside a container, what does `localhost` mean? Why would your app fail to reach Kafka?"
- **Why:** The bank's 11.8 gotcha, asked directly.
- **Follow-up ladder:** "How do containers in the same compose file talk?"
- **Strong answer:** "`localhost` is the container's own network namespace, not the host or another container. So `localhost:9092` from the midas container points at nothing. Compose puts services on a shared network with DNS by service name, so use `kafka:9092`. Kafka adds a twist: the broker *advertises* an address after bootstrap. With `PLAINTEXT://localhost:9092` advertised, clients in other containers would connect to themselves. Configure two listeners: an internal one `kafka:29092` for containers and an external one `localhost:9092` for the host."
- **Avoid:** "Docker forwards localhost automatically."
- **Likelihood:** Medium

### GC.104 Q: "Your JVM in a container gets OOM-killed even though the heap looked fine. Why?"
- **Why:** A JVM + container memory follow-up.
- **Follow-up ladder:** "How do you size it?"
- **Strong answer:** "The container limit covers the **whole process**: heap plus metaspace, thread stacks, direct buffers (Kafka/Netty), code cache and GC overhead. JDK 10+ is container-aware, and the default max heap is 25% of the limit (`MaxRAMPercentage`). Set `-XX:MaxRAMPercentage=70–75` to leave room for non-heap. The kernel's OOM killer gives exit 137 (`OOMKilled`), unlike a Java `OutOfMemoryError`. Monitor RSS, not just heap."
- **Avoid:** "Just raise `-Xmx` to the limit."
- **Likelihood:** Low-Medium

### GC.105 Q: "Liveness vs readiness probe — what would each check for midas-core?"
- **Why:** A Kubernetes follow-up to 11.8 and 10.5.
- **Follow-up ladder:** "Should liveness check the DB?"
- **Strong answer:** "Readiness: 'can I serve traffic now?' The DB pool is reachable, the Kafka consumer is assigned, startup is done. When it fails, the pod is removed from the Service (no traffic), but not restarted. Liveness: 'am I stuck beyond recovery?' The JVM is responsive; when it fails, the pod restarts. **Don't** put the DB in liveness: a DB outage would restart every pod in a loop and make things worse. Boot 2.3+ exposes `/actuator/health/liveness` and `/readiness` groups."
- **Avoid:** Using the same check for both.
- **Likelihood:** Medium

### GC.106 Q: "Rolling deploy of 6 consumer pods. What happens to Kafka processing?"
- **Why:** Combines K8s with Kafka rebalancing (GC.93).
- **Follow-up ladder:** "How do you make it smoother?"
- **Strong answer:** "Each pod leaving or joining triggers a rebalance, so with eager rebalancing there are several stop-the-world pauses and some reprocessing (idempotency again). Smoother: cooperative-sticky assignor, static membership (`group.instance.id` = pod name with a StatefulSet), graceful shutdown that commits offsets (SIGTERM → container stop), `maxUnavailable: 1`, and a `terminationGracePeriodSeconds` longer than the worst-case batch processing time."
- **Avoid:** "No impact."
- **Likelihood:** Low-Medium

### WebSockets (if you say "push updates to the client")

### GC.107 Q: "How does a WebSocket connection start? How is it different from HTTP polling, long polling and SSE?"
- **Why:** Bank 13.5 mentions WebSockets; expect a drill.
- **Follow-up ladder:** "Which would you use for order tracking?"
- **Strong answer:** "It starts as an HTTP/1.1 request with `Upgrade: websocket` and `Sec-WebSocket-Key`. The server replies `101 Switching Protocols` and the same TCP connection becomes a full-duplex, framed channel. Polling: repeated requests, simple but wasteful and laggy. Long polling: the server holds a request until data arrives, then the client re-requests. SSE: a one-way server→client stream over plain HTTP with auto-reconnect and simple proxies. For tracking (server→client only), SSE is often enough; WebSockets when the client also sends frequently (chat, rider app)."
- **Avoid:** "WebSockets are a different port or protocol from the start."
- **Likelihood:** Medium

### GC.108 Q: "You have 20 WebSocket servers. Rider location for order 123 arrives at server 3, but the customer is connected to server 11. How does the update reach them?"
- **Why:** The core WebSocket scaling problem.
- **Follow-up ladder:** "What if a server dies?" → "Reconnect storms?"
- **Strong answer:** "A pub/sub backplane. Each server subscribes to channels for the orders its clients watch (Redis pub/sub `order:123`, or a Kafka topic consumed by every gateway with filtering). Server 3 publishes and server 11 pushes to its socket. Alternatively, a routing table (`order → server`) in Redis plus direct forwarding. On server death, clients reconnect with **jittered backoff** to any server, resubscribe, and fetch the latest state via REST so no update is lost. Heartbeats or ping-pong detect dead connections, and connection limits per node plus load balancing keep things even."
- **Avoid:** "Sticky sessions solve it." Stickiness only pins one client to a server; it doesn't route messages between servers.
- **Likelihood:** Medium

### GC.109 Q: "In Spring, how would you implement WebSocket push?"
- **Why:** Ties back to your stack.
- **Follow-up ladder:** "How do you authenticate a WebSocket?"
- **Strong answer:** "`spring-boot-starter-websocket` with STOMP: `@EnableWebSocketMessageBroker`, clients subscribe to `/topic/orders/{id}`, and the server sends via `SimpMessagingTemplate.convertAndSend(...)`. For multiple instances, use a broker relay (RabbitMQ or ActiveMQ STOMP) instead of the simple in-memory broker. Auth: validate a JWT during the handshake (query param or header in the CONNECT frame) and authorize subscriptions so a user can only subscribe to their own order."
- **Avoid:** Inventing APIs. Keep it to this level.
- **Likelihood:** Low-Medium

### Others you might mention

### GC.110 Q: "You said virtual threads would help with the blocking HTTP call. Any catch?"
- **Why:** A follow-up to 15.13 and GC.15.
- **Follow-up ladder:** "Would virtual threads raise your Kafka throughput?"
- **Strong answer:** "Pinning: in JDK 21, blocking inside a `synchronized` block (or native code) pins the virtual thread to its carrier, which loses the benefit (JDK 24 removes most `synchronized` pinning). Also, virtual threads don't increase what downstream systems can handle: the DB pool (10 connections) and the incentive API become the limit, so you need bulkheads. For my consumer, throughput is bound by partition count and serial per-partition processing, not threads, so virtual threads help the REST side more."
- **Avoid:** "Virtual threads make everything faster."
- **Likelihood:** Low-Medium

### GC.111 Q: "You'd use a `@Modifying` UPDATE for the debit (8.2). What happens to entities already in the persistence context?"
- **Why:** A follow-up to the bank's own best answer. Many candidates miss this.
- **Follow-up ladder:** "How do you fix it?" → "Does `flushAutomatically` matter?"
- **Strong answer:** "A bulk JPQL UPDATE goes **straight to the DB** and bypasses the persistence context. If `UserRecord` 1 was loaded earlier in the same transaction, the in-memory entity still shows the **old balance**. Worse, if it's dirty, a later flush could overwrite the new value with the stale one. Fixes: `@Modifying(clearAutomatically = true, flushAutomatically = true)` (flush pending changes before, clear the context after), or don't load the entity at all in that transaction (use `getReferenceById` for the FK, 14.7)."
- **Avoid:** "Hibernate updates the entity automatically."
- **Likelihood:** Medium (if you gave the 8.2 answer)

### GC.112 Q: "In 8.6 you catch `DataIntegrityViolationException` for duplicates. But the exception happens inside a `@Transactional` method. Where exactly do you catch it, and what state is the transaction in?"
- **Why:** A follow-up to the bank's idempotency answer.
- **Follow-up ladder:** "Why can't you catch it inside `process` and continue?" → "What does Postgres do after an error in a tx?"
- **Strong answer:** "With `saveAndFlush`, the INSERT runs immediately and the violation is thrown inside the method. The transaction is then marked **rollback-only**, and in Postgres the transaction is *aborted* ('current transaction is aborted, commands ignored'), so you can't keep using it. Catching it inside and returning normally leads to `UnexpectedRollbackException` at commit. So catch it **outside** the transactional boundary: in the listener, around the call to the proxy. The transaction has fully rolled back; treat it as 'already processed' and ack. Or insert the idempotency row first with `INSERT … ON CONFLICT DO NOTHING` and check the affected row count, so no exception is thrown at all."
- **Avoid:** "Catch it in the service and carry on."
- **Likelihood:** Medium

### GC.113 Q: "You'd invalidate Redis AFTER_COMMIT (10.2). What if that DELETE fails?"
- **Why:** A follow-up to the bank's cache answer.
- **Follow-up ladder:** "How do you make invalidation reliable?"
- **Strong answer:** "The cache stays stale until the TTL expires, which is why the TTL is the safety net. More reliable: drive invalidation from the outbox or CDC stream (GC.115), so a failed delete is retried from the log, or cache with a version and compare. Make sure the 'delete after commit' listener failing can't affect the committed transfer (it doesn't: it runs after commit, so log and alert)."
- **Avoid:** "It won't fail."
- **Likelihood:** Low-Medium

### GC.114 Q: "You'd key by `senderId` (8.4). One merchant sends 40% of all transfers. Now what?"
- **Why:** The hot-key or skewed-partition follow-up.
- **Follow-up ladder:** "Can you split one key across partitions safely?"
- **Strong answer:** "One partition, and so one consumer, gets 40% of the load: a hot partition, and adding consumers doesn't help. Options: a dedicated topic or partition with a faster processing path for that merchant; sub-keying (`merchantId#bucketN`) when per-key ordering can be relaxed, such as for credits; or batching that merchant's transfers. If ordering per sender is required (debits), you can't split without a different consistency mechanism, like per-account sequence numbers or optimistic version checks in the DB."
- **Avoid:** "Add partitions" (the hot key still maps to one).
- **Likelihood:** Low-Medium

### GC.115 Q: "You mentioned Debezium/CDC for the outbox (10.10). How does CDC actually work?"
- **Why:** Name-drop follow-up.
- **Follow-up ladder:** "What does the event look like?" → "Ordering guarantees?"
- **Strong answer:** "The database's write-ahead log (Postgres WAL, via logical decoding and a replication slot; MySQL binlog) is the source of truth for every committed change. Debezium (a Kafka Connect source) reads it and emits change events (before/after row images, operation, transaction metadata) to Kafka. Only **committed** changes appear, in commit order per table/key, with no polling. For the outbox, the Debezium outbox event router turns outbox rows into clean domain events. Caveats: replication slots retain WAL if the connector lags (disk can fill), and you must handle schema changes."
- **Avoid:** "It polls the table."
- **Likelihood:** Low-Medium

### GC.116 Q: "You said JWT. What's inside one? What's the `alg: none` attack? HS256 vs RS256?"
- **Why:** A follow-up to 6.2.
- **Follow-up ladder:** "Where do you put the user id?" → "Is the payload encrypted?"
- **Strong answer:** "Three base64url parts, `header.payload.signature`. The header has `alg` and `kid`. The payload has claims (`sub`, `exp`, `iat`, `iss`, `aud`, roles). It's **signed, not encrypted**, so anyone can read the payload: never put secrets in it. `alg: none` attack: a forged token claims no signature, and a naive verifier accepts it. Fix: the verifier pins the allowed algorithms. HS256: a shared secret, so every verifier can also mint tokens. RS256/ES256: the private key signs, services verify with the public key (JWKS), which is better for many services."
- **Avoid:** "JWT is encrypted."
- **Likelihood:** Medium

### GC.117 Q: "How do you pick the bcrypt cost factor?"
- **Why:** A follow-up to 6.3.
- **Follow-up ladder:** "How do you upgrade existing hashes?"
- **Strong answer:** "Tune it so one hash takes ~100–300ms on production hardware: slow for attackers, tolerable per login. Cost is exponential (each +1 doubles the work). Spring's `BCryptPasswordEncoder` defaults to 10. Upgrade path: on successful login, if the stored hash's cost is below the target, re-hash with the new cost (`DelegatingPasswordEncoder` / `upgradeEncoding`). Note bcrypt's 72-byte password limit; argon2id is the modern alternative."
- **Avoid:** "As high as possible" (that's a login DoS vector).
- **Likelihood:** Low

### GC.118 Q: "Why REST between midas and the incentive service, not gRPC?"
- **Why:** If you say "microservices", they ask about protocols.
- **Follow-up ladder:** "When would gRPC win?"
- **Strong answer:** "The simulation's API was REST/JSON, and REST is universal, debuggable with curl, and easy for browsers. gRPC uses HTTP/2 + Protobuf: smaller binary payloads, a strict schema with codegen (a contract that's enforced), streaming, multiplexing and deadlines propagated across calls. It wins for high-volume internal service-to-service calls. The trade-offs: tooling, no browser support without a proxy, and harder ad-hoc debugging."
- **Avoid:** "gRPC is always faster, so always better."
- **Likelihood:** Low-Medium

### GC.119 Q: "How do you check that your Postgres query actually uses the index?"
- **Why:** A follow-up to 4.3 and 4.7.
- **Follow-up ladder:** "Why might it choose a seq scan anyway?"
- **Strong answer:** "`EXPLAIN (ANALYZE, BUFFERS) SELECT …` shows the plan with actual timings: look for `Index Scan` / `Index Only Scan` / `Bitmap Index Scan` vs `Seq Scan`. The planner may choose a seq scan when the table is small, the predicate matches a big fraction of rows (low selectivity), statistics are stale (run `ANALYZE`), or the query wraps the column in a function or cast (`WHERE CAST(processed_at AS DATE) = …` can't use a plain index on `processed_at`; use a range instead). `OR` across two columns can also defeat it (4.7)."
- **Avoid:** "Indexes are always used."
- **Likelihood:** Medium

### GC.120 Q: "Your saga compensation (10.4) — what if the compensating step itself fails?"
- **Why:** A follow-up to the bank's sharding answer.
- **Follow-up ladder:** "Can a saga be isolated?"
- **Strong answer:** "Compensations must be **idempotent and retried until they succeed** (persisted saga state, retry with backoff). If they still can't succeed (the account is closed), escalate to a manual-intervention queue with alerts, because money can't just be dropped. Sagas aren't isolated: intermediate states (debited, not yet credited) are visible. Mitigate with semantic locks (status PENDING), pending balances, and designing steps so 'reserve' comes before 'commit'."
- **Avoid:** "Compensation can't fail."
- **Likelihood:** Low-Medium

*End of Gap-check additions (GC.1–GC.120).*

---

# Your story — drafted personal answers

> These replace the **[Confirm]** placeholders in 1.3, 1.5, 11.12, 1.8, GC.2, GC.3, GC.1, 12.2, 16.17 and GC.9.
> **How they were built:** from what the repo actually shows, plus the publicly known structure of the Forage JPMorganChase "Midas" simulation (5 tasks: project setup → Kafka listener → H2/JPA → Incentive API call → balance REST endpoint, with a starter repo and a provided incentive JAR).
> **Before you use them:** re-read them against your own memory and your Forage task pages. Anything marked ✏️ is a choice only you can make. **Never say something here that isn't true for you.** Interviewers dig one level deeper on exactly these questions.

---

## P.1 "What did Forage give you and what did you write?" (replaces 1.3)

**Script (≈45 seconds):**
> "Forage gave me the problem and the scaffolding. There were five tasks: set up the project, consume transactions from Kafka, validate them and store them in an H2 database with JPA, call an Incentive REST API, and expose a balance endpoint. They provided a starter repository with the package layout and some base classes, the expected port and config names, the Incentive API as a pre-built JAR, and test classes to check each task.
>
> What I wrote was the actual behaviour: the Kafka listener, all the validation rules, the balance updates and audit record inside one transaction, the REST client to the incentive API, and the balance controller.
>
> After finishing the simulation, I rebuilt it into a standalone repository and went beyond what the tasks checked. I added proper Kafka error handling so a bad message can't block the consumer, timeouts plus a fallback on the incentive call, BigDecimal with fixed precision for money, my own small incentive-api module so the project runs without the provided JAR, Docker Compose for Kafka, sample-data scripts, my own test suite including an end-to-end test on embedded Kafka, and GitHub Actions CI."

**If they push: "Which classes existed before you started?"**
> "From memory, the starter had the `foundation` package with the transaction message class, a `UserRecord` entity with a repository, and a config key for the topic. It also had Forage's task test classes and some test helpers that sent sample transactions and loaded users. My final repo doesn't keep their task tests. I replaced them with my own tests, organized by layer."
- ✏️ **Check this against your Forage starter repo or task pages before the interview.** Name only the classes you're sure about. If unsure, say: "I don't remember the exact file list, but the split was: they gave the skeleton and the checks; I wrote the logic."

**Evidence in the repo that supports this** (useful if they look):
- `incentive-api/` and README:144-146 say explicitly that the original was a pre-built JAR.
- There are no Forage `TaskOneTests`…`TaskFiveTests` in the repo; the tests are your own layer-based ones.
- README:5 credits the simulation openly.

**Avoid:** "I designed the whole thing from scratch." Also avoid the opposite, "I just followed the instructions." The truth (they set the problem, you built and extended it) sounds strongest.

---

## P.2 "What was the hardest part / a bug you hit?" (replaces 1.5 and 11.12)

> ✏️ **Pick the ONE story that actually happened to you.** All four are real problems this code solves, so the *technical content* is accurate either way. But only say "I hit this bug" if you did. If none happened as a bug, use the framing **"the trickiest thing to get right was…"**, which is honest and just as good.

### Option A — Kafka message deserialization (the most likely one in this simulation)
> "The trickiest part was getting Kafka messages to deserialize into my `Transaction` class. Spring's `JsonDeserializer` by default looks for a type header with the *producer's* Java class name, and it only trusts certain packages. Messages coming from another producer, or from the console producer in my script, either had no header or the wrong class name, so deserialization failed.
>
> The worse part was what happens when deserialization fails. It happens inside the consumer's poll, before my listener even runs, so without handling, the consumer keeps re-reading the same bad message forever and the partition is stuck.
>
> I fixed it in two steps. First, ignore type headers and always deserialize to `Transaction` (`use.type.headers: false` plus a default type). Second, wrap the deserializer in `ErrorHandlingDeserializer`, so a bad message is handed to the error handler and skipped instead of looping."
- **Follow-up you should expect:** "How did you find out it was looping?" → ✏️ your real answer (e.g., "the log repeated the same exception for the same offset").
- **Code to point at:** core.yml:25-29, KC:21-24.

### Option B — the startup race with demo users
> "Users are seeded at startup, but the Kafka listener could start consuming before the seed data existed, so early transactions got rejected as 'unknown user'. A `CommandLineRunner` didn't help, because it runs after the listener containers have already started. I moved seeding into a `SmartInitializingSingleton`, which runs after all beans are created but before Spring starts the Kafka containers and the web server."
- **Code:** DDL:17-18, 22, 33.

### Option C — BigDecimal comparison
> "A balance assertion failed even though the numbers looked identical: `70.0` versus `70.00`. `BigDecimal.equals` compares scale as well as value. I switched to `compareTo` in the code (`canAfford`) and `isEqualByComparingTo` in the tests, and made the database columns `NUMERIC(19,2)`."
- **Code:** UR:44, the tests' assertions.

### Option D — a flaky async test
> "My first end-to-end test used `Thread.sleep` after sending to Kafka, and it failed randomly when the consumer was slow to start. I replaced it with Awaitility, which polls until the condition is true, up to a timeout. I also wait for a specific record count rather than a fixed time."
- **Code:** KafkaIntegrationTest:50-51.

**Avoid:** combining several into one dramatic story, or claiming days of debugging you can't describe.

---

## P.3 "When did you do this? Why are there no commits?" (replaces 1.8, GC.2, GC.3)

**First, fix the repo (do this before the interview):**
1. `git add` and commit what exists now **with today's real date**, then push to GitHub.
2. **Do not backdate commits** (`GIT_COMMITTER_DATE` / `--date` tricks). If an interviewer notices, it destroys trust, and it's the one thing here that would actually be dishonest.
3. From now on, make small, real commits for any improvement you make (e.g., add the boundary test GC.63, the DLT, the `scale <= 2` check). Even 5–10 genuine commits before the interview makes the history look alive, and gives you fresh, true things to talk about.

**Script:**
> "I did the Forage simulation in December 2025, inside their environment with their starter repository. Afterwards I rebuilt it as a standalone project: I restructured it, replaced their task checks with my own tests, added the incentive-api stand-in, error handling, Docker Compose and CI. ✏️ *[I only pushed that rebuilt version recently / on <month>]*, which is why the GitHub history starts with one large commit rather than the step-by-step history. The commits after that are improvements I've made since, like ✏️ *[name a real one]*."

**If they ask "how do you normally commit?":**
> "Small commits, one logical change each, message in the imperative: 'Add dead-letter topic for failed transactions'. Feature branch, pull request, and CI must pass before merging."

**Avoid:** dates that differ between your resume, LinkedIn and README. ✏️ Check that all three say December 2025.

---

## P.4 "Did you use AI tools?" (replaces GC.1)

> ✏️ **Pick the version that's true.** Interviewers in 2026 assume some AI use. What hurts you is denying it and then failing to explain a line.

**Version 1 — you used AI for parts of it:**
> "Yes, as an assistant, the way I'd use it at work. I used it for ✏️ *[e.g., boilerplate like entity getters and test scaffolding, the README wording, and to explain Spring Kafka error-handling options]*. The design decisions and the logic I worked out myself, and I verified everything: I traced the sample batch by hand to check the README's numbers, and I checked how the retry config actually behaves. Pick any line and I'll explain it. Or I can change something live, like adding a stored rejection reason."

**Version 2 — you didn't use AI for the code:**
> "Not for the code. I worked from the Forage task instructions, the Spring and Kafka docs, and Stack Overflow when I got stuck, for example ✏️ *[the deserialization issue]*. I have used AI since then to prepare for interviews, to quiz myself on what could go wrong with the design. That's how I found gaps like the missing idempotency key."

**Either way, prepare for the proof request:** be ready to make one small change live. Easiest candidates:
- add a `>= 100.00` boundary test (GC.63)
- reject amounts with more than 2 decimal places in `validate()` (W9)
- replace `existsById` + `findById` with one `findById` (W10)

**Avoid:** "No" if the answer is yes. "AI wrote it and I reviewed it" (it undersells you if you actually understand it).

---

## P.5 "What would you do differently from day one?" (replaces 12.2)

These are your **genuine opinions about the design**, drawn from the real gaps in the code. Say them in first person; you can stand behind all of them.

> "Four things.
>
> **First, I'd design the message with a transaction ID from the start.** Without one, I can't tell a redelivered message from a new transfer, so a crash at the wrong moment can apply the same transfer twice. Adding it later means changing the producer's contract, which is harder than getting it right at the start.
>
> **Second, I'd keep the database transaction short.** I put the incentive API call inside the transaction because it was the simplest way to keep everything atomic. But it means a slow API holds a database connection for up to seven seconds. I'd fetch the incentive first, or make incentives a separate asynchronous step.
>
> **Third, I'd test the failure paths as early as the happy path.** My README says a malformed message is skipped, but I never wrote a test that sends one. The same goes for a timeout from the incentive API, and for a duplicate message. Those are the cases that break in production.
>
> **Fourth, I'd use Postgres with Testcontainers from the beginning instead of H2.** With an in-memory database, restarting the app quietly resets all balances while Kafka remembers it already processed those messages. And H2 hides differences like locking behaviour and which indexes are created automatically."

**Short version (15 seconds):** "Idempotency key in the message from day one, short transactions with no network calls inside, tests for failure paths not just happy paths, and a real database from the start."

---

## P.6 "Why `com.jpmc`, port 33400 and the `foundation` package?" (replaces 16.17 and GC.9)

**Script:**
> "Those came from the Forage simulation. The starter project used the `com.jpmc.midascore` package and a `foundation` package for the message classes. The tasks specified port 33400 for the balance endpoint and the `general.kafka-topic` config key, and their test harness expected those exact names. I kept them so the project stayed recognisable as the simulation.
>
> If I turned it into a portfolio project independent of Forage, I'd rename the groupId and packages to my own namespace, like `io.github.<my-username>.midas`, because a `com.jpmc` groupId implies an affiliation with JPMorgan that I don't have. I'd also rename `foundation` to something clearer like `dto` or `messaging`, and `UserRecord` to `UserEntity`, so it isn't confused with Java records."

- ✏️ **Check against your Forage task pages** that port 33400 and `general.kafka-topic` were specified by them. The repo's own evidence: `general.` is an unusual namespace (core.yml:4-5), and the README credits the simulation.
- **Follow-up: "Why didn't you rename them already?"** → "Keeping the simulation's names made it easy to compare with the original tasks. Renaming is a mechanical refactor I'd do before publishing it as my own project." (Or just do the rename before the interview and say so.)

**Avoid:** implying you picked `com.jpmc` yourself, or that you have any connection to JPMorgan beyond the public simulation.

---

## Checklist before using these answers
- [ ] Re-read P.1 against your Forage task pages; keep only class names you're sure about.
- [ ] Pick **one** story in P.2 and rehearse its follow-up ("how did you find it?").
- [ ] Commit and push the repo with real dates (P.3). Make a few genuine improvement commits.
- [ ] Choose your truthful version in P.4, and practise one live change.
- [ ] Make sure the date (Dec 2025) matches across your resume, LinkedIn and README.
