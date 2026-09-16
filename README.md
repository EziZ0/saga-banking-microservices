# Digital Banking System

![Java](https://img.shields.io/badge/Java-17-orange)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-4.1.1-brightgreen)
![Spring Cloud](https://img.shields.io/badge/Spring%20Cloud-2025.1.3-6DB33F)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-Event--Driven-231F20)
![Redis](https://img.shields.io/badge/Redis-Cache%20%2F%20State-DC382D)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED)

A backend simulation of a digital bank built as a set of independent **Spring Boot microservices**. It supports account management, peer-to-peer fund transfers, third-party payment collection (Razorpay), real-time rule-based fraud detection, OTP step-up verification, and event-driven customer notifications — all coordinated asynchronously through **Apache Kafka** using the **SAGA (choreography) pattern** for distributed transactions.

---

## Table of Contents

1. [Architecture](#architecture)
2. [Services](#services)
3. [The Transfer Saga — How Money Actually Moves](#the-transfer-saga--how-money-actually-moves)
4. [Kafka Topics Reference](#kafka-topics-reference)
5. [Tech Stack](#tech-stack)
6. [Project Structure](#project-structure)

---

## Architecture

The system follows a **microservices + event-driven architecture**. Synchronous REST/Feign calls are used only where an immediate answer is required (e.g. "deduct this balance now"); everything else — fraud checks, notifications, status propagation — happens asynchronously over Kafka so that services stay decoupled and independently deployable.

```mermaid
flowchart TB
    Client([Client / Frontend])

    subgraph Gateway["API Gateway (:8080)"]
        GW[Spring Cloud Gateway<br/>Routing + Rate Limiting]
    end

    subgraph Services["Core Services"]
        ACC[account-service :8081<br/>MySQL: account_db]
        TXN[transaction-service :8082<br/>MySQL: transaction_db]
        PAY[payment-service :8083<br/>MySQL: payment_db]
        FRAUD[fraud-detection-service :8084<br/>stateless]
        NOTIF[notification-service :8085<br/>stateless]
    end

    subgraph Infra["Infrastructure"]
        KAFKA[(Apache Kafka)]
        REDIS[(Redis)]
        RAZORPAY[[Razorpay API]]
    end

    Client --> GW
    GW -->|/api/v1/accounts/**| ACC
    GW -->|/api/v1/transactions/**| TXN
    GW -->|/api/v1/payments/**| PAY

    TXN -- Feign: deduct/credit --> ACC
    FRAUD -- Feign: get balance --> ACC
    PAY -- REST --> RAZORPAY

    TXN <-->|events| KAFKA
    ACC <-->|events| KAFKA
    FRAUD <-->|events| KAFKA
    PAY -->|events| KAFKA
    KAFKA -->|events| NOTIF

    TXN -.OTP store.-> REDIS
    FRAUD -.velocity/avg counters.-> REDIS
    GW -.rate-limit counters.-> REDIS
```

## Services

| Service | Port | Datastore | Responsibility |
|---|---|---|---|
| **api-gateway** | 8080 | Redis (rate limiter) | Single entry point; routes requests to downstream services |
| **account-service** | 8081 | MySQL (`account_db`) | Owns account data and balances; exposes deduct/credit endpoints used inside the SAGA |
| **transaction-service** | 8082 | MySQL (`transaction_db`) + Redis (OTP) | Orchestrates transfers, drives the SAGA, handles OTP verification |
| **payment-service** | 8083 | MySQL (`payment_db`) | Creates Razorpay orders, handles Razorpay webhooks for deposits |
| **fraud-detection-service** | 8084 | Redis (counters) | Consumes every initiated transaction and scores it for fraud |
| **notification-service** | 8085 | — (stateless) | Consumes all customer-facing events and logs alerts (debit/credit/OTP/fraud/refund/payment) |

## The Transfer Saga — How Money Actually Moves

A transfer is not a single database transaction — it's a **choreographed SAGA** spread across `transaction-service`, `account-service`, and `fraud-detection-service`, coordinated entirely through Kafka events. This is the core design of the system, so it's worth walking through in full.

```mermaid
sequenceDiagram
    actor U as User
    participant TXN as transaction-service
    participant ACC as account-service
    participant K as Kafka
    participant FRAUD as fraud-detection-service
    participant NOTIF as notification-service

    U->>TXN: POST /transactions/transfer
    TXN->>ACC: PUT /accounts/{sender}/deduct (Feign)
    ACC-->>TXN: OK — balance deducted
    TXN->>TXN: save Transaction as PROCESSING
    TXN->>K: publish transaction.initiated
    TXN-->>U: 201 Created (PROCESSING)

    K->>FRAUD: transaction.initiated
    FRAUD->>ACC: GET /accounts/{sender}/balance (Feign)
    ACC-->>FRAUD: balance

    alt Transaction looks clean
        FRAUD->>K: publish fraud.check.clean
        K->>TXN: fraud.check.clean
        TXN->>TXN: mark COMPLETED
        TXN->>K: publish transaction.completed
        K->>ACC: transaction.completed
        ACC->>ACC: credit receiver
        K->>NOTIF: transaction.completed
        NOTIF-->>U: DEBIT / CREDIT alerts (logged)
    else Suspicious activity detected
        FRAUD->>K: publish verification.required
        K->>TXN: verification.required
        TXN->>TXN: generate OTP, store in Redis (TTL 5m)
        TXN->>TXN: mark PENDING_VERIFICATION
        TXN->>K: publish transaction.otp.generated
        K->>NOTIF: transaction.otp.generated
        NOTIF-->>U: OTP alert (logged)

        U->>TXN: POST /transactions/{id}/verify?otp=...

        alt OTP correct
            TXN->>TXN: mark COMPLETED
            TXN->>K: publish transaction.completed
            K->>ACC: transaction.completed
            ACC->>ACC: credit receiver
            K->>NOTIF: transaction.completed
        else OTP wrong or expired
            TXN->>K: publish fraud.detected (only if wrong OTP, not on expiry)
            K->>ACC: fraud.detected
            ACC->>ACC: block sender account
            TXN->>ACC: PUT /accounts/{sender}/credit (Feign) — compensate
            ACC-->>TXN: OK — refunded
            TXN->>TXN: mark FLAGGED
            TXN->>K: publish transaction.refunded
            K->>NOTIF: transaction.refunded / fraud.detected
            NOTIF-->>U: REFUND / SECURITY alerts (logged)
        end
    end
```

**Why the deduction happens before the fraud check:** the sender's balance is deducted synchronously up front (SAGA step 1) so the money is provisionally "in flight" and can't be double-spent while the asynchronous fraud check runs. If the fraud check or OTP step fails, `account-service`'s `credit` endpoint is invoked as the **compensating transaction** (SAGA step 4) to reverse the deduction — this is the same endpoint used for crediting the receiver on a successful transfer.

## Kafka Topics Reference

| Topic | Producer | Consumer(s) | Purpose |
|---|---|---|---|
| `transaction.initiated` | transaction-service | fraud-detection-service | New transfer needs a fraud check |
| `fraud.check.clean` | fraud-detection-service | transaction-service | Transaction passed all fraud checks |
| `verification.required` | fraud-detection-service | transaction-service | Transaction needs OTP step-up |
| `transaction.otp.generated` | transaction-service | notification-service | Deliver OTP to the user |
| `transaction.completed` | transaction-service | account-service, notification-service | Credit receiver; send debit/credit alerts |
| `transaction.refunded` | transaction-service | notification-service | SAGA compensation happened; notify sender |
| `fraud.detected` | transaction-service | account-service, notification-service | Block the account; alert the user |
| `payment.completed` | payment-service | notification-service | Razorpay deposit succeeded |
| `payment.failed` | payment-service | notification-service | Razorpay deposit failed |

## Tech Stack

- **Language / Runtime:** Java 17
- **Framework:** Spring Boot 4.1.1, Spring Cloud 2025.1.3
- **API Gateway:** Spring Cloud Gateway (WebFlux, reactive)
- **Persistence:** Spring Data JPA + MySQL (one schema per service — `account_db`, `transaction_db`, `payment_db`)
- **Messaging:** Apache Kafka (via Spring Kafka), event payloads as JSON
- **Caching / Ephemeral state:** Redis (OTP storage, fraud counters, gateway rate limiting)
- **Inter-service calls:** OpenFeign
- **Payments:** Razorpay Java SDK
- **Other:** Lombok, ModelMapper, Spring Validation, Spring Boot Actuator
- **Containerization:** Docker Compose (Redis, Zookeeper, Kafka)

## Project Structure

```
DigitalBankingSystem/
├── api-gateway/               # Routing + rate limiting (port 8080)
├── account-service/           # Accounts & balances (port 8081)
├── transaction-service/       # Transfer SAGA orchestration (port 8082)
├── payment-service/           # Razorpay integration (port 8083)
├── fraud-detection-service/   # Rule-based fraud engine (port 8084)
├── notification-service/      # Event-driven alerts (port 8085)
└── docker-compose.yml         # Redis + Kafka + Zookeeper
```

Each service is an independent Maven project (own `pom.xml`, `mvnw`) following the standard `controller → service → repository` layering, with `dto`, `entity`/`model`, `event`, and `client` (Feign) packages where relevant.
