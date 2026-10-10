# Week Project: **FraudStream** — real-time card fraud detection with Java, Kafka (MSK) and AWS Lambda

## Context
The second portfolio project for a Cloud/DevOps job hunt (after CloudDrop). CloudDrop covered containers, k3s, GitHub Actions and GitOps. **FraudStream covers event-driven and serverless work**: **Java 21**, **Apache Kafka on Amazon MSK**, **AWS Lambda**, and the DevOps side: **Jenkins**, **IAM (least privilege, no static keys)** and **Terraform** on AWS. It is built in one week.

The real-life case: a card payment processor has to decide within seconds whether a transaction looks fraudulent. Banks and fintechs do this with Kafka and Java.

**Decisions made:** Amazon MSK provisioned (the realistic choice at low cost), Terraform (builds on CloudDrop), Jenkins on EC2 provisioned by Terraform (IAM role, no keys), CloudWatch for observability.

## What the system does
1. A **payment API** (`POST /transactions`) receives card transactions (cardId, amount, currency, merchant, MCC, country, lat/lon, timestamp)
2. It validates each one and publishes it to the Kafka topic **`transactions`**, keyed by `cardId` (so each card's events stay in order inside a partition)
3. A **fraud-detector Lambda** consumes the topic (Lambda event source mapping for MSK) and runs a **rules engine**:
   - `HighAmountRule`: the amount is more than 5× the card's average, or above a fixed limit
   - `VelocityRule`: more than 5 transactions on the same card within 60 s
   - `ImpossibleTravelRule`: two transactions whose implied speed is over 900 km/h (haversine distance ÷ time between them)
   - `RiskyMerchantRule`: high-risk MCCs (gambling, crypto, wire transfer)
   - The rule scores are added up into a decision: **APPROVE / REVIEW / BLOCK**
4. It saves the decision to **DynamoDB** and updates the card profile (last location, last timestamp, rolling average, velocity counter with TTL)
5. BLOCK and REVIEW decisions are published to the topic **`fraud-alerts`**. An **alert-notifier Lambda** consumes that topic and sends an **SNS** email
6. Messages that can't be processed (bad JSON, failed validation) go to the topic **`transactions.dlq`** instead of blocking the partition
7. `GET /transactions/{id}` returns the decision and the rules that fired

## Architecture
```
Simulator (Java CLI: normal traffic + injected fraud patterns)
      │ HTTPS
      ▼
API Gateway (HTTP API) ──> Lambda "ingest-api" (Java 21, SnapStart, in VPC)
                                │  produce (SASL/IAM, port 9098)
                                ▼
       ┌──────────── Amazon MSK (2× kafka.t3.small, 2 AZs, private subnets) ───────────┐
       │  transactions (6 partitions)   fraud-alerts (3)   transactions.dlq (1)          │
       └───────┬───────────────────────────────▲──────────────────┬───────────────────┘
               │ event source mapping           │ produce          │ event source mapping
               ▼                                │                  ▼
     Lambda "fraud-detector" (in VPC) ──────────┘        Lambda "alert-notifier" ──> SNS (email)
               │
               └──> DynamoDB (transactions, card-profile)   ← via the free Gateway VPC endpoint

VPC: 2 private subnets (MSK + Lambdas) + 1 public subnet (Jenkins). No NAT gateway:
     DynamoDB gateway endpoint + STS and Lambda interface endpoints (the MSK event source mapping needs them)
Jenkins EC2 (Docker, JCasC) ── IAM instance profile ──assume──> fraudstream-deployer role
     reached through an SSM port-forward (no open inbound ports) | polls GitHub for changes
Terraform state ──> S3 backend (with lockfile) | CloudWatch dashboard + alarms | AWS Budget
```

## Repo layout
```
FraudStream/
├── PLAN.md
├── pom.xml                       Maven parent (Java 21, JUnit 5, JaCoCo gate, Checkstyle, SpotBugs)
├── common/                       model (Transaction, FraudDecision), Jackson serde, KafkaProducerFactory (IAM auth)
├── ingest-api/                   API Gateway handlers: POST /transactions, GET /transactions/{id}
├── fraud-detector/               KafkaEvent handler, rules/ (one class per rule), ScoringEngine, repositories
├── alert-notifier/               KafkaEvent handler → SNS
├── simulator/                    CLI load generator with fraud scenarios
├── local/                        docker-compose.yml (Kafka KRaft, kafka-ui, dynamodb-local)
├── infra/
│   ├── platform/                 applied by you as admin: vpc.tf, endpoints.tf, msk.tf, jenkins.tf, iam.tf
│   │                             (Lambda execution roles + deployer role), s3.tf (artifacts), budget.tf
│   └── app/                      applied by Jenkins: lambda.tf, apigw.tf, dynamodb.tf, sns.tf, esm.tf,
│                                 cloudwatch.tf (dashboard + alarms); reads platform outputs via remote state
├── jenkins/                      Dockerfile, plugins.txt, casc.yaml (JCasC), user_data.sh
├── Jenkinsfile
├── scripts/                      create-topics.sh (idempotent, IAM auth), smoke-test.sh
└── README.md, docs/              architecture diagram, screenshots
```

**IAM design** (a key interview topic):
- The Lambda execution roles live in **`platform`**, created by the admin. The `app` stack, which Jenkins applies, can only `iam:PassRole` those specific roles. Jenkins can't create roles, so it can't escalate its own privileges
- Each Lambda gets its own role with only what it needs:
  - ingest: `kafka-cluster:Connect/WriteData/DescribeTopic` on `transactions`, plus `dynamodb:GetItem`
  - detector: read `transactions` + its consumer group, write `fraud-alerts`/`dlq`, `PutItem/UpdateItem/GetItem` on its 2 tables, and the ENI permissions a VPC Lambda needs
  - notifier: read `fraud-alerts`, plus `sns:Publish` on 1 topic
- Jenkins instance profile: state bucket, artifacts bucket and `sts:AssumeRole` into `fraudstream-deployer` only. The deployer role is scoped to `fraudstream-*` resource ARNs
- There are **no access keys anywhere**. You reach Jenkins through SSM, and MSK uses IAM auth instead of SCRAM passwords

## Timeline (7 days, ~4–5h/day)

### Day 0 — prep (1–2h)
- Install JDK 21, Maven, Docker, Terraform, AWS CLI and the Session Manager plugin
- **Set up an AWS Budget alert (US$15)** before creating anything
- Create the GitHub repo and `.gitignore` (`target/`, `*.tfstate`, `.terraform/`, `.env`); create the state bucket by hand (or reuse the one from CloudDrop with a new key)

### Day 1 — domain + rules engine, TDD (local only)
- Maven multi-module skeleton; `Transaction` record, `FraudDecision`, `RuleResult`
- Write the 4 rules and `ScoringEngine` test-first: **JUnit 5 + AssertJ**, parameterized tests for the edge cases (exact thresholds, the 60 s window boundary, the antimeridian in the haversine distance, missing location)
- Configure the JaCoCo gate at **≥ 80% line coverage** so `mvn verify` fails below it

### Day 2 — Lambda handlers + Kafka, locally
- `ingest-api` handler (`APIGatewayV2HTTPEvent`): validation (Jakarta Bean Validation) and producing with `acks=all` and an idempotent producer. Unit tests use Kafka's **`MockProducer`**
- `fraud-detector` handler (`KafkaEvent`): base64-decodes the batch, runs the rules, writes to DynamoDB with a **conditional put on `transactionId`** (idempotency, because Kafka is at-least-once), and sends poison messages to the DLQ. Unit tests use Mockito for `DynamoDbClient`
- `alert-notifier` handler, with unit tests that mock `SnsClient`
- `local/docker-compose.yml`: Kafka (KRaft) + kafka-ui + dynamodb-local. **Integration tests** (`*IT.java`, run by maven-failsafe) use **Testcontainers** Kafka
- Use Powertools for AWS Lambda (Java) for structured JSON logs and EMF metrics (`TransactionsScored`, `FraudBlocked`)

### Day 3 — Terraform `platform` (VPC, MSK, IAM)
- `vpc.tf`: 2 private subnets in 2 AZs, 1 public subnet, IGW, no NAT
- `endpoints.tf`: DynamoDB gateway endpoint; STS and Lambda interface endpoints
- `msk.tf`: 2× `kafka.t3.small`, IAM client auth, TLS in transit, encryption at rest, a custom configuration with `auto.create.topics.enable=false` and replication factor 2. The security group **references itself on 9098** (the event source mapping uses the cluster's SG, which is a common gotcha). MSK creation takes about 25 min, so start it first
- `iam.tf`: Lambda execution roles, the Jenkins instance profile and the deployer role, as described above
- `s3.tf`: a versioned, encrypted artifacts bucket with Block Public Access; `budget.tf`
- Run `terraform fmt`, `validate`, `plan`, `apply`

### Day 4 — Terraform `app` + first manual deploy
- `lambda.tf`: 3 functions on the `java21` runtime (arm64, which is cheaper), code from S3 at `s3://artifacts/<git-sha>/*.jar`. Turn on **SnapStart** for ingest-api (published version + alias). The Kafka producer is created lazily or re-created after restore, because SnapStart doesn't keep network connections alive across restores
- `esm.tf`: event source mappings for MSK (`transactions` → detector, `fraud-alerts` → notifier), with batch size, batching window and starting position LATEST
- `apigw.tf` (HTTP API, throttling), `dynamodb.tf` (on-demand, TTL on the velocity counters, PITR), `sns.tf` (email subscription)
- `scripts/create-topics.sh`: `kafka-topics.sh --create --if-not-exists` with IAM auth. It runs from the Jenkins box, which is inside the VPC
- Deploy by hand once and send a `curl` POST to check the whole flow works before automating it

### Day 5 — Jenkins (the CI/CD core)
- `jenkins.tf`: a t3.medium in the public subnet, SG with **no inbound rules**, IMDSv2 required, an encrypted EBS volume, and `user_data.sh` that installs Docker and runs the Jenkins image
- `jenkins/`: a custom image built FROM `jenkins/jenkins:lts-jdk21` with `plugins.txt` (pipeline, git, junit, jacoco, warnings-ng, pipeline-aws, configuration-as-code) and **JCasC** `casc.yaml`, so admin user, credentials-free AWS access and the job are all set up as code
- Open the UI with `aws ssm start-session --document-name AWS-StartPortForwardingSession` → `localhost:8080`
- `Jenkinsfile` (declarative) stages:
  1. **Checkout**, then **Build & Unit Test**: `mvn -B verify`, publishing JUnit + JaCoCo reports (the build fails under 80%)
  2. **Static analysis**: Checkstyle + SpotBugs, published with warnings-ng
  3. **Integration tests**: Testcontainers, using the Docker socket on the agent
  4. **IaC checks**: `terraform fmt -check`, `validate`, `tflint`, `checkov`
  5. **Package & publish**: shaded jars uploaded to S3 under the git SHA
  6. **Terraform plan** (`infra/app`) under `withAWS(role: 'fraudstream-deployer')`, then **manual approval** (`input`, only on `main`), then **apply**
  7. **Ensure topics**, then **smoke test**: POST a transaction known to be fraudulent and check that DynamoDB shows `BLOCK`
- `post { always { junit ...; cleanWs() } }`; the job polls the repo every 5 min (no inbound webhook is needed)

### Day 6 — resilience, observability, load
- `cloudwatch.tf`: a dashboard (requests/s, p95 duration, fraud rate, **ESM `OffsetLag`**, errors, throttles) and alarms (OffsetLag over a threshold, Lambda errors, DLQ messages) → SNS
- Simulator: 50–200 transactions/s with scenarios (`--scenario=velocity|travel|high-amount|normal`). Watch OffsetLag rise and fall, and watch Lambda scale with the partitions
- Chaos tests: send malformed JSON (it should land in the DLQ with no stuck partition) and re-send a duplicate `transactionId` (it should be stored once)
- Take screenshots: Jenkins Blue Ocean green, the dashboard during the load test, the alert email

### Day 7 — docs + teardown
- README: the problem, the architecture diagram, the IAM design, how to run it, screenshots, **cost**, "what I learned" (SnapStart + Kafka, ESM networking, idempotency), "next steps"
- Record a short demo video (the simulator injects fraud → the email arrives)
- Run **`terraform destroy`** on `app` and then `platform`

## Optional extras
- **Avro + AWS Glue Schema Registry** for schema evolution
- Replace the static rules with a model on SageMaker or Bedrock and keep the rules as a fallback
- Jenkins agents on ephemeral EC2 or ECS (EC2 Fleet plugin) instead of building on the controller
- Multibranch pipeline with a separate `dev`/`prod` workspace, and promotion between them
- AWS X-Ray tracing from start to finish; Lambda Powertools idempotency utility
- Kafka Streams app (on ECS) for windowed aggregations, compared with Lambda in the README

## Estimated cost
- While the platform is up: MSK 2× t3.small ≈ US$0.09/h + interface endpoints ≈ US$0.04/h + Jenkins t3.medium ≈ US$0.04/h → **~US$0.18/h**
- Lambda, API Gateway, DynamoDB on-demand and SNS stay within the free tier at this volume
- **Run `terraform destroy` on `platform` at the end of each work day** (recreating MSK takes ~25 min) → ~US$5 for the week. If you leave it running all week, it costs **~US$30**. The budget alert protects you

## Verification (checklist)
1. `mvn verify` passes locally, with all unit tests green and coverage ≥ 80%; `*IT` tests pass against Testcontainers Kafka
2. `terraform plan` is clean for both stacks; MSK is `ACTIVE`; `create-topics.sh` lists the 3 topics
3. Both event source mappings are `Enabled` with the last processing result `OK`
4. `curl -X POST .../transactions` with a normal transaction returns `APPROVE` from `GET /transactions/{id}`
5. Simulator `--scenario=travel` gives `BLOCK` and an SNS email arrives
6. A malformed message lands in `transactions.dlq`; a duplicate `transactionId` is stored only once
7. A push to `main` makes Jenkins run all stages green, waits for approval, deploys the new jar version, and passes the smoke test
8. IAM check: the Jenkins instance has no access keys; `aws iam simulate-principal-policy` shows the deployer **cannot** `iam:CreateRole`
9. Under load, the dashboard shows OffsetLag recovering to ~0
10. `terraform destroy` on both stacks; Billing shows no ongoing cost

## What goes on the resume
"Built a real-time card fraud detection platform on AWS: Java 21 Lambdas (SnapStart) consuming from Amazon MSK with IAM auth, a rules engine with idempotent DynamoDB writes and DLQ handling, an API Gateway ingest layer and SNS alerting. Infrastructure provisioned with Terraform (VPC endpoints, no NAT, least-privilege IAM split between platform and app). CI/CD with Jenkins on EC2 (JCasC, IAM role assumption, no static keys) running JUnit/Mockito/Testcontainers with an 80% JaCoCo gate, static analysis, tflint/checkov and gated Terraform deploys."
