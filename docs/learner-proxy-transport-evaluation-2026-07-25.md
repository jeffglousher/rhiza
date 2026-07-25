# Learner/proxy transport evaluation

Date: 2026-07-25
Status: architecture decision; implementation deferred
Scope: post-commit read plane only

## Decision

Rhiza의 learner/proxy는 합의 membership에 추가하지 않고, **비권위적
post-commit plane**으로 분리한다. 이번 단계에서는 ZeroMQ도 learner runtime도
구현하지 않는다. 먼저 아래 안전 계약, 전환 절차, 벤치마크 게이트를 설계
기준으로 고정한다.

- MVP learner는 checkpoint V2를 복구하고, bounded `LogPeer`로 로그를
  tail하며, 현재 configuration의 서로 다른 voter quorum에서 각 entry를
  독립 확인한 뒤 순서대로 적용한다.
- learner는 확인된 연속 prefix와 명시적 watermark만으로 local read를
  제공한다. propose, write, admin, checkpoint publish는 절대 수행하지 않는다.
- proxy는 별도 인증·인가를 거친 read-only 기능이며 기본값은 disabled다.
  proxy 또는 relay의 단일 주장은 권위가 없고 downstream은 voter identity와
  현재 configuration을 검증한다.
- 운영 기본 codec/transport는 plaintext `tcp-rkyv`다. `http`와
  `tcp-postcard`는 배타적인 rollback/diagnostic 모드로만 남는다. TLS,
  fallback, negotiation, mixed mode는 없다. learner/ZeroMQ 실험은 이 기본
  전환과 같은 변경 묶음으로 평가하거나 배포하지 않는다.

## 현재 제약과 신뢰 경계

Rhiza membership은 정확히 3~7개의 voter로만 구성되고 learner role이 없다.
현재 relay와 proposer도 configured voter여야 한다. checkpoint V2의 identity
검증, bounded `LogPeer`, hash chain 및 configuration 검증은 재사용할 수 있다.

그러나 현재 `LogEntry` 자체에는 제3자가 독립 검증할 수 있는 quorum proof가
없다. 따라서 단일 voter, relay, learner 또는 proxy가 전달한 entry를
“committed”로 간주하면 안 된다. MVP의 commit evidence는 현재
configuration digest에 속한 `q = floor(n/2) + 1`개의 **서로 다른 voter
관찰값이 동일한 `(index, previous_hash, entry_hash, entry)`를 확인했다는
사실**이다. 연결 수준의 source identity도 이 voter identity에 묶여야 한다.
portable voter attestation이 없는 wire에서는 downstream이 voter 관찰을
직접 재확인해야 하며, proxy가 합성한 인증서를 quorum proof라고 부르지 않는다.

## MVP learner 계약

1. checkpoint V2의 cluster/profile/epoch/configuration/recovery identity,
   manifest, snapshot hash와 materializer fingerprint를 검증해 fresh local
   state로 복구한다.
2. checkpoint tip 다음 index부터 현재 configuration의 voter들을 병렬 조회한다.
   응답 크기, in-flight 수, deadline과 재시도 횟수는 모두 bounded다.
3. distinct voter quorum이 동일한 entry를 관찰한 경우에만 hash chain,
   configuration transition, command/domain 제약을 검증하고 다음 index를
   적용한다. minority의 충돌·지연 응답은 채택하지 않는다.
4. 적용이 끝난 연속 prefix만
   `W = (cluster, epoch, config_id, index, entry_hash)` watermark로 공개한다.
   local read 응답은 사용한 `W`와 observed-at을 포함하며, requested minimum
   watermark를 만족하지 못하면 stale data 대신 retryable unavailable을
   반환한다.
5. gap, quorum 상실, 같은 index의 충돌, chain/config 불일치가 발생하면
   watermark를 전진시키지 않고 fail closed한다. 다른 voter의 bounded replay,
   필요하면 검증된 checkpoint에서의 clean rebuild가 유일한 복구 경로다.

learner의 local state와 watermark는 cache다. voter quorum과 checkpoint만
복구 권위이며 learner를 잃어도 합의, write availability, checkpoint/GC에
영향이 없어야 한다.

## Read-only proxy 계약

- 별도 endpoint, credential, audience와 read scope를 사용하고 기본적으로
  listen하지 않는다. voter/admin credential을 재사용하지 않는다.
- 허용 operation은 health, watermark, bounded read, bounded replay뿐이다.
  write/propose, membership, stop/replace, checkpoint publish/GC와 임의
  forwarding은 protocol 수준에서 거부한다.
- 모든 응답은 source cluster/configuration과 learner watermark를 포함한다.
  downstream은 configured voter 집합, distinct identity, chain/config 및
  요구 watermark를 검증한다. proxy identity만으로 freshness나 commit을
  승인하지 않는다.
- 인증 성공은 membership admission이 아니다. 특히 ZeroMQ ZAP은
  인증 protocol일 뿐 Rhiza voter admission이나 authorization을 대신하지
  않는다.

## Cutover protocol

1. **Freeze baseline:** plaintext `tcp-rkyv` 운영 경로, membership/config digest,
   checkpoint tip과 commit/read SLO를 기록한다. learner/proxy flag는 off다.
2. **Shadow learner:** 기존 checkpoint V2와 bounded `LogPeer`만 사용해
   외부 트래픽 없이 복구·tail한다. voter 노드의 applied prefix와 state hash를
   지속 비교하고 divergence 시 중단한다.
3. **Read canary:** 별도 권한을 가진 한 canary client에만 proxy read를
   허용한다. 기존 read와 결과·watermark를 동시 비교하며 write/admin route는
   존재하지 않아야 한다.
4. **Bounded expansion:** 1% → 10% → 50% → 100%의 read-only client를
   단계적으로 이동한다. 각 단계는 아래 safety/isolation gate를 한 관찰
   window 이상 통과해야 한다. client는 minimum watermark 미충족 시 기존
   authoritative read로 fallback한다.
5. **Rollback:** proxy flag를 끄고 learner cache를 폐기한다. voter,
   membership, qlog, checkpoint와 write path에는 되돌릴 변경이 없어야 한다.

향후 ZeroMQ 실험도 별도 cutover로 수행한다. 먼저 기존 경로와 나란히
post-commit mirror만 shadow 수신하고, 모든 message를 `(origin, config_id,
index, entry_hash)`로 대조한다. 다음에 ROUTER/DEALER를 registration과
bounded replay에만 canary 적용한다. XSUB→XPUB는 fanout 벤치마크 게이트를
통과한 경우에만 실험한다. 어느 단계에서도 ZeroMQ가 유일한 replay 원천이
되어서는 안 되며, gap 탐지 후 voter replay fallback은 필수다.

## Safety and operational invariants

### Security

- frame 크기, multipart 수, collection 길이, replay 범위, client별 rate와
  deadline을 제한하고 malformed/foreign identity는 적용 전에 거부한다.
- transport authentication, proxy authorization, Rhiza membership 검증은
  서로 다른 검사다. 어느 하나의 성공도 나머지를 암묵 승인하지 않는다.
- configuration transition 경계에서 구·신 voter 응답을 한 quorum으로
  혼합하지 않는다. credential rotation은 config activation과 fail-closed로
  맞춘다.

### Backpressure

- consensus/apply thread는 learner나 subscriber를 기다리지 않는다.
  mirror enqueue는 post-commit이고 bounded이며 가득 차면 명시적 drop/gap
  metric을 남긴다.
- client별 queue와 global byte budget을 두고 느린 client를 격리·연결
  종료한다. watermark는 enqueue가 아니라 검증·적용 완료 후에만 전진한다.
- PUB 계열은 high-water mark에서 message를 drop할 수 있으므로
  sequence/index gap detection과 replay가 없는 PUB/SUB 경로는 금지한다.

### Relay loop

- message identity는 `(cluster, epoch, config_id, index, entry_hash)`이며
  duplicate는 idempotently 제거한다.
- origin voter identity, ingress relay identity와 hop count를 보존한다.
  MVP는 최대 한 relay hop만 허용하고 origin으로 재유입되는 message를
  거부한다.
- relay output은 consensus/proposer/admin ingress에 연결할 수 없다.
  relay가 새 voter 관찰을 만들거나 같은 voter를 여러 표로 계산해서는 안 된다.

## Test contracts

| 계층 | 보호할 관찰 가능 동작 |
|---|---|
| Prefix/quorum | 3·5·7 voter에서 distinct quorum만 entry를 승인하고 duplicate identity, minority fork, mixed configuration, wrong previous hash를 거부한다. |
| Watermark | restart·duplicate·out-of-order 입력에도 단조 증가하며 gap/conflict에서는 멈추고 minimum watermark 미충족 read는 실패한다. |
| Recovery | checkpoint V2 + exact suffix가 voter와 동일 state/hash를 만들며 corrupt/foreign checkpoint와 truncated suffix는 fail closed한다. |
| Fault integration | voter loss, delayed/minority equivocation, quorum loss, config transition 중에도 미확인 entry를 읽지 않고 quorum 회복 후 prefix만 재개한다. |
| Capability denial | proxy의 propose/write/admin/checkpoint/GC 요청은 인증 여부와 무관하게 거부되고 voter state가 변하지 않는다. |
| Backpressure | 느린·중단 client가 consensus latency를 막지 않으며 모든 injected drop/gap을 탐지해 bounded replay 또는 rebuild로 복구한다. |
| Relay | duplicate와 loop가 한 번만 적용되고 hop 초과·origin 재유입·identity 위조가 거부된다. |
| Cutover | 각 traffic 단계의 결과와 watermark가 baseline과 일치하고 flag-off만으로 기존 read path로 즉시 복귀한다. |

parser, chain/config validator와 watermark state machine에는 example test와
함께 property test를 둔다. 생성된 valid prefix는 항상 수용되고, 임의
재정렬·삭제·중복·단일-bit 변조는 watermark를 잘못 전진시키지 않아야 한다.

## Staged benchmark matrix and gates

공통 matrix는 voter `3/5/7`, entry `128 B/4 KiB/64 KiB`, reader
`1/8/64`, RTT `0.2/5/25 ms`, steady/catch-up, 정상/1% gap/10% burst gap,
fast/slow/disconnected client를 교차한다. 동일 host pinning, persistence,
security, workload seed와 순서 회전으로 다음 후보를 비교한다.

| 단계 | 후보 | 필수 측정 |
|---|---|---|
| B0 | current bounded `LogPeer` + Postcard baseline | voter commit throughput/p99, learner apply rate/lag, catch-up time, CPU/RSS, bytes |
| B1 | shadow learner/proxy, transport 변경 없음 | B0 대비 consensus 영향, quorum-check 비용, read p99, divergence |
| B2 | ROUTER/DEALER registration + replay experiment | replay throughput/p99, reconnect recovery, fairness, queue/RSS, gap repair |
| B3 | XSUB→XPUB fanout experiment | verified deliveries/core, publisher CPU/egress, slow-subscriber drop와 repair |
| B4 | failure soak | restart, partition, config cutover, credential rotation, 24 h memory/queue bound |

Go/no-go gate는 누적 적용한다.

1. **Safety:** undetected gap, wrong-prefix read, unauthorized capability,
   relay loop와 state divergence가 모두 0이어야 한다.
2. **Isolation:** 어느 matrix cell에서도 voter commit throughput 저하가
   2%를 넘거나 p99 증가가 5%를 넘으면 no-go다.
3. **Recovery:** injected gap 100%가 탐지되고 bounded replay 또는 clean
   rebuild로 회복되어야 하며, slow client에서도 정한 byte/RSS bound를
   초과하거나 consensus thread가 block되면 no-go다.
4. **ROUTER/DEALER:** baseline보다 catch-up p99가 나빠지지 않으면서 replay
   throughput이 20% 이상 높거나 replay CPU가 20% 이상 낮을 때만 다음
   실험으로 승격한다.
5. **XSUB→XPUB:** 64-reader fanout에서 verified deliveries/core가 25% 이상
   높거나 publisher CPU/egress가 25% 이상 낮고, 동일 gap-repair 결과를
   보일 때만 채택 후보가 된다. 이득이 없으면 기존 transport를 유지한다.

rkyv 기본 전환은 이 matrix와 분리한다. B0의 현재 transport baseline은
plaintext `tcp-rkyv`이며, learner/ZeroMQ 변경은 별도의 안전·격리·성능
게이트를 독립적으로 통과해야 한다.

## ZeroMQ 판단 근거

ZeroMQ socket은 비동기 message queue와 socket type별 routing/queue
semantics를 제공하지만, 그것이 Rhiza admission이나 membership을 정의하지는
않는다. PUB/XPUB는 mute/high-water 상태에서 drop할 수 있고,
ROUTER/DEALER는 routing과 비동기 request/reply의 도구이므로 애플리케이션이
identity, replay, ordering과 authorization을 별도로 구현해야 한다.

- [libzmq socket types and semantics](https://zeromq.github.io/libzmq/zmq_socket.html)
- [libzmq socket options, including HWM and identity controls](https://zeromq.github.io/libzmq/zmq_setsockopt.html)
- [libzmq built-in proxy](https://libzmq.readthedocs.io/en/latest/zmq_proxy.html)
- [ZeroMQ Guide: advanced pub-sub reliability patterns](https://zguide.zeromq.org/docs/chapter5/)
- [ZeroMQ RFC 27: ZAP authentication protocol](https://rfc.zeromq.org/spec/27/)

결론적으로 ZeroMQ는 admission 문제의 해답이 아니다. ROUTER/DEALER와
XSUB→XPUB는 위 benchmark가 실제 이득을 증명한 뒤의 제한적 transport
실험일 뿐이며, 이번 단계의 산출물은 이 결정 문서까지다.
