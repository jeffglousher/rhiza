# Rhiza SQLite QWAL v3 복제 계약

> 상태: **클린 설치 전용 QWAL v3 구현 완료**
>
> 범위: QuePaxa가 결정한 SQLite 물리 효과의 준비, 배치, 적용, 복구
>
> 호환성: QWAL v2, control/QCTL generation 5, 이전 QSNP를 읽거나 마이그레이션하지 않는다.

## 1. 결정

Rhiza SQL의 내구성 원본은 SQLite 파일이 아니라 **2/3 Recorder quorum이 내구화한
`StoredCommand` 전체**다. command에는 QWAL page after-image와 request receipt가 함께
들어간다. winning proposer만 SQL을 실행하고 follower는 SQL 문장을 재실행하지 않는다.

```text
exact-base SQLite에서 SQL 실행
  -> native SQLite WAL frame 캡처
  -> QWAL v3(changed pages + receipts + base/target state root)
  -> 2/3 Recorder WAL 내구화 및 결정
  -> 로컬 SQLite 적용과 read visibility
  -> ACK
```

Recorder quorum WAL이 strict ACK의 유일한 필수 디스크 장벽이다. canonical SQLite,
control sidecar와 file qlog는 `synchronous=OFF` 또는 buffered로 유지하는 **폐기 가능한
materialized view**다. 로컬 apply는 ACK 전에 끝나지만 별도 로컬 내구성 경계는 아니다.

이 모델은 trigger, foreign-key cascade, rowid/`AUTOINCREMENT`와 허용된 비결정적 함수의
winning 결과를 exact replay한다. 패배한 제안은 canonical DB를 바꾸지 않는다.

## 2. 구현 경계

| 책임 | 구현 기준점 |
| --- | --- |
| native WAL parser와 checksum/commit 검증 | [`wal_capture.rs`](../crates/rhiza-sql/src/wal_capture.rs) |
| `StateIdentityV3` Merkle page state | [`page_state.rs`](../crates/rhiza-sql/src/page_state.rs) |
| QWAL v3 codec과 in-place page apply | [`qwal.rs`](../crates/rhiza-sql/src/qwal.rs) |
| 준비, inode seal, canonical WAL 격리, decided apply | [`SqliteStateMachine`](../crates/rhiza-sql/src/lib.rs) |
| generation 6 control, receipt, embedded qlog | [`control.rs`](../crates/rhiza-sql/src/control.rs) |
| runtime recovery와 readiness | [`NodeRuntime`](../crates/rhiza-node/src/lib.rs) |
| checkpoint publish, restore와 compaction | [`durability.rs`](../crates/rhiza-node/src/durability.rs) |
| physical replication 계약 테스트 | [`qwal_contracts.rs`](../crates/rhiza-sql/tests/qwal_contracts.rs) |

현재 포맷 이름과 framing은 다음과 같다.

- QWAL v3: `QWAL\0\x04`
- QSNP v3: `QSNP\0\x04`
- control/QCTL generation 6: `RHIZA-SQL-CONTROL\0\x06`, `QCTL\0\x06`
- canonical client command는 계속 QSQL v2다. QSQL은 입력 포맷이고 QWAL 버전과 무관하다.
- archive API의 checkpoint v2도 별도 외부 checkpoint 프로토콜이며 QWAL v2를 뜻하지 않는다.
- executor policy `rhiza-sql-qwal-batch-v3-policy-v8-compat`은 command-scoped TEMP와 bundled
  virtual table 허용 범위를 materializer fingerprint에 묶는다.

## 3. QWAL v3와 page-state identity

```text
QwalEnvelopeV3 {
  cluster_id, epoch, configuration_id, recovery_generation,
  base_index, base_hash,
  base_state:   { page_size, page_count, state_root },
  target_state: { page_size, page_count, state_root },
  materializer_fingerprint,
  receipts: [{ request_id, request_digest, result_blob }, ...],
  pages: [{ page_no, after_image }, ...]
}
```

`state_root`는 page number와 page bytes의 leaf, canonical empty subtree, internal node,
page size와 page count를 domain-separated hash로 묶는다. dense `PageStateCacheV3`는
authoritative data가 아니라 닫힌 SQLite 파일에서 언제든 재생성할 수 있는 cache다. 정상
write에서는 changed page만 overlay해 target root를 계산하고 설치 뒤 같은 patch로 cache를
갱신한다. 매 요청 전체 DB hash나 전체 page diff는 하지 않는다.

QWAL 검증은 다음을 fail closed로 강제한다.

- canonical postcard bytes와 정확한 v3 magic
- 1..=1,024개의 고유 receipt, 512 KiB 이하의 envelope/result/page image
- 엄격히 증가하는 page number와 정확한 page 크기
- grow 시 base EOF 뒤의 모든 새 page 포함
- SQLite header page size와 target page count 일치
- cached base root 일치와 changed-page overlay로 계산한 target root 일치
- cluster/epoch/configuration/recovery generation/materializer fingerprint 일치

로컬 path, WAL salt, `-shm`, staging 이름과 entry hash는 payload에 넣지 않는다.

## 4. 준비: native WAL capture

`prepare_sql_batch_effect`는 materialized tip, control의 `user_state`, pending 부재를 먼저
확인한다. lifecycle gate 아래에서 canonical connection을 닫고 Unix에서는 열린 file과
pathname이 같은 regular inode인지 `dev/ino/len/mtime/ctime` seal로 고정한다.

별도 clone, reflink, full-copy staging file은 만들지 않는다. exclusive lifecycle gate가 read pool과
canonical writer를 모두 배제한 동안 canonical main file 자체를 sealed base로 유지하고, SQLite의
native `-wal`만 speculative overlay로 사용한다.

speculative connection은 WAL mode, `synchronous=OFF`, autocheckpoint 0,
checkpoint-on-close disabled로 실행한다.
commit 뒤 connection이 살아 있을 때 `-wal` descriptor를 열고 metadata를 seal한 다음,
connection을 닫아도 보존된 descriptor에서 frame을 직접 파싱한다. parser는 WAL magic/version,
page size, salts, rolling checksums, complete frames, 하나의 최종 commit marker와 target page
count를 검증한다. capture 전후 descriptor seal이 달라지면 거부한다.

capture 성공과 모든 오류 경로에서 owned WAL/SHM을 제거하고 parent directory를 sync한 뒤 main
file의 inode seal과 전체 page-state root가 준비 전 control state와 정확히 같은지 다시 확인한다.
이 증명이 끝나기 전에는 canonical connection을 reopen하지 않는다. 시작 시 남은 sidecar도 SQLite가
열기 전에 같은 검사를 거치며, main root가 committed control state와 같을 때만 폐기한다. 다르면
replay를 추측하지 않고 fail closed한다.

현재 canonical WAL 준비는 held descriptor와 pathname의 동일 inode를 증명할 수 있는 Unix에서만
지원한다. 동등한 file-identity primitive가 없는 non-Unix target에서는 준비를 명시적으로 거부한다.

마지막 committed frame의 page after-image만 보존하고 base와 같은 image는 제거한다. 새 page는
누락 없이 포함한다. custom recording VFS, checkpoint 뒤 full-file diff, full target digest는
production 준비 경로에 없다. full-file diff와 byte digest는 테스트 oracle 또는 checkpoint
용도로만 남아 있다.

ordered batch는 member별 savepoint를 사용한다. 실패 member만 rollback하고 성공 subset의
page effect와 receipt를 하나의 decided entry에 묶는다. 전부 실패하면 effect, consensus와
qlog가 모두 없다. retry는 같은 request ID와 canonical bytes를 사용하며 ID가 같고 bytes가
다르면 conflict다. foreign winner 뒤에는 stale effect를 재사용하지 않고 새 exact base에서
unseen member만 다시 준비한다.

한 QSQL command의 `statements[]`도 같은 savepoint 안에서 순서대로 실행된다. statement는
일반 `main` DDL/DML뿐 아니라 `SELECT`/`VALUES`처럼 행을 반환하는 read도 될 수 있다. 이 행은
`RETURNING`과 같은 typed statement result로 receipt에 들어가므로 duplicate retry와 reopen 뒤에도
winning proposer가 관측한 결과를 그대로 반환한다. pure-read execute는 changed page가 없는 QWAL을
만들 수 있지만 receipt와 log anchor는 남는다. 일반 조회에는 log를 쓰지 않는 query API를 사용한다.

## 5. 적용: decided effect patch

적용 전 lifecycle/read-write gate를 닫고 entry chain, identity, receipt와 page-state transition을
모두 검증한다.

local proposal의 승패와 무관하게 consensus가 결정한 QWAL effect만 적용한다. connection을
checkpoint/close한 뒤 seal된 canonical inode에 changed page만 in-place write하고 target 길이를
맞춘다. 설치한 page bytes와 갱신된 Merkle target root를 다시 확인한다. prepared target promotion,
전체 DB temp reconstruction/rename은 없다.

물리 설치 뒤 control generation 6의 한 비내구 transaction이 applied tip, target state,
receipts와 embedded qlog entry를 같은 anchor로 게시한다. connection은 exclusive lifecycle
gate 안에서 먼저 reopen될 수 있지만, control commit이 성공하기 전에는 read와 ACK를 허용하지
않는다. commit 실패 시 다시 닫는다.

DB 설치 뒤 control 게시가 실패하면 connection을 닫고 모든 read/receipt API를 차단한다.
같은 process에서는 exact payload만 재시도할 수 있다. 중간 page write 뒤 crash 또는 재시작 시
DB/control root가 맞지 않으므로 로컬 복구를 추측하지 않고 Recorder 기반 rebuild를 요구한다.

## 6. Recorder-authoritative recovery와 GC

시작과 restore는 다음 불변조건을 지킨다.

1. SQLite와 control pair를 함께 검증하고 closed DB에 예상 밖 `-wal`/`-shm`이 있으면 거부한다.
2. control tip/state root와 실제 DB의 rebuilt `StateIdentityV3`가 정확히 같아야 한다.
3. recovery anchor가 없고 materializer가 손상됐으면 로컬 `sqlite/`를 sibling quarantine한 뒤
   genesis view를 만들고 qlog와 certified Recorder decisions를 재생한다.
4. recovery anchor가 있으면 손상된 view를 임의 genesis로 만들지 않고 verified checkpoint를
   요구한다. restore는 target node identity로 QSNP를 설치하고 checkpoint suffix를 적용한다.
5. restored materializer tip과 local qlog tip/hash는 checkpoint root와 정확히 같아야 하며,
   qlog가 그 root hash를 실제 entry 또는 compaction anchor로 포함해야 한다.
6. 그 뒤 peer candidate를 certified decision과 대조하고 Recorder tail을 적용한다. 이 과정이
   끝나기 전 runtime은 생성되지 않으므로 `/readyz`가 열리지 않는다. runtime 중 invariant
   오류는 readiness를 latch-off한다.
7. quorum이 command나 decision certificate를 증명하지 못하면 `Unavailable` 또는
   reconciliation error로 fail closed한다.

checkpoint compaction은 materialized tip을 고정하고 local committed tail을 archive에 먼저
publish한다. snapshot을 publish한 뒤 archive에서 다시 restore해 **anchor와 bytes가 동일함**을
확인한 다음에만 local file qlog와 embedded qlog prefix를 compact한다.

Recorder WAL rotation은 frame 안의 full command를 checksum이 있는 QCMD 파일로 승격한 뒤 WAL을
비운다. 현재 QCMD 개별 GC는 구현하지 않았다. 따라서 checkpoint가 충분한 복구 근거가 되기
전에 Recorder command를 지우는 위험은 없지만, Recorder disk 사용량을 제한하는 별도
checkpoint-certificate 기반 GC 프로토콜은 남은 작업이다.

## 7. SQL 호환 계약과 클린 설치

이 전환은 rolling upgrade가 아니다. 모든 voter를 같은 binary와 fingerprint로 구성한 빈 data
directory에서 시작한다. 다음 입력은 fallback 없이 거부한다.

- QWAL v2(`QWAL\0\x03`)와 더 오래된 QWAL payload
- control/QCTL generation 5와 이전 sidecar/snapshot
- 이전 QSNP와 legacy `__rhiza_meta`/`__rhiza_requests` DB
- old QEFX/QBCH history, dual writer/decoder, statement replay fallback
- 기존 DB/control의 in-place migration과 자동 downgrade

### 7.1 실질적 Hiqlite/Haqlite 호환 범위

호환 목표는 다른 제품에서 SQLite parser까지 우연히 도달하는 모든 문장이 아니라, HA 환경에서
문서화하고 재현할 수 있는 애플리케이션 SQL이다.

- Hiqlite 0.14는 일반 execute/`RETURNING`, ordered transaction, raw string batch, migration과
  local/consistent query를 공개한다. 다만 write SQL을 각 replica에서 다시 실행하므로 비결정적
  함수를 write connection에서 금지하고, `ATTACH`, TEMP, raw transaction control처럼 별도 차단이
  없는 connection-local 기능을 HA 계약으로 검증하지 않는다.
- Haqlite는 unfenced leader에서 raw `rusqlite::Connection`과 Hrana session을 노출하여 prepared
  statement와 transaction을 폭넓게 실행하고 WAL을 물리 복제한다. 그러나 기본 Continuous mode는
  WAL을 background로 전송하므로 Rhiza strict의 quorum-durable ACK와 같은 보장이 아니다.
- Rhiza는 일반 `main` SQL의 폭은 이 두 제품과 맞추되, QWAL page effect와 exact receipt가 자동
  recovery 가능한 범위만 연다. 다른 제품의 filter 부재나 node-local side effect는 호환 요구로
  간주하지 않는다.

### 7.2 지원하는 SQL

- 일반 `main` schema DDL/DML: table/index/view/trigger 생성과 제거, `ALTER TABLE`, `ANALYZE`,
  `REINDEX`, CTE, UPSERT, generated column, STRICT table, WITHOUT ROWID, partial index, foreign-key
  cascade와 `AUTOINCREMENT`를 SQLite가 허용하는 범위에서 실행한다.
- QSQL command는 1..=64개의 single statement를 `statements[]`로 받아 원자적으로 실행한다.
  statement 하나의 SQL 문자열에 여러 문장을 넣지 않는다. raw `BEGIN` 대신 이 command 경계를
  transaction으로 사용한다.
- execute 안의 `SELECT`, `VALUES`, DML `RETURNING`과 row-producing PRAGMA 결과는 모두 typed
  statement result로 receipt에 저장된다. query API는 SQLite가 read-only로 판정한 statement만
  실행하고 local 또는 ReadBarrier consistency를 선택한다.
- TEMP table/index/view/trigger는 **한 command 안에서 생성, 사용, 제거**할 때만 허용한다. command
  종료 시 `sqlite_temp_schema`에 객체가 하나라도 남으면 command 전체를 rollback한다. TEMP의
  중간 계산으로 생긴 최종 `main` page effect와 receipt만 복제한다.
- 다음 observational PRAGMA를 query와 execute에서 허용한다.
  - argument 허용: `foreign_key_check`, `foreign_key_list`, `index_info`, `index_list`,
    `index_xinfo`, `integrity_check`, `quick_check`, `table_info`, `table_list`, `table_xinfo`
  - no-argument only: `analysis_limit`, `application_id`, `auto_vacuum`, `automatic_index`,
    `busy_timeout`, `cache_size`, `cache_spill`, `case_sensitive_like`, `cell_size_check`,
    `checkpoint_fullfsync`, `collation_list`, `compile_options`, `count_changes`, `data_version`,
    `default_cache_size`, `defer_foreign_keys`, `empty_result_callbacks`, `encoding`,
    `freelist_count`, `foreign_keys`, `full_column_names`, `fullfsync`, `function_list`,
    `hard_heap_limit`, `ignore_check_constraints`, `journal_size_limit`, `legacy_alter_table`,
    `locking_mode`, `max_page_count`, `mmap_size`, `module_list`, `page_count`, `page_size`,
    `pragma_list`, `query_only`, `read_uncommitted`, `recursive_triggers`,
    `reverse_unordered_selects`, `schema_version`, `secure_delete`, `short_column_names`,
    `soft_heap_limit`, `synchronous`, `temp_store`, `threads`, `trusted_schema`, `user_version`
- replicated PRAGMA mutation은 `user_version = value`, `application_id = value`와 `PRAGMA
  optimize`만 허용한다. optimize는 argument 없는 형식과 `u32` decimal/`0x` hexadecimal mask만
  받으며, 문자열이나 범위 밖 값은 거부한다. 앞의 두 값은 `main` header page로, optimize 결과는
  일반 page effect로 exact replay한다.
- persistent virtual table module은 binary/fingerprint에 포함된 `fts3`, `fts3tokenize`, `fts4`,
  `fts4aux`, `fts5`, `fts5vocab`, `dbstat`, `rtree`, `rtree_i32` 아홉 개만 대소문자 구분 없이
  허용한다. FTS/RTree의 create/insert/update/delete/drop, DBSTAT의 create/query, follower apply와
  reopen 및 FTS `MATCH`/RTree/DBSTAT 조회를 계약 테스트한다.
- time/random 함수, trigger 간접 변경처럼 statement replication에서 위험한 결과도 winning
  proposer의 final pages와 receipt를 복제하므로 follower가 SQL을 재실행하지 않는다.

### 7.3 계속 차단하는 최소 범위

- raw `BEGIN`/`COMMIT`/`ROLLBACK`과 사용자 `SAVEPOINT`/`RELEASE`: atomicity와 rollback은
  `statements[]` command/savepoint가 소유한다.
- `ATTACH`/`DETACH`와 `main` 밖 durable database: QWAL, snapshot과 recovery가 attached file을
  권위 있는 상태로 포함하지 않는다.
- command 종료 뒤 남는 TEMP object와 요청 간 TEMP/session state.
- `VACUUM`, `VACUUM INTO`와 file/network/process side effect: 현재 outer transaction과 bounded
  QWAL 계약 밖의 maintenance/artifact다.
- 외부 또는 사용자 등록 virtual-table module, 임의 UDF/collation, loadable extension과
  `load_extension()`.
- `user_version`/`application_id` 외의 PRAGMA setter, non-`u32` optimize argument,
  journal/checkpoint/page-size/locking/`writable_schema`/`schema_version` 등 storage 또는 connection
  invariant를 바꾸는 setter. query API에서 mutation으로 판정되는 statement도 항상 거부한다.
- unknown authorizer action과 exact 내부 sentinel 접근. 예약 이름은 대소문자 무시 정확히
  `__rhiza_kv`, legacy `__rhiza_meta`, legacy `__rhiza_requests` 및 그 SQLite autoindex다.
  `__rhiza_` prefix 전체를 예약하지 않으므로 예를 들어 `__rhiza_user_table`은 허용한다.

### 7.4 자원 경계

- command당 statement 1..=64개, statement당 SQL 1..=64 KiB와 parameter 최대 999개,
  request ID 최대 256 bytes, canonical encoded command 최대 512 KiB
- command 전체 typed result는 최대 1,024행/1 MiB지만, receipt와 page를 포함한 QWAL envelope
  자체는 최대 512 KiB이므로 더 작은 physical 한계가 먼저 적용될 수 있음
- inline physical group에는 receipt 1..=1,024개, changed page/QWAL envelope 최대 512 KiB.
  batch receipt encoding은 각 member savepoint commit 전에 누적 512 KiB를 검사하며, 전부 실패한
  external batch는 effect 없이 정렬된 member error만 반환한다.
- native WAL parser의 raw capture work는 최대 8 MiB다. 같은 1~2개 page를 반복 overwrite한
  frame은 이 별도 work budget 안에서 허용하지만, 최종 고유 page image와 encoded QWAL은 계속
  512 KiB 이하이어야 한다. 따라서 이 변경은 큰 `CREATE INDEX`나 bulk `UPDATE` 지원이 아니다.
- 외부 effect producer는 같은 canonical WAL sandbox를 사용하되 최종 typed SQL page effect와
  receipt prefix를 합쳐 64 MiB, raw WAL capture work를 256 MiB로 제한한다. receipt aggregate는
  계속 512 KiB 이하이며 bundle은 canonical 256 KiB chunk로 나뉜다. producer는 config digest,
  intended slot, prev hash가 정확히 주어진 뒤에만 QEFX command를 만든다.
- 외부 apply는 QEFX chunk/digest/profile manifest와 canonical page-effect encoding을 모두 검증해
  만든 `VerifiedQwalEffectBundleV4`만 받는다. raw external bytes나 inline QWAL fallback을 apply
  경계에 전달하지 않는다. Node/Recorder transport 연결은 별도 integration 단계다.
- query는 기본 1,000행, 요청 최대 10,000행, 결과 최대 1 MiB, 기본 실행 제한 5초
- SQL value는 NULL, signed 64-bit integer, finite real, UTF-8 text와 blob으로 제한

## 8. strict 보장과 한계

- ACK된 command의 전체 QWAL과 receipt는 3-peer 중 2 Recorder에 먼저 내구화된다.
- 한 voter의 영구 손실까지 quorum progress와 RPO=0를 유지할 수 있다. 두 voter의 durable
  data를 동시에 잃으면 외부 durable checkpoint 없이는 RPO=0를 증명할 수 없다.
- leaderless consensus는 failover와 병렬 proposal에 유리하지만 단건 latency의 하한인 network
  RTT와 quorum flush를 제거하지 않는다. 처리량은 concurrent outstanding request와 logical
  batch가 group commit을 채울 때 올라간다.
- SQLite/control/qlog만 가진 한 node의 peer 없는 독립 재개는 strict 계약이 아니다.
- inline QWAL은 512 KiB 상한이다. typed QEFX SQL foundation은 64 MiB까지 producer/verify/apply를
  제공하지만 durable Recorder transport와 consensus 연결 전에는 Node request path에서 사용하지 않는다.
- canonical main inode seal proof는 Unix에서 활성화되며, sidecar와 page-state 검증은 모든
  플랫폼에서 fail closed한다.

## 9. 구현 완료와 남은 검증

구현 및 자동화 테스트가 확인하는 항목:

- native WAL checksum/commit parsing과 changed-page reproduction
- Merkle base/target transition, grow/shrink와 corrupt page/root 거부
- canonical WAL prepare의 main-file 불변성, decided in-place patch, inode/symlink race 거부
- partial apply 및 post-control failure의 read 차단과 exact retry
- QSNP restore, checkpoint-root equality, tail catch-up와 readiness gate
- QWAL v2 및 control generation 5 rejection
- 일반 main DDL/DML, pure-read execute receipt, command-scoped TEMP, observational/header PRAGMA와
  bundled FTS/RTree/DBSTAT의 proposer/follower/reopen exact replay
- HTTP atomic FTS5 write, duplicate receipt retry와 ReadBarrier `MATCH` query

아직 release gate로 남은 항목:

- clean Linux NVMe에서 Recorder `fdatasync`, native capture와 follower apply를 분리한 반복 benchmark
- 3-peer Kubernetes에서 emptyDir/no-PVC failure 및 checkpoint+Recorder recovery 재측정
- 손상 종류와 1/2/3 peer loss를 포함한 전체 HTTP rejoin/recovery E2E
- 큰 DB startup은 입력을 page 단위로 읽지만 모든 page hash를 다시 계산한다. 측정상 필요할 때만
  authenticated persisted cache 등으로 O(DB size) startup hashing을 줄인다.
- checkpoint certificate에 묶인 bounded Recorder QCMD GC

벤치마크는 durability mode, voter 수, concurrency, logical batch 크기, payload, host/filesystem을
함께 기록한다. 검증되지 않은 수치를 이 계약의 근거로 사용하지 않는다.

## 10. 공식 SQLite 자료

- [Write-Ahead Logging](https://www.sqlite.org/wal.html)
- [WAL-mode File Format](https://www.sqlite.org/walformat.html)
- [Database File Format](https://www.sqlite.org/fileformat.html)
- [Atomic Commit](https://www.sqlite.org/atomiccommit.html)
- [How To Corrupt An SQLite Database File](https://www.sqlite.org/howtocorrupt.html)
