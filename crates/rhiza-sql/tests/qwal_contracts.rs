use rhiza_core::{
    ConfigurationState, EntryType, LogAnchor, LogEntry, LogHash, Snapshot, StopBinding,
};
use rhiza_sql::{
    decode_qwal_v3, encode_put_request, encode_qwal_v3, encode_sql_command,
    restore_recovery_snapshot_file, restore_snapshot_file, ControlStore, Error, SqlBatchMember,
    SqlCommand, SqlCommandResult, SqlStatement, SqlValue, SqliteStateMachine, MAX_SQL_EFFECT_BYTES,
    QWAL_V3_MAGIC,
};

fn entry(index: u64, prev_hash: LogHash, payload: &[u8]) -> LogEntry {
    LogEntry {
        cluster_id: "cluster-a".into(),
        epoch: 1,
        config_id: 1,
        index,
        entry_type: EntryType::Command,
        payload: payload.to_vec(),
        prev_hash,
        hash: LogEntry::calculate_hash(
            "cluster-a",
            index,
            1,
            1,
            EntryType::Command,
            prev_hash,
            payload,
        ),
    }
}

fn noop(index: u64, prev_hash: LogHash) -> LogEntry {
    LogEntry {
        cluster_id: "cluster-a".into(),
        epoch: 1,
        config_id: 1,
        index,
        entry_type: EntryType::Noop,
        payload: Vec::new(),
        prev_hash,
        hash: LogEntry::calculate_hash("cluster-a", index, 1, 1, EntryType::Noop, prev_hash, &[]),
    }
}

fn prepared_qwal(
    db: &SqliteStateMachine,
    command: &SqlCommand,
    base_index: u64,
    base_hash: LogHash,
) -> (Vec<u8>, Vec<u8>) {
    let request = encode_sql_command(command).unwrap();
    let preparation = db
        .prepare_sql_batch_effect(
            &[SqlBatchMember {
                command,
                request_payload: &request,
            }],
            base_index,
            base_hash,
        )
        .unwrap();
    preparation.results.into_iter().next().unwrap().unwrap();
    let effect = preparation.effect.unwrap();
    assert!(effect.starts_with(QWAL_V3_MAGIC));
    (request, effect)
}

fn query(db: &SqliteStateMachine, sql: &str) -> Vec<Vec<SqlValue>> {
    db.query_sql(
        &SqlStatement {
            sql: sql.into(),
            parameters: vec![],
        },
        100,
        64 * 1024,
    )
    .unwrap()
    .rows
}

fn command(request_id: &str, statements: &[&str]) -> SqlCommand {
    SqlCommand {
        request_id: request_id.into(),
        statements: statements
            .iter()
            .map(|sql| SqlStatement {
                sql: (*sql).into(),
                parameters: vec![],
            })
            .collect(),
    }
}

fn open_pair(dir: &tempfile::TempDir) -> (SqliteStateMachine, SqliteStateMachine) {
    (
        SqliteStateMachine::open(
            dir.path().join("proposer.sqlite"),
            "cluster-a",
            "node-1",
            1,
            1,
        )
        .unwrap(),
        SqliteStateMachine::open(
            dir.path().join("follower.sqlite"),
            "cluster-a",
            "node-2",
            1,
            1,
        )
        .unwrap(),
    )
}

fn apply_replicated(
    proposer: &SqliteStateMachine,
    follower: &SqliteStateMachine,
    command: &SqlCommand,
    base_index: u64,
    base_hash: LogHash,
) -> (Vec<u8>, LogEntry, SqlCommandResult) {
    let (request, effect) = prepared_qwal(proposer, command, base_index, base_hash);
    let decided = entry(base_index + 1, base_hash, &effect);
    let proposer_result = proposer
        .apply_entry_with_result(&decided)
        .unwrap()
        .sql_result()
        .cloned()
        .unwrap();
    let follower_result = follower
        .apply_entry_with_result(&decided)
        .unwrap()
        .sql_result()
        .cloned()
        .unwrap();
    assert_eq!(follower_result, proposer_result);
    assert_eq!(
        follower.canonical_db_digest().unwrap(),
        proposer.canonical_db_digest().unwrap()
    );
    (request, decided, proposer_result)
}

#[test]
fn batch_preparation_commits_successes_once_and_isolates_failed_members() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
        .unwrap();
    let commands = [
        SqlCommand {
            request_id: "batch-create".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE batch_items(value TEXT NOT NULL)".into(),
                parameters: vec![],
            }],
        },
        SqlCommand {
            request_id: "batch-fail".into(),
            statements: vec![
                SqlStatement {
                    sql: "CREATE TEMP TABLE reusable(value TEXT)".into(),
                    parameters: vec![],
                },
                SqlStatement {
                    sql: "INSERT INTO batch_items VALUES ('must-rollback')".into(),
                    parameters: vec![],
                },
            ],
        },
        SqlCommand {
            request_id: "batch-insert".into(),
            statements: [
                "CREATE TEMP TABLE reusable(value TEXT)",
                "INSERT INTO reusable VALUES ('kept')",
                "INSERT INTO batch_items SELECT value FROM reusable RETURNING value",
                "DROP TABLE temp.reusable",
            ]
            .into_iter()
            .map(|sql| SqlStatement {
                sql: sql.into(),
                parameters: vec![],
            })
            .collect(),
        },
    ];
    let payloads = commands
        .iter()
        .map(|command| encode_sql_command(command).unwrap())
        .collect::<Vec<_>>();
    let members = commands
        .iter()
        .zip(&payloads)
        .map(|(command, request_payload)| SqlBatchMember {
            command,
            request_payload,
        })
        .collect::<Vec<_>>();

    let preparation = db
        .prepare_sql_batch_effect(&members, 0, LogHash::ZERO)
        .unwrap();
    assert_eq!(preparation.results.len(), 3);
    assert!(preparation.results[0].is_ok());
    assert!(preparation.results[1].is_err());
    assert!(preparation.results[2].is_ok());
    let payload = preparation.effect.unwrap();
    let effect = decode_qwal_v3(&payload).unwrap();
    assert_eq!(
        effect
            .receipts
            .iter()
            .map(|receipt| receipt.request_id.as_str())
            .collect::<Vec<_>>(),
        ["batch-create", "batch-insert"]
    );

    let entry = entry(1, LogHash::ZERO, &payload);
    db.apply_entry(&entry).unwrap();
    assert_eq!(
        query(&db, "SELECT value FROM batch_items"),
        [vec![SqlValue::Text("kept".into())]]
    );
    for (command, request) in [(&commands[0], &payloads[0]), (&commands[2], &payloads[2])] {
        let (outcome, result) = db
            .check_sql_request(&command.request_id, request)
            .unwrap()
            .unwrap();
        assert_eq!(outcome.original_log_index(), 1);
        assert_eq!(outcome.original_log_hash(), entry.hash);
        assert!(result.is_some());
    }
    assert!(db
        .check_sql_request(&commands[1].request_id, &payloads[1])
        .unwrap()
        .is_none());
}

#[test]
fn one_thousand_twenty_four_successes_share_one_entry_and_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite");
    let db = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
    let setup = SqlCommand {
        request_id: "batch-1024-setup".into(),
        statements: vec![SqlStatement {
            sql: "CREATE TABLE batch_1024(value INTEGER NOT NULL)".into(),
            parameters: vec![],
        }],
    };
    let (_, setup_effect) = prepared_qwal(&db, &setup, 0, LogHash::ZERO);
    let setup_entry = entry(1, LogHash::ZERO, &setup_effect);
    db.apply_entry(&setup_entry).unwrap();
    let commands = (0usize..1024)
        .map(|index| SqlCommand {
            request_id: format!("batch-1024-{index:04}"),
            statements: vec![SqlStatement {
                sql: "INSERT INTO batch_1024(value) VALUES (?1)".into(),
                parameters: vec![SqlValue::Integer(index as i64)],
            }],
        })
        .collect::<Vec<_>>();
    let requests = commands
        .iter()
        .map(|command| encode_sql_command(command).unwrap())
        .collect::<Vec<_>>();
    let members = commands
        .iter()
        .zip(&requests)
        .map(|(command, request_payload)| SqlBatchMember {
            command,
            request_payload,
        })
        .collect::<Vec<_>>();

    let preparation = db
        .prepare_sql_batch_effect(&members, 1, setup_entry.hash)
        .unwrap();
    assert_eq!(preparation.results.len(), 1024);
    assert!(preparation.results.iter().all(Result::is_ok));
    let payload = preparation.effect.unwrap();
    assert!(payload.len() <= MAX_SQL_EFFECT_BYTES);
    assert_eq!(decode_qwal_v3(&payload).unwrap().receipts.len(), 1024);
    let decided = entry(2, setup_entry.hash, &payload);
    db.apply_entry(&decided).unwrap();
    drop(db);

    let reopened = SqliteStateMachine::open_existing(&path).unwrap();
    for index in [0, 1023] {
        let (outcome, _) = reopened
            .check_sql_request(&commands[index].request_id, &requests[index])
            .unwrap()
            .unwrap();
        assert_eq!(outcome.original_log_index(), 2);
        assert_eq!(outcome.original_log_hash(), decided.hash);
    }
    assert_eq!(
        query(&reopened, "SELECT count(*) FROM batch_1024"),
        [vec![SqlValue::Integer(1024)]]
    );
}

#[test]
fn all_failed_batch_produces_no_effect_and_leaves_the_database_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
        .unwrap();
    let commands = ["missing_a", "missing_b"].map(|table| SqlCommand {
        request_id: format!("fail-{table}"),
        statements: vec![SqlStatement {
            sql: format!("INSERT INTO {table} VALUES (1)"),
            parameters: vec![],
        }],
    });
    let payloads = commands
        .iter()
        .map(|command| encode_sql_command(command).unwrap())
        .collect::<Vec<_>>();
    let members = commands
        .iter()
        .zip(&payloads)
        .map(|(command, request_payload)| SqlBatchMember {
            command,
            request_payload,
        })
        .collect::<Vec<_>>();
    let before = db.canonical_db_digest().unwrap();

    let preparation = db
        .prepare_sql_batch_effect(&members, 0, LogHash::ZERO)
        .unwrap();

    assert!(preparation.effect.is_none());
    assert!(preparation.results.iter().all(Result::is_err));
    assert_eq!(db.canonical_db_digest().unwrap(), before);
    assert_eq!(db.applied_tip_value().unwrap(), (0, LogHash::ZERO));
}

#[test]
fn batch_preparation_rejects_1025_members_before_mutation() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
        .unwrap();
    let command = SqlCommand {
        request_id: "same".into(),
        statements: vec![SqlStatement {
            sql: "CREATE TABLE never_applied(value INTEGER)".into(),
            parameters: vec![],
        }],
    };
    let payload = encode_sql_command(&command).unwrap();
    let members = (0..1025)
        .map(|_| SqlBatchMember {
            command: &command,
            request_payload: &payload,
        })
        .collect::<Vec<_>>();

    assert!(matches!(
        db.prepare_sql_batch_effect(&members, 0, LogHash::ZERO),
        Err(Error::InvalidCommand(_))
    ));
    assert_eq!(db.applied_tip_value().unwrap(), (0, LogHash::ZERO));
}

#[test]
fn put_effect_rejects_a_request_payload_that_does_not_match_its_arguments() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
        .unwrap();

    assert!(matches!(
        db.prepare_put_effect(
            "request-1",
            "key-1",
            "value-1",
            b"put\trequest-1\tkey-1\tdifferent",
            0,
            LogHash::ZERO,
        ),
        Err(Error::InvalidCommand(message)) if message.contains("canonical")
    ));
    assert_eq!(db.applied_tip_value().unwrap(), (0, LogHash::ZERO));
}

#[test]
fn put_request_encoder_rejects_ambiguous_or_unidentified_requests() {
    assert_eq!(
        encode_put_request("request-1", "key-1", "").unwrap(),
        b"put\trequest-1\tkey-1\t"
    );
    assert!(encode_put_request("", "key-1", "value-1").is_err());
    assert!(encode_put_request("request-1", "", "value-1").is_err());
    assert!(encode_put_request("request\t1", "key-1", "value-1").is_err());
    assert!(encode_put_request(&"x".repeat(257), "key-1", "value-1").is_err());
}

#[test]
fn existing_qwal_pair_opens_when_supplied_configuration_is_ahead() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite");
    let initial = ConfigurationState::active(1, LogHash::digest(&[b"initial"]));
    let db = SqliteStateMachine::open_with_configuration(
        &path,
        "cluster-a",
        "node-1",
        1,
        initial.clone(),
    )
    .unwrap();
    drop(db);

    let reopened = SqliteStateMachine::open_with_configuration(
        &path,
        "cluster-a",
        "node-1",
        1,
        ConfigurationState::stopped(
            1,
            initial.digest(),
            LogAnchor::new(1, LogHash::digest(&[b"stop"])),
            StopBinding::Unbound,
        ),
    )
    .unwrap();

    assert_eq!(reopened.configuration_state_value().unwrap(), initial);
}

#[test]
fn qwal_effect_reproduces_complete_sqlite_behavior_from_an_exact_base() {
    let dir = tempfile::tempdir().unwrap();
    let proposer = SqliteStateMachine::open(
        dir.path().join("proposer.sqlite"),
        "cluster-a",
        "node-1",
        1,
        1,
    )
    .unwrap();
    let follower = SqliteStateMachine::open(
        dir.path().join("follower.sqlite"),
        "cluster-a",
        "node-2",
        1,
        1,
    )
    .unwrap();

    let base_digest = proposer.canonical_db_digest().unwrap();
    assert_eq!(follower.canonical_db_digest().unwrap(), base_digest);

    let command = SqlCommand {
        request_id: "whole-engine-effect".into(),
        statements: vec![
            SqlStatement {
                sql: "CREATE TABLE parents(\
                          id INTEGER PRIMARY KEY AUTOINCREMENT,\
                          nonce BLOB NOT NULL,\
                          created_at TEXT NOT NULL\
                      )"
                .into(),
                parameters: vec![],
            },
            SqlStatement {
                sql: "CREATE TABLE children(\
                          id INTEGER PRIMARY KEY,\
                          parent_id INTEGER NOT NULL \
                              REFERENCES parents(id) ON DELETE CASCADE\
                      )"
                .into(),
                parameters: vec![],
            },
            SqlStatement {
                sql: "CREATE TABLE audit(\
                          parent_id INTEGER NOT NULL,\
                          nonce_hex TEXT NOT NULL,\
                          created_at TEXT NOT NULL\
                      )"
                .into(),
                parameters: vec![],
            },
            SqlStatement {
                sql: "CREATE TRIGGER parents_audit AFTER INSERT ON parents BEGIN \
                          INSERT INTO audit(parent_id, nonce_hex, created_at)\
                          VALUES (NEW.id, hex(NEW.nonce), NEW.created_at);\
                      END"
                .into(),
                parameters: vec![],
            },
            SqlStatement {
                sql: "INSERT INTO parents(nonce, created_at)\
                      VALUES (randomblob(16), strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))\
                      RETURNING id, hex(nonce), created_at"
                    .into(),
                parameters: vec![],
            },
            SqlStatement {
                sql: "INSERT INTO children(id, parent_id) VALUES (10, last_insert_rowid())".into(),
                parameters: vec![],
            },
            SqlStatement {
                sql: "DELETE FROM parents WHERE id = 1 RETURNING id".into(),
                parameters: vec![],
            },
        ],
    };

    let (_request, effect) = prepared_qwal(&proposer, &command, 0, LogHash::ZERO);
    assert_eq!(
        proposer.canonical_db_digest().unwrap(),
        base_digest,
        "speculative execution must not change the proposer canonical database"
    );

    let decided = entry(1, LogHash::ZERO, &effect);
    let proposer_result = proposer
        .apply_entry_with_result(&decided)
        .unwrap()
        .sql_result()
        .cloned()
        .unwrap();
    let follower_result = follower
        .apply_entry_with_result(&decided)
        .unwrap()
        .sql_result()
        .cloned()
        .unwrap();

    assert_eq!(follower_result, proposer_result);
    assert_eq!(
        follower.canonical_db_digest().unwrap(),
        proposer.canonical_db_digest().unwrap(),
        "applying the page effect must reproduce the winning staging bytes"
    );
    let inserted = &proposer_result.statement_results[4]
        .returning
        .as_ref()
        .unwrap()
        .rows[0];
    assert_eq!(inserted[0], SqlValue::Integer(1));
    assert!(matches!(&inserted[1], SqlValue::Text(value) if value.len() == 32));
    assert!(matches!(&inserted[2], SqlValue::Text(value) if !value.is_empty()));
    assert_eq!(
        query(&proposer, "SELECT count(*) FROM children"),
        [[SqlValue::Integer(0)]]
    );
    assert_eq!(
        query(&follower, "SELECT count(*) FROM children"),
        [[SqlValue::Integer(0)]]
    );
    assert_eq!(
        query(
            &proposer,
            "SELECT seq FROM sqlite_sequence WHERE name='parents'"
        ),
        [[SqlValue::Integer(1)]]
    );
    assert_eq!(
        query(
            &proposer,
            "SELECT parent_id, nonce_hex, created_at FROM audit"
        ),
        vec![inserted.clone()]
    );
    assert_eq!(
        query(
            &follower,
            "SELECT parent_id, nonce_hex, created_at FROM audit"
        ),
        vec![inserted.clone()]
    );
}

#[test]
fn replicated_read_statements_bind_exact_rows_into_the_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let (proposer, follower) = open_pair(&dir);
    let mixed = command(
        "mixed-read-write",
        &[
            "CREATE TABLE mixed(value INTEGER NOT NULL)",
            "CREATE TEMP TABLE scratch(value INTEGER NOT NULL)",
            "INSERT INTO scratch VALUES (2), (3)",
            "INSERT INTO mixed SELECT value FROM scratch",
            "DROP TABLE temp.scratch",
            "SELECT count(*) AS count, sum(value) AS total FROM mixed",
            "VALUES ('receipt-row'), (hex(randomblob(8)))",
            "UPDATE mixed SET value = value * 10 RETURNING value",
        ],
    );
    let (request, decided, result) =
        apply_replicated(&proposer, &follower, &mixed, 0, LogHash::ZERO);
    assert_eq!(
        result.statement_results[5].returning.as_ref().unwrap().rows,
        [[SqlValue::Integer(2), SqlValue::Integer(5)]]
    );
    let values = &result.statement_results[6].returning.as_ref().unwrap().rows;
    assert_eq!(values[0], [SqlValue::Text("receipt-row".into())]);
    assert!(matches!(&values[1][0], SqlValue::Text(value) if value.len() == 16));
    assert_eq!(
        query(&follower, "SELECT value FROM mixed ORDER BY value"),
        [[SqlValue::Integer(20)], [SqlValue::Integer(30)]]
    );
    assert_eq!(
        follower
            .check_sql_request("mixed-read-write", &request)
            .unwrap()
            .unwrap()
            .1,
        Some(result)
    );

    let lingering = command(
        "temp-must-not-escape",
        &[
            "CREATE TEMP TABLE lingering(value INTEGER)",
            "INSERT INTO mixed VALUES (999)",
        ],
    );
    let payload = encode_sql_command(&lingering).unwrap();
    let rejected = proposer
        .prepare_sql_batch_effect(
            &[SqlBatchMember {
                command: &lingering,
                request_payload: &payload,
            }],
            1,
            decided.hash,
        )
        .unwrap();
    assert!(rejected.effect.is_none());
    assert!(
        matches!(&rejected.results[0], Err(Error::InvalidCommand(message)) if message.contains("TEMP"))
    );
    assert_eq!(
        query(&proposer, "SELECT max(value) FROM mixed"),
        [[SqlValue::Integer(30)]]
    );
}

#[test]
fn bundled_fts5_and_rtree_vtables_replay_mutation_delete_and_drop() {
    let dir = tempfile::tempdir().unwrap();
    let proposer_path = dir.path().join("proposer.sqlite");
    let follower_path = dir.path().join("follower.sqlite");
    let (proposer, follower) = open_pair(&dir);
    let modules = query(
        &proposer,
        "SELECT name FROM pragma_module_list ORDER BY name",
    );
    for expected in [
        "fts3",
        "fts3tokenize",
        "fts4",
        "fts4aux",
        "fts5",
        "fts5vocab",
        "rtree",
        "rtree_i32",
        "dbstat",
    ] {
        assert!(modules.contains(&vec![SqlValue::Text(expected.into())]));
    }
    let create = command(
        "vtable-create",
        &[
            "CREATE VIRTUAL TABLE legacy3 USING fts3(body)",
            "CREATE VIRTUAL TABLE legacy4 USING fts4(body)",
            "CREATE VIRTUAL TABLE documents USING fts5(body)",
            "CREATE VIRTUAL TABLE safe_reserved_token USING fts5(__rhiza_kv)",
            "CREATE VIRTUAL TABLE whole_single USING fts5('content=__rhiza_kv')",
            "CREATE VIRTUAL TABLE whole_double USING fts5(\"content=__rhiza_meta\")",
            "CREATE VIRTUAL TABLE whole_backtick USING fts5(`content=__rhiza_requests`)",
            "CREATE VIRTUAL TABLE whole_bracket USING fts5([content=__rhiza_kv])",
            "CREATE VIRTUAL TABLE whole_escaped USING fts5('content=''__rhiza_kv''')",
            "CREATE VIRTUAL TABLE block_comment_safe USING fts5(body, /* content='__rhiza_kv' */ tokenize='porter')",
            "CREATE VIRTUAL TABLE line_comment_safe USING fts5(body, -- content='__rhiza_kv'\n tokenize='porter')",
            "CREATE VIRTUAL TABLE terms4 USING fts4aux(legacy4)",
            "CREATE VIRTUAL TABLE vocabulary USING fts5vocab(documents, row)",
            "CREATE VIRTUAL TABLE tokens USING fts3tokenize(simple)",
            "INSERT INTO legacy4(body) VALUES ('legacy term')",
            "INSERT INTO legacy3(body) VALUES ('old three')",
            "INSERT INTO documents(rowid, body) VALUES (1, 'alpha one'), (2, 'beta two')",
            "CREATE VIRTUAL TABLE boxes USING rtree(id, min_x, max_x, min_y, max_y)",
            "CREATE VIRTUAL TABLE int_boxes USING rtree_i32(id, min_x, max_x, min_y, max_y)",
            "CREATE VIRTUAL TABLE page_stats USING dbstat",
            "INSERT INTO boxes VALUES (1, 0, 10, 0, 10), (2, 20, 30, 20, 30)",
            "INSERT INTO int_boxes VALUES (1, 1, 9, 2, 8), (2, 20, 30, 20, 30)",
        ],
    );
    let (_, first, _) = apply_replicated(&proposer, &follower, &create, 0, LogHash::ZERO);
    let mutate = command(
        "vtable-mutate",
        &[
            "UPDATE documents SET body = 'gamma three' WHERE rowid = 1",
            "DELETE FROM documents WHERE rowid = 2",
            "UPDATE legacy3 SET body = 'updated three' WHERE rowid = 1",
            "UPDATE boxes SET min_x = 5, max_x = 15 WHERE id = 1",
            "DELETE FROM boxes WHERE id = 2",
            "UPDATE int_boxes SET min_x = 3 WHERE id = 1",
            "DELETE FROM int_boxes WHERE id = 2",
            "DROP TABLE boxes",
        ],
    );
    let (_, second, _) = apply_replicated(&proposer, &follower, &mutate, 1, first.hash);
    assert_eq!(
        query(
            &follower,
            "SELECT rowid, body FROM documents WHERE documents MATCH 'gamma'"
        ),
        [[SqlValue::Integer(1), SqlValue::Text("gamma three".into())]]
    );
    assert_eq!(
        query(
            &follower,
            "SELECT count(*) FROM sqlite_schema WHERE name = 'boxes'"
        ),
        [[SqlValue::Integer(0)]]
    );
    assert_eq!(
        query(
            &follower,
            "SELECT token FROM tokens WHERE input = 'Hello World'"
        ),
        [
            [SqlValue::Text("hello".into())],
            [SqlValue::Text("world".into())]
        ]
    );
    assert_eq!(
        query(
            &follower,
            "SELECT DISTINCT term FROM terms4 WHERE term = 'legacy'",
        ),
        [[SqlValue::Text("legacy".into())]]
    );
    assert_eq!(
        query(
            &follower,
            "SELECT body FROM legacy3 WHERE legacy3 MATCH 'updated'",
        ),
        [[SqlValue::Text("updated three".into())]]
    );
    assert_eq!(
        query(
            &follower,
            "SELECT term FROM vocabulary WHERE term = 'gamma'",
        ),
        [[SqlValue::Text("gamma".into())]]
    );
    assert_eq!(
        query(&follower, "SELECT min_x, max_x FROM int_boxes WHERE id = 1"),
        [[SqlValue::Integer(3), SqlValue::Integer(9)]]
    );
    assert_ne!(
        query(&follower, "SELECT count(*) FROM page_stats"),
        [[SqlValue::Integer(0)]]
    );
    for (table, column) in [
        ("whole_single", "content=__rhiza_kv"),
        ("whole_double", "content=__rhiza_meta"),
        ("whole_backtick", "content=__rhiza_requests"),
        ("whole_bracket", "content=__rhiza_kv"),
        ("whole_escaped", "content='__rhiza_kv'"),
    ] {
        assert_eq!(
            query(
                &follower,
                &format!("SELECT name FROM pragma_table_info('{table}')"),
            ),
            [[SqlValue::Text(column.into())]]
        );
    }
    for denied in [
        command(
            "unknown-vtable",
            &["CREATE VIRTUAL TABLE forbidden USING custom_module(value)"],
        ),
        command(
            "internal-content-vtable",
            &["CREATE VIRTUAL TABLE forbidden_content USING fts5(key, value, content='__rhiza_kv')"],
        ),
    ] {
        let payload = encode_sql_command(&denied).unwrap();
        let rejected = proposer
            .prepare_sql_batch_effect(
                &[SqlBatchMember {
                    command: &denied,
                    request_payload: &payload,
                }],
                2,
                second.hash,
            )
            .unwrap();
        assert!(rejected.effect.is_none());
        assert!(rejected.results[0].is_err());
    }
    drop(proposer);
    drop(follower);
    let proposer = SqliteStateMachine::open_existing(&proposer_path).unwrap();
    let follower = SqliteStateMachine::open_existing(&follower_path).unwrap();
    assert_eq!(
        query(&proposer, "SELECT rowid, body FROM documents"),
        query(&follower, "SELECT rowid, body FROM documents")
    );
    for sql in [
        "SELECT body FROM legacy3",
        "SELECT term FROM vocabulary WHERE term = 'gamma'",
        "SELECT min_x, max_x FROM int_boxes",
        "SELECT count(*) FROM page_stats",
    ] {
        assert_eq!(query(&proposer, sql), query(&follower, sql));
    }
}

#[test]
fn modern_sqlite_schema_and_dml_features_keep_exact_page_replay() {
    let dir = tempfile::tempdir().unwrap();
    let proposer_path = dir.path().join("proposer.sqlite");
    let follower_path = dir.path().join("follower.sqlite");
    let (proposer, follower) = open_pair(&dir);
    let modern = command(
        "modern-sqlite",
        &[
            "CREATE TABLE modern(id INTEGER PRIMARY KEY, base INTEGER NOT NULL, doubled INTEGER GENERATED ALWAYS AS (base * 2) STORED) STRICT",
            "CREATE INDEX modern_positive ON modern(base) WHERE base > 0",
            "INSERT INTO modern(id, base) VALUES (1, 2) ON CONFLICT(id) DO UPDATE SET base = excluded.base",
            "INSERT INTO modern(id, base) VALUES (1, 7) ON CONFLICT(id) DO UPDATE SET base = excluded.base",
            "ALTER TABLE modern ADD COLUMN note TEXT",
            "CREATE VIEW modern_view AS SELECT id, doubled, note FROM modern",
            "CREATE TABLE labels(name TEXT PRIMARY KEY, weight INTEGER NOT NULL) WITHOUT ROWID",
            "WITH input(name, weight) AS (VALUES ('exact', 9)) INSERT INTO labels SELECT * FROM input",
            "SELECT id, doubled, note FROM modern_view",
            "CREATE TABLE __rhiza_user_table(value TEXT)",
            "INSERT INTO __rhiza_user_table VALUES ('allowed')",
            "PRAGMA user_version = 23",
            "PRAGMA application_id = 1380473162",
            "SELECT count(*) FROM sqlite_schema WHERE name = 'sqlite_stat1'",
            "PRAGMA optimize=65538",
            "PRAGMA optimize",
            "PRAGMA optimize(65538)",
        ],
    );
    let (_, decided, result) = apply_replicated(&proposer, &follower, &modern, 0, LogHash::ZERO);
    assert_eq!(
        result.statement_results[8].returning.as_ref().unwrap().rows,
        [[SqlValue::Integer(1), SqlValue::Integer(14), SqlValue::Null]]
    );
    assert_eq!(
        result.statement_results[13]
            .returning
            .as_ref()
            .unwrap()
            .rows,
        [[SqlValue::Integer(0)]]
    );
    assert_eq!(
        query(&follower, "SELECT name, weight FROM labels"),
        [[SqlValue::Text("exact".into()), SqlValue::Integer(9)]]
    );
    assert_eq!(
        query(&follower, "SELECT value FROM __rhiza_user_table"),
        [[SqlValue::Text("allowed".into())]]
    );
    for denied in [
        command("schema-version-write", &["PRAGMA schema_version = 99"]),
        command("optimize-invalid", &["PRAGMA optimize = 'garbage'"]),
        command("incremental-vacuum-write", &["PRAGMA incremental_vacuum"]),
    ] {
        let payload = encode_sql_command(&denied).unwrap();
        let rejected = proposer
            .prepare_sql_batch_effect(
                &[SqlBatchMember {
                    command: &denied,
                    request_payload: &payload,
                }],
                1,
                decided.hash,
            )
            .unwrap();
        assert!(rejected.effect.is_none());
        assert!(rejected.results[0].is_err());
    }
    drop(proposer);
    drop(follower);
    let proposer = SqliteStateMachine::open_existing(&proposer_path).unwrap();
    let follower = SqliteStateMachine::open_existing(&follower_path).unwrap();
    for (pragma, expected) in [
        ("PRAGMA user_version", 23),
        ("PRAGMA application_id", 1_380_473_162),
    ] {
        assert_eq!(query(&proposer, pragma), [[SqlValue::Integer(expected)]]);
        assert_eq!(query(&proposer, pragma), query(&follower, pragma));
    }
    assert_eq!(
        query(&proposer, "SELECT count(*) FROM sqlite_stat1"),
        query(&follower, "SELECT count(*) FROM sqlite_stat1")
    );
    assert_ne!(
        query(&proposer, "SELECT count(*) FROM sqlite_stat1"),
        [[SqlValue::Integer(0)]]
    );
    assert_eq!(
        query(&proposer, "SELECT value FROM __rhiza_user_table"),
        [[SqlValue::Text("allowed".into())]]
    );
    assert_eq!(
        query(&proposer, "SELECT value FROM __rhiza_user_table"),
        query(&follower, "SELECT value FROM __rhiza_user_table")
    );
}

#[test]
fn pure_replicated_read_receipt_survives_duplicate_retry_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let follower_path = dir.path().join("follower.sqlite");
    let (proposer, follower) = open_pair(&dir);
    let command = command(
        "pure-read",
        &[
            "SELECT 40 + 2 AS answer",
            "VALUES ('stable'), (hex(randomblob(8)))",
        ],
    );
    let (request, decided, result) =
        apply_replicated(&proposer, &follower, &command, 0, LogHash::ZERO);
    assert!(decode_qwal_v3(&decided.payload).unwrap().pages.is_empty());
    assert_eq!(
        follower
            .check_sql_request("pure-read", &request)
            .unwrap()
            .unwrap()
            .1,
        Some(result.clone())
    );
    drop(follower);
    let follower = SqliteStateMachine::open_existing(&follower_path).unwrap();
    assert_eq!(
        follower
            .apply_entry_with_result(&decided)
            .unwrap()
            .sql_result(),
        Some(&result)
    );
    assert_eq!(
        follower
            .check_sql_request("pure-read", &request)
            .unwrap()
            .unwrap()
            .1,
        Some(result)
    );
}

#[test]
fn qwal_apply_rejects_an_effect_prepared_from_a_stale_base() {
    let dir = tempfile::tempdir().unwrap();
    let proposer = SqliteStateMachine::open(
        dir.path().join("proposer.sqlite"),
        "cluster-a",
        "node-1",
        1,
        1,
    )
    .unwrap();
    let follower = SqliteStateMachine::open(
        dir.path().join("follower.sqlite"),
        "cluster-a",
        "node-2",
        1,
        1,
    )
    .unwrap();
    let command = SqlCommand {
        request_id: "stale-base".into(),
        statements: vec![SqlStatement {
            sql: "CREATE TABLE must_not_exist(id INTEGER PRIMARY KEY)".into(),
            parameters: vec![],
        }],
    };
    let (_, effect) = prepared_qwal(&proposer, &command, 0, LogHash::ZERO);

    let winner = noop(1, LogHash::ZERO);
    follower.apply_entry(&winner).unwrap();
    let before = follower.canonical_db_digest().unwrap();
    let stale = entry(2, winner.hash, &effect);

    assert!(matches!(
        follower.apply_entry_with_result(&stale),
        Err(Error::InvalidEntry(_))
    ));
    assert_eq!(follower.canonical_db_digest().unwrap(), before);
    assert_eq!(
        query(
            &follower,
            "SELECT count(*) FROM sqlite_schema WHERE name='must_not_exist'"
        ),
        [[SqlValue::Integer(0)]]
    );
}

#[test]
fn qwal_apply_rejects_a_forged_target_root_before_writing_canonical_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite");
    let db = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
    let command = SqlCommand {
        request_id: "forged-target-root".into(),
        statements: vec![SqlStatement {
            sql: "CREATE TABLE must_not_be_installed(value TEXT NOT NULL)".into(),
            parameters: vec![],
        }],
    };
    let (_, encoded) = prepared_qwal(&db, &command, 0, LogHash::ZERO);
    let mut forged = decode_qwal_v3(&encoded).unwrap();
    forged.target_state.state_root = LogHash::digest(&[b"forged-target-root"]);
    let forged = encode_qwal_v3(&forged).unwrap();
    let before = std::fs::read(&path).unwrap();

    assert!(matches!(
        db.apply_entry(&entry(1, LogHash::ZERO, &forged)),
        Err(Error::InvalidEntry(message)) if message.contains("target page state")
    ));
    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert_eq!(db.applied_tip_value().unwrap(), (0, LogHash::ZERO));
}

#[test]
fn qwal_apply_rejects_forged_no_change_target_with_inconsistent_pages() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite");
    let db = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
    let setup = SqlCommand {
        request_id: "forged-no-change-setup".into(),
        statements: vec![
            SqlStatement {
                sql: "CREATE TABLE forged_no_change(value TEXT NOT NULL)".into(),
                parameters: vec![],
            },
            SqlStatement {
                sql: "INSERT INTO forged_no_change VALUES ('before')".into(),
                parameters: vec![],
            },
        ],
    };
    let (_, setup_effect) = prepared_qwal(&db, &setup, 0, LogHash::ZERO);
    let setup_entry = entry(1, LogHash::ZERO, &setup_effect);
    db.apply_entry(&setup_entry).unwrap();
    let update = SqlCommand {
        request_id: "forged-no-change".into(),
        statements: vec![SqlStatement {
            sql: "UPDATE forged_no_change SET value = 'after!'".into(),
            parameters: vec![],
        }],
    };
    let (_, encoded) = prepared_qwal(&db, &update, 1, setup_entry.hash);
    let mut forged = decode_qwal_v3(&encoded).unwrap();
    assert!(!forged.pages.is_empty());
    assert_eq!(forged.base_state.page_count, forged.target_state.page_count);
    forged.target_state = forged.base_state;
    let forged = encode_qwal_v3(&forged).unwrap();

    assert!(matches!(
        db.apply_entry(&entry(2, setup_entry.hash, &forged)),
        Err(Error::InvalidEntry(message)) if message.contains("target page state")
    ));
    assert_eq!(db.applied_tip_value().unwrap(), (1, setup_entry.hash));
    assert!(matches!(
        db.query_sql(
            &SqlStatement {
                sql: "SELECT value FROM forged_no_change".into(),
                parameters: vec![],
            },
            1,
            64,
        ),
        Err(Error::InvalidEntry(message)) if message.contains("pending")
    ));
    drop(db);
    let reopened = SqliteStateMachine::open_existing(&path).unwrap();
    assert_eq!(
        query(&reopened, "SELECT value FROM forged_no_change"),
        [[SqlValue::Text("before".into())]]
    );
}

#[test]
fn pure_noop_commit_survives_reopen_and_replay_but_rejects_a_different_hash() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite");
    let winner = noop(1, LogHash::ZERO);
    let db = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();

    assert_eq!(db.apply_entry(&winner).unwrap().applied_index(), 1);
    assert_eq!(
        ControlStore::open_existing_unchecked(path.with_extension("sqlite.control"))
            .unwrap()
            .pending()
            .unwrap(),
        None
    );
    drop(db);

    let reopened = SqliteStateMachine::open_existing(&path).unwrap();
    assert_eq!(reopened.apply_entry(&winner).unwrap().applied_index(), 1);
    let mut conflicting = winner.clone();
    conflicting.payload = b"different".to_vec();
    conflicting.hash = conflicting.recompute_hash();
    assert!(matches!(
        reopened.apply_entry(&conflicting),
        Err(Error::InvalidEntry(message)) if message.contains("different hash")
    ));
    assert_eq!(reopened.applied_tip_value().unwrap(), (1, winner.hash));
}

#[test]
fn qwal_prepare_rejects_an_effect_larger_than_the_inline_limit() {
    assert_eq!(MAX_SQL_EFFECT_BYTES, 512 * 1024);
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
        .unwrap();
    let command = SqlCommand {
        request_id: "oversized-effect".into(),
        statements: vec![
            SqlStatement {
                sql: "CREATE TABLE blobs(value BLOB NOT NULL)".into(),
                parameters: vec![],
            },
            SqlStatement {
                sql: format!(
                    "INSERT INTO blobs(value) VALUES (randomblob({}))",
                    MAX_SQL_EFFECT_BYTES + 64 * 1024
                ),
                parameters: vec![],
            },
        ],
    };
    let request = encode_sql_command(&command).unwrap();
    let before = db.canonical_db_digest().unwrap();

    assert!(matches!(
        db.prepare_sql_batch_effect(
            &[SqlBatchMember {
                command: &command,
                request_payload: &request,
            }],
            0,
            LogHash::ZERO,
        ),
        Err(Error::ResourceExhausted(_))
    ));
    assert_eq!(db.canonical_db_digest().unwrap(), before);
}

#[test]
fn qwal_duplicate_apply_returns_the_original_result_without_reapplying_pages() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
        .unwrap();
    let command = SqlCommand {
        request_id: "duplicate-effect".into(),
        statements: vec![
            SqlStatement {
                sql: "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)".into(),
                parameters: vec![],
            },
            SqlStatement {
                sql: "INSERT INTO items VALUES (1, 'once') RETURNING id, value".into(),
                parameters: vec![],
            },
        ],
    };
    let (request, effect) = prepared_qwal(&db, &command, 0, LogHash::ZERO);
    let decided = entry(1, LogHash::ZERO, &effect);

    let first = db.apply_entry_with_result(&decided).unwrap();
    let digest = db.canonical_db_digest().unwrap();
    let duplicate = db.apply_entry_with_result(&decided).unwrap();

    assert_eq!(duplicate.sql_result(), first.sql_result());
    assert_eq!(db.canonical_db_digest().unwrap(), digest);
    assert_eq!(
        query(&db, "SELECT id, value FROM items"),
        [[SqlValue::Integer(1), SqlValue::Text("once".into())]]
    );
    assert_eq!(
        db.check_sql_request("duplicate-effect", &request)
            .unwrap()
            .unwrap()
            .1
            .as_ref(),
        first.sql_result()
    );
}

#[test]
fn qwal_snapshot_restore_rebinds_node_identity_without_changing_user_bytes_or_result() {
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source.sqlite");
    let source = SqliteStateMachine::open(&source_path, "cluster-a", "node-1", 1, 1).unwrap();
    let command = SqlCommand {
        request_id: "snapshot-effect".into(),
        statements: vec![
            SqlStatement {
                sql: "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)".into(),
                parameters: vec![],
            },
            SqlStatement {
                sql: "INSERT INTO items VALUES (7, 'preserved') RETURNING id, value".into(),
                parameters: vec![],
            },
        ],
    };
    let (request, effect) = prepared_qwal(&source, &command, 0, LogHash::ZERO);
    let decided = entry(1, LogHash::ZERO, &effect);
    let original = source
        .apply_entry_with_result(&decided)
        .unwrap()
        .sql_result()
        .cloned()
        .unwrap();
    let source_digest = source.canonical_db_digest().unwrap();
    let snapshot = source.create_snapshot(1).unwrap();
    drop(source);

    let restored_path = dir.path().join("restored.sqlite");
    restore_snapshot_file(&restored_path, &snapshot, "node-2").unwrap();
    let restored = SqliteStateMachine::open(&restored_path, "cluster-a", "node-2", 1, 1).unwrap();

    assert_eq!(restored.canonical_db_digest().unwrap(), source_digest);
    assert_eq!(restored.applied_index_value().unwrap(), 1);
    assert_eq!(
        query(&restored, "SELECT id, value FROM items"),
        [[SqlValue::Integer(7), SqlValue::Text("preserved".into())]]
    );
    assert_eq!(
        restored
            .check_sql_request("snapshot-effect", &request)
            .unwrap()
            .unwrap()
            .1,
        Some(original)
    );
}

#[test]
fn qwal_snapshot_restore_rejects_user_bytes_that_do_not_match_the_bound_page_state() {
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source.sqlite");
    let source = SqliteStateMachine::open(&source_path, "cluster-a", "node-1", 1, 1).unwrap();
    let command = SqlCommand {
        request_id: "snapshot-root-mismatch".into(),
        statements: vec![
            SqlStatement {
                sql: "CREATE TABLE snapshot_values(value TEXT NOT NULL)".into(),
                parameters: vec![],
            },
            SqlStatement {
                sql: "INSERT INTO snapshot_values VALUES ('preserved')".into(),
                parameters: vec![],
            },
        ],
    };
    let (_, effect) = prepared_qwal(&source, &command, 0, LogHash::ZERO);
    source
        .apply_entry(&entry(1, LogHash::ZERO, &effect))
        .unwrap();
    let snapshot = source.create_snapshot(1).unwrap();
    let mut tampered = snapshot.db_bytes().to_vec();
    let offset = tampered
        .windows(b"preserved".len())
        .position(|window| window == b"preserved")
        .expect("snapshot contains the inserted SQLite value");
    tampered[offset..offset + b"tampered!".len()].copy_from_slice(b"tampered!");
    let tampered = Snapshot::new(snapshot.manifest().clone(), tampered);

    assert!(matches!(
        restore_snapshot_file(dir.path().join("rejected.sqlite"), &tampered, "node-2"),
        Err(Error::InvalidSnapshot(message)) if message.contains("page state")
    ));
}

#[test]
fn qwal_only_apply_rejects_legacy_qsql_and_qefx_payloads() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
        .unwrap();
    let command = SqlCommand {
        request_id: "legacy-qsql".into(),
        statements: vec![SqlStatement {
            sql: "CREATE TABLE forbidden_replay(id INTEGER PRIMARY KEY)".into(),
            parameters: vec![],
        }],
    };
    let qsql = encode_sql_command(&command).unwrap();
    assert!(qsql.starts_with(b"QSQL\0\x02"));
    let before = db.canonical_db_digest().unwrap();

    let legacy_payloads = [
        qsql,
        b"QEFX\0\x01{}".to_vec(),
        b"QWAL\0\x03legacy-v2".to_vec(),
    ];
    for payload in legacy_payloads {
        assert!(matches!(
            db.apply_entry(&entry(1, LogHash::ZERO, &payload)),
            Err(Error::InvalidCommand(_))
        ));
        assert_eq!(db.canonical_db_digest().unwrap(), before);
    }
}

#[test]
fn current_qwal_database_reopens_with_application_tables() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("current.sqlite");
    let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
    let tables = command(
        "application-tables",
        &[
            "CREATE TABLE __rhiza_meta(value TEXT NOT NULL)",
            "CREATE TABLE __rhiza_requests(value TEXT NOT NULL)",
            "INSERT INTO __rhiza_meta VALUES ('meta')",
            "INSERT INTO __rhiza_requests VALUES ('request')",
        ],
    );
    let (_, effect) = prepared_qwal(&database, &tables, 0, LogHash::ZERO);
    database
        .apply_entry(&entry(1, LogHash::ZERO, &effect))
        .unwrap();
    drop(database);

    let reopened = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
    assert_eq!(
        query(&reopened, "SELECT value FROM __rhiza_meta"),
        [[SqlValue::Text("meta".into())]]
    );
    assert_eq!(
        query(&reopened, "SELECT value FROM __rhiza_requests"),
        [[SqlValue::Text("request".into())]]
    );
}

#[test]
fn incomplete_database_control_pairs_fail_without_mutating_either_file() {
    for (name, database, control) in [
        ("database-only", Some(b"database".as_slice()), None),
        ("control-only", None, Some(b"control".as_slice())),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let control_path = path.with_extension("sqlite.control");
        if let Some(bytes) = database {
            std::fs::write(&path, bytes).unwrap();
        }
        if let Some(bytes) = control {
            std::fs::write(&control_path, bytes).unwrap();
        }
        let before_database = std::fs::read(&path).ok();
        let before_control = std::fs::read(&control_path).ok();

        assert!(
            SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).is_err(),
            "{name}"
        );
        assert_eq!(std::fs::read(&path).ok(), before_database, "{name}");
        assert_eq!(std::fs::read(&control_path).ok(), before_control, "{name}");
    }
}

#[test]
fn recovery_restore_overrides_the_embedded_generation_for_the_next_effect() {
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source.sqlite");
    let source = SqliteStateMachine::open(&source_path, "cluster-a", "node-1", 1, 1).unwrap();
    let setup = SqlCommand {
        request_id: "setup-recovery".into(),
        statements: vec![SqlStatement {
            sql: "CREATE TABLE recovered(id INTEGER PRIMARY KEY)".into(),
            parameters: vec![],
        }],
    };
    let (_, setup_effect) = prepared_qwal(&source, &setup, 0, LogHash::ZERO);
    let setup_entry = entry(1, LogHash::ZERO, &setup_effect);
    source.apply_entry(&setup_entry).unwrap();
    let recovery = source.create_recovery_snapshot(7).unwrap();

    let target_path = dir.path().join("target.sqlite");
    restore_recovery_snapshot_file(
        &target_path,
        recovery.db_bytes(),
        recovery.anchor(),
        "node-2",
    )
    .unwrap();
    let target = SqliteStateMachine::open_existing(&target_path).unwrap();
    let next = SqlCommand {
        request_id: "after-recovery".into(),
        statements: vec![SqlStatement {
            sql: "INSERT INTO recovered(id) VALUES (1)".into(),
            parameters: vec![],
        }],
    };
    let (_, next_effect) = prepared_qwal(&target, &next, 1, setup_entry.hash);
    assert_eq!(decode_qwal_v3(&next_effect).unwrap().recovery_generation, 7);
}
