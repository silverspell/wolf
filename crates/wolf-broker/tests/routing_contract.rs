use wolf_broker::binding::Binding;
use wolf_broker::exchange::{
    ArgumentValue, ExchangeArguments, ExchangeConfig, ExchangeKind, ExchangeName, ExchangeRegistry,
};
use wolf_broker::router::Router;
use wolf_broker::topic::{RoutingKey, TopicPattern, TopicTrie};
use wolf_broker::{DomainError, QueueName};

fn queue(name: &str) -> QueueName {
    QueueName::new(name).expect("valid queue name")
}

fn direct_exchange() -> ExchangeConfig {
    ExchangeConfig::new(ExchangeKind::Direct)
}

fn fanout_exchange() -> ExchangeConfig {
    ExchangeConfig::new(ExchangeKind::Fanout)
}

fn topic_exchange() -> ExchangeConfig {
    ExchangeConfig::new(ExchangeKind::Topic)
}

fn sorted(mut queues: Vec<QueueName>) -> Vec<String> {
    queues.sort();
    queues
        .into_iter()
        .map(|queue| queue.as_str().to_string())
        .collect()
}

#[test]
fn direct_exchange_routes_only_exact_routing_key_matches() {
    let mut registry = ExchangeRegistry::new();
    registry
        .declare("events", direct_exchange())
        .expect("declare direct exchange");
    registry
        .bind("events", queue("alpha-1"), "alpha")
        .expect("bind alpha-1");
    registry
        .bind("events", queue("alpha-2"), "alpha")
        .expect("bind alpha-2");
    registry
        .bind("events", queue("beta"), "beta")
        .expect("bind beta");

    let router = Router::new(registry);

    assert_eq!(
        sorted(router.route("events", "alpha").expect("route")),
        vec!["alpha-1".to_string(), "alpha-2".to_string()]
    );
    assert_eq!(
        sorted(router.route("events", "beta").expect("route")),
        vec!["beta".to_string()]
    );
    assert!(router.route("events", "gamma").expect("route").is_empty());
}

#[test]
fn fanout_exchange_routes_each_bound_queue_once() {
    let mut registry = ExchangeRegistry::new();
    registry
        .declare("broadcast", fanout_exchange())
        .expect("declare fanout exchange");
    registry
        .bind("broadcast", queue("one"), "ignored-a")
        .expect("bind one");
    registry
        .bind("broadcast", queue("two"), "ignored-b")
        .expect("bind two");
    registry
        .bind("broadcast", queue("three"), "ignored-c")
        .expect("bind three");
    registry
        .bind("broadcast", queue("one"), "ignored-d")
        .expect("duplicate bind is idempotent");

    let router = Router::new(registry);
    let routed = router.route("broadcast", "anything").expect("route");

    assert_eq!(
        sorted(routed),
        vec!["one".to_string(), "three".to_string(), "two".to_string()]
    );
}

#[test]
fn topic_exchange_handles_star_hash_and_empty_segments() {
    let mut registry = ExchangeRegistry::new();
    registry
        .declare("events", topic_exchange())
        .expect("declare topic exchange");
    registry
        .bind("events", queue("star"), "a.*")
        .expect("bind star");
    registry
        .bind("events", queue("hash"), "a.#")
        .expect("bind hash");
    registry
        .bind("events", queue("middle"), "#.b.#")
        .expect("bind middle");
    registry
        .bind("events", queue("empty"), "")
        .expect("bind empty pattern");

    let router = Router::new(registry);

    assert_eq!(
        sorted(router.route("events", "a.b").expect("route")),
        vec!["hash".to_string(), "middle".to_string(), "star".to_string()]
    );
    assert_eq!(
        sorted(router.route("events", "a").expect("route")),
        vec!["hash".to_string()]
    );
    assert_eq!(
        sorted(router.route("events", "x.y.b.z").expect("route")),
        vec!["middle".to_string()]
    );
    assert_eq!(
        sorted(router.route("events", "").expect("route")),
        vec!["empty".to_string()]
    );
    assert!(
        router
            .route("events", "a.")
            .expect("route")
            .contains(&queue("star"))
    );
}

#[test]
fn topic_constructor_agrees_with_trie_for_edge_cases() {
    let cases = [
        ("*", "one", true),
        ("*", "", true),
        ("*", "one.two", false),
        ("#", "", true),
        ("#", "one", true),
        ("#", "one.two.three", true),
        ("#.#", "", true),
        ("#.#", "one", true),
        ("a.#", "a", true),
        ("a.#", "a.b.c", true),
        ("a.*", "a", false),
        ("a.*", "a.b", true),
        ("a.*", "a.b.c", false),
        ("a.#.b", "a.b", true),
        ("a.#.b", "a.x.y.b", true),
        ("a.#.b", "a.x.y", false),
        ("a..b", "a..b", true),
        ("a..b", "a.b", false),
        ("A", "a", false),
    ];

    for (pattern, routing_key, expected) in cases {
        let parsed = TopicPattern::new(pattern);
        assert_eq!(
            parsed.matches(routing_key),
            expected,
            "pattern {pattern:?} against {routing_key:?}"
        );

        let mut trie = TopicTrie::new();
        trie.insert(&parsed, queue("q"));
        let mut matched = Vec::new();
        trie.match_into(routing_key, &mut matched);
        assert_eq!(
            !matched.is_empty(),
            expected,
            "trie pattern {pattern:?} against {routing_key:?}"
        );
    }
}

#[test]
fn default_and_predefined_exchanges_follow_reserved_name_rules() {
    let registry = ExchangeRegistry::new();
    let default = registry.exchange("").expect("default exchange");
    assert_eq!(default.config().kind, ExchangeKind::Direct);
    assert_eq!(
        registry
            .exchange("amq.direct")
            .expect("amq.direct")
            .config()
            .kind,
        ExchangeKind::Direct
    );
    assert_eq!(
        registry
            .exchange("amq.fanout")
            .expect("amq.fanout")
            .config()
            .kind,
        ExchangeKind::Fanout
    );
    assert_eq!(
        registry
            .exchange("amq.topic")
            .expect("amq.topic")
            .config()
            .kind,
        ExchangeKind::Topic
    );

    let mut registry = registry;
    for result in [
        registry.declare("", direct_exchange()),
        registry.declare("amq.direct", direct_exchange()),
        registry.declare("amq.custom", direct_exchange()),
    ] {
        assert!(matches!(result, Err(DomainError::AccessRefused { .. })));
    }

    assert!(matches!(
        registry.bind("", queue("q"), "q"),
        Err(DomainError::AccessRefused { .. })
    ));
    assert!(matches!(
        registry.unbind("", queue("q"), "q"),
        Err(DomainError::AccessRefused { .. })
    ));
    assert!(matches!(
        registry.delete(""),
        Err(DomainError::AccessRefused { .. })
    ));

    let router = Router::new(registry);
    assert_eq!(
        sorted(router.route("", "implicit-queue").expect("route")),
        vec!["implicit-queue".to_string()]
    );
}

#[test]
fn redeclare_and_exchange_type_errors_use_domain_contracts() {
    let mut registry = ExchangeRegistry::new();
    let mut arguments = ExchangeArguments::new();
    arguments.insert(
        "alternate".to_string(),
        ArgumentValue::String("x".to_string()),
    );

    let mut config = ExchangeConfig::new(ExchangeKind::Direct);
    config.arguments = arguments;

    registry.declare("stable", config.clone()).expect("declare");
    registry
        .declare("stable", config)
        .expect("identical redeclare is idempotent");

    let error = registry
        .declare("stable", ExchangeConfig::new(ExchangeKind::Fanout))
        .expect_err("different kind must fail");
    assert!(matches!(error, DomainError::PreconditionFailed { .. }));

    let error = ExchangeKind::parse("headers").expect_err("headers unsupported");
    assert!(matches!(error, DomainError::CommandInvalid { .. }));

    let error = ExchangeKind::parse("not-a-type").expect_err("unknown type");
    assert!(matches!(error, DomainError::CommandInvalid { .. }));

    assert_eq!(
        ExchangeKind::parse("direct").expect("direct"),
        ExchangeKind::Direct
    );
    assert_eq!(
        ExchangeKind::parse("fanout").expect("fanout"),
        ExchangeKind::Fanout
    );
    assert_eq!(
        ExchangeKind::parse("topic").expect("topic"),
        ExchangeKind::Topic
    );
}

#[test]
fn missing_exchange_and_idempotent_delete_are_explicit() {
    let mut registry = ExchangeRegistry::new();

    let error = registry
        .bind("missing", queue("q"), "q")
        .expect_err("unknown exchange");
    assert_eq!(
        error,
        DomainError::ExchangeNotFound {
            vhost: String::new(),
            exchange: "missing".to_string(),
        }
    );

    assert!(!registry.delete("missing").expect("idempotent delete"));
    registry
        .declare("temporary", direct_exchange())
        .expect("declare");
    registry.bind("temporary", queue("q"), "q").expect("bind");
    assert!(registry.delete("temporary").expect("delete existing"));
    assert!(!registry.delete("temporary").expect("second delete"));
}

#[test]
fn binding_is_public_and_unbind_is_idempotent() {
    let binding = Binding::new(
        ExchangeName::new("events"),
        queue("q"),
        RoutingKey::new("a.#"),
    );

    assert_eq!(binding.source.as_str(), "events");
    assert_eq!(binding.queue.as_str(), "q");
    assert_eq!(binding.routing_key.as_str(), "a.#");

    let mut registry = ExchangeRegistry::new();
    registry
        .declare("events", topic_exchange())
        .expect("declare");
    registry.bind("events", queue("q"), "a.#").expect("bind");
    assert!(
        registry
            .unbind("events", queue("q"), "a.#")
            .expect("unbind")
    );
    assert!(
        !registry
            .unbind("events", queue("q"), "a.#")
            .expect("unbind again")
    );
}
