use ermya_graph::gql::{self, GqlStatement, GqlValue, param_substitution};
use ermya_graph::{Graph, props};
use ermya_graph_server::graph_accessor::{DefaultGraphAccessor, GraphAccessor};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

fn bound(text: &str, value: GqlValue) -> GqlStatement {
    let mut statement = gql::parse_statement(text).unwrap();
    param_substitution::apply(&mut statement, &HashMap::from([("id".into(), value)])).unwrap();
    statement
}

#[test]
fn node_pattern_parameters_select_only_bound_values_and_are_not_query_text() {
    let mut graph = Graph::new();
    graph.add_node("Item", props! {"id" => 1i64}).unwrap();
    graph.add_node("Item", props! {"id" => 2i64}).unwrap();
    let hostile = "'} ) DELETE n //";
    graph.add_node("Item", props! {"id" => hostile}).unwrap();
    for value in [
        GqlValue::Int(1),
        GqlValue::Int(2),
        GqlValue::Str(hostile.into()),
    ] {
        let GqlStatement::Query(query) = bound(
            "MATCH (n:Item {id: $id}) RETURN n.id AS value",
            value.clone(),
        ) else {
            panic!()
        };
        let result = gql::execute(&graph, &query, 0).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0]["value"], value);
    }
    assert_eq!(graph.node_count(), 3);
}

#[test]
fn relationship_and_endpoint_parameters_constrain_fixed_and_variable_length_paths() {
    let mut graph = Graph::new();
    let a = graph.add_node("Item", props! {"id" => 1i64}).unwrap();
    let b = graph.add_node("Item", props! {"id" => 2i64}).unwrap();
    graph.add_edge("LINK", a, b, props! {"id" => 1i64}).unwrap();
    graph.add_edge("LINK", a, b, props! {"id" => 2i64}).unwrap();
    for query in [
        "MATCH (a:Item {id:$id})-[r:LINK {id:$id}]->(b) RETURN r.id AS value",
        "MATCH (a:Item {id:$id})-[:LINK {id:$id}*1..2]->(b) RETURN b.id AS value",
    ] {
        let GqlStatement::Query(q) = bound(query, GqlValue::Int(1)) else {
            panic!()
        };
        assert_eq!(gql::execute(&graph, &q, 0).unwrap().len(), 1);
    }
}

#[test]
fn missing_and_unsupported_parameters_fail_before_execution() {
    let mut statement = gql::parse_statement("MATCH (n {id:$id}) RETURN n").unwrap();
    assert!(matches!(
        param_substitution::apply(&mut statement, &HashMap::<String, GqlValue>::new()),
        Err(param_substitution::ParamError::MissingParameter(_))
    ));
    assert!(matches!(
        param_substitution::apply(
            &mut statement,
            &HashMap::from([("id".into(), GqlValue::Map(HashMap::new()))])
        ),
        Err(param_substitution::ParamError::UnsupportedParamValue { .. })
    ));
}

#[test]
fn null_and_list_pattern_parameters_do_not_widen_match_or_count() {
    let mut graph = Graph::new();
    graph.add_node("Item", props! {"id" => 1i64}).unwrap();
    for value in [GqlValue::Null, GqlValue::List(vec![GqlValue::Int(1)])] {
        for text in [
            "MATCH (n:Item {id:$id}) RETURN n",
            "MATCH (n:Item {id:$id}) RETURN count(n) AS total",
        ] {
            let GqlStatement::Query(q) = bound(text, value.clone()) else {
                panic!()
            };
            let rows = gql::execute(&graph, &q, 0).unwrap();
            if text.contains("count") {
                assert_eq!(rows[0]["total"], GqlValue::Int(0));
            } else {
                assert!(rows.is_empty());
            }
        }
    }
}

#[test]
fn pattern_binding_applies_to_mutations_in_autocommit_and_transactions() {
    for transactional in [false, true] {
        let mut graph = Graph::new();
        graph.enable_mvcc();
        let selected = graph.add_node("Item", props! {"id"=>1i64}).unwrap();
        let other = graph.add_node("Item", props! {"id"=>2i64}).unwrap();
        let graph = Arc::new(RwLock::new(graph));
        let accessor = DefaultGraphAccessor::new(graph.clone());
        let GqlStatement::Mutation(mutation) = bound(
            "MATCH (n:Item {id:$id}) SET n.changed = true",
            GqlValue::Int(1),
        ) else {
            panic!()
        };
        if transactional {
            let txn = accessor.begin_txn().unwrap();
            accessor
                .execute_mutation_in_txn(txn, &mutation, HashMap::new(), None)
                .unwrap();
            accessor.commit_txn(txn).unwrap();
        } else {
            accessor
                .execute_mutation(&mutation, HashMap::new(), None)
                .unwrap();
        }
        let g = graph.read().unwrap();
        assert!(
            g.node(selected)
                .unwrap()
                .properties()
                .contains_key("changed")
        );
        assert!(!g.node(other).unwrap().properties().contains_key("changed"));
    }
}

#[test]
fn pattern_parameters_bind_inside_pipeline_and_shortest_path() {
    let mut graph = Graph::new();
    let a = graph.add_node("Item", props! {"id"=>1i64}).unwrap();
    let b = graph.add_node("Item", props! {"id"=>2i64}).unwrap();
    graph.add_edge("LINK", a, b, props! {"id"=>1i64}).unwrap();
    let accessor = DefaultGraphAccessor::new(Arc::new(RwLock::new(graph)));
    let GqlStatement::Pipeline(p) = bound(
        "MATCH (n:Item {id:$id}) WITH n RETURN n.id AS value",
        GqlValue::Int(1),
    ) else {
        panic!()
    };
    assert_eq!(
        accessor
            .execute_pipeline(&p, HashMap::new(), 0, None)
            .unwrap()
            .0
            .len(),
        1
    );
    let mut q = gql::parse_statement(
        "MATCH (a:Item), (b:Item) RETURN shortestPath((a)-[:LINK {id:$id}*1..2]->(b)) AS path",
    )
    .unwrap();
    assert!(matches!(
        param_substitution::apply(&mut q, &HashMap::<String, GqlValue>::new()),
        Err(param_substitution::ParamError::MissingParameter(_))
    ));
    param_substitution::apply(&mut q, &HashMap::from([("id".into(), GqlValue::Int(1))])).unwrap();
}

#[test]
fn relationship_pattern_selects_only_matching_mutation_targets() {
    let mut graph = Graph::new();
    graph.enable_mvcc();
    let a = graph.add_node("Item", props! {}).unwrap();
    let selected = graph.add_node("Item", props! {}).unwrap();
    let other = graph.add_node("Item", props! {}).unwrap();
    graph
        .add_edge("LINK", a, selected, props! {"id" => 1i64})
        .unwrap();
    graph
        .add_edge("LINK", a, other, props! {"id" => 2i64})
        .unwrap();
    let graph = Arc::new(RwLock::new(graph));
    let accessor = DefaultGraphAccessor::new(graph.clone());
    let GqlStatement::Mutation(m) = bound(
        "MATCH (a)-[r:LINK {id:$id}]->(b) SET b.changed = true",
        GqlValue::Int(1),
    ) else {
        panic!()
    };
    let txn = accessor.begin_txn().unwrap();
    accessor
        .execute_mutation_in_txn(txn, &m, HashMap::new(), None)
        .unwrap();
    accessor.commit_txn(txn).unwrap();
    let g = graph.read().unwrap();
    assert!(
        g.node(selected)
            .unwrap()
            .properties()
            .contains_key("changed")
    );
    assert!(!g.node(other).unwrap().properties().contains_key("changed"));
}
