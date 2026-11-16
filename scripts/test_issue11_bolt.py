"""Pattern parameter regressions over the official Bolt driver."""
import sys
import uuid
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError
from test_issue14_bolt import main


def probe(uri):
    label = 'Issue11' + uuid.uuid4().hex
    report = {}
    with GraphDatabase.driver(uri, auth=None, max_transaction_retry_time=0) as driver:
        with driver.session(database='neo4j') as session:
            def run(query, **params):
                return [r.data() for r in session.run(query, params)]
            for key in [1, 2]:
                run(f'CREATE (n:{label} {{id:$id, value:0}})', id=key)
            report['node_read'] = {'pass': run(f'MATCH (n:{label} {{id:$id}}) RETURN n.id AS id', id=2) == [{'id':2}]}
            run(f'MATCH (n:{label} {{id:$id}}) SET n.value = $value', id=1, value=10)
            with session.begin_transaction() as tx:
                tx.run(f'MATCH (n:{label} {{id:$id}}) SET n.value = $value', id=2, value=20).consume()
                tx.commit()
            rows=run(f'MATCH (n:{label}) RETURN n.id AS id, n.value AS value ORDER BY n.id')
            report['targeted_mutations']={'pass': rows == [{'id':1,'value':10},{'id':2,'value':20}]}
            run(f'CREATE (n:{label} {{id:3, value:0}})')
            for key in [1, 2]:
                run(f'MATCH (a:{label} {{id:$left}}), (b:{label} {{id:$right}}) CREATE (a)-[:LINK {{id:$id, value:0}}]->(b)',left=1,right=key+1,id=key)
            with session.begin_transaction() as tx:
                tx.run(f'MATCH (a:{label})-[r:LINK {{id:$id}}]->(b) SET b.value = $value',id=2,value=30).consume()
                tx.commit()
            rows=run(f'MATCH (a:{label})-[r:LINK]->(b) RETURN r.id AS id, b.value AS value ORDER BY r.id')
            report['relationship_mutation']={'pass': rows == [{'id':1,'value':20},{'id':2,'value':30}]}
            hostile="'} ) DETACH DELETE n //"
            run(f'CREATE (n:{label} {{id:$id}})',id=hostile)
            report['no_interpolation']={'pass':run(f'MATCH (n:{label} {{id:$id}}) RETURN n.id AS id',id=hostile)==[{'id':hostile}]}
            report['null_is_not_wildcard']={'pass':run(f'MATCH (n:{label} {{id:$id}}) RETURN count(n) AS total',id=None)==[{'total':0}]}
            try:
                run(f'MATCH (n:{label} {{id:$id}}) RETURN n')
                report['missing_parameter']={'pass':False}
            except Neo4jError as error:
                report['missing_parameter']={'pass':error.code=='Neo.ClientError.Statement.ParameterMissing','code':error.code}
            run(f'MATCH (n:{label}) DETACH DELETE n')
            report['cleanup']={'pass':run(f'MATCH (n:{label}) RETURN n')==[]}
    return report


if __name__ == '__main__':
    sys.exit(main(probe_fn=probe))
