const neo4j = require('neo4j-driver');

const uri = 'bolt+s://db-e76ecb2a.databases.cognodb.com';
const user = 'cognodb';
const password = '436ce6fa7613033fdf91c7736b471767';

async function seed() {
    console.log('Connecting to CognoDB Cloud at', uri, '...');
    const driver = neo4j.driver(uri, neo4j.auth.basic(user, password));
    const session = driver.session();

    try {
        await driver.verifyConnectivity();
        console.log('Connected successfully!');

        console.log('1. Creating Person nodes...');
        await session.run(`
            UNWIND range(1, 150) AS id
            MERGE (p:Person {uid: id, name: 'User_' + toString(id), age: 20 + (id % 40), bucket: id % 10})
        `);
        console.log('Created 150 Person nodes.');

        console.log('2. Creating FOLLOWS relationships...');
        await session.run(`
            UNWIND range(1, 150) AS id
            MATCH (a:Person {uid: id}), (b:Person {uid: ((id * 7) % 150) + 1})
            WHERE a <> b
            MERGE (a)-[r:FOLLOWS {weight: (id % 5) + 1, since: 2020 + (id % 4)}]->(b)
        `);

        console.log('3. Creating FRIENDS_WITH relationships...');
        await session.run(`
            UNWIND range(1, 150) AS id
            MATCH (a:Person {uid: id}), (b:Person {uid: ((id * 13) % 150) + 1})
            WHERE a <> b
            MERGE (a)-[r:FRIENDS_WITH {strength: (id % 10)}]->(b)
        `);

        console.log('4. Creating INTERACTED_WITH relationships...');
        await session.run(`
            UNWIND range(1, 150) AS id
            MATCH (a:Person {uid: id}), (b:Person {uid: ((id * 31) % 150) + 1})
            WHERE a <> b
            MERGE (a)-[r:INTERACTED_WITH {count: (id % 20) + 1}]->(b)
        `);

        const countNodes = await session.run('MATCH (n) RETURN count(n) AS nodeCount');
        const countRels = await session.run('MATCH ()-[r]->() RETURN count(r) AS relCount');

        const n = countNodes.records[0].get('nodeCount').toNumber ? countNodes.records[0].get('nodeCount').toNumber() : countNodes.records[0].get('nodeCount');
        const r = countRels.records[0].get('relCount').toNumber ? countRels.records[0].get('relCount').toNumber() : countRels.records[0].get('relCount');

        console.log('==================================================');
        console.log(` SUCCESS! CognoDB Cloud Graph Populated:`);
        console.log(` Nodes: ${n}`);
        console.log(` Relationships: ${r}`);
        console.log('==================================================');
    } catch (err) {
        console.error('Error seeding CognoDB:', err);
    } finally {
        await session.close();
        await driver.close();
    }
}

seed();
