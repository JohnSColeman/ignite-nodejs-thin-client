/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

require('jasmine-expect');

const path = require('path');
const TestingHelper = require('./TestingHelper');
const {IgniteClient, CacheConfiguration, Transaction, TransactionConcurrency, TransactionIsolation} = require('apache-ignite-client');

// Use a standalone config that has no dependency on JavaThinCompatibilityTest.CustomAffinity
const FEATURE_TEST_CONFIG = path.join(__dirname, 'configs', 'ignite-config-feature-tests.xml');
const _origGetConfigPath = TestingHelper.getConfigPath.bind(TestingHelper);
TestingHelper.getConfigPath = () => FEATURE_TEST_CONFIG;

const CACHE_NAME = '__test_transactions_cache';

describe('transactions test suite >', () => {
    let igniteClient = null;

    beforeAll((done) => {
        Promise.resolve().
            then(async () => {
                await TestingHelper.init();
                igniteClient = TestingHelper.igniteClient;
                await igniteClient.destroyCache(CACHE_NAME).catch(() => {});
                // Cache must be TRANSACTIONAL for transactions to work
                const cfg = new CacheConfiguration().setAtomicityMode(CacheConfiguration.CACHE_ATOMICITY_MODE.TRANSACTIONAL);
                await igniteClient.getOrCreateCache(CACHE_NAME, cfg);
            }).
            then(done).
            catch(error => done.fail(error));
    }, TestingHelper.TIMEOUT);

    afterAll((done) => {
        Promise.resolve().
            then(async () => {
                await igniteClient.destroyCache(CACHE_NAME).catch(() => {});
                await TestingHelper.cleanUp();
            }).
            then(done).
            catch(error => done());
    }, TestingHelper.TIMEOUT);

    afterEach((done) => {
        Promise.resolve().
            then(async () => {
                await igniteClient.getCache(CACHE_NAME).removeAll();
            }).
            then(done).
            catch(error => done.fail(error));
    }, TestingHelper.TIMEOUT);

    it('beginTransaction returns a Transaction object', (done) => {
        Promise.resolve().
            then(async () => {
                const tx = await igniteClient.beginTransaction();
                expect(tx).toBeDefined();
                expect(tx).toBeInstanceOf(Transaction);
                await tx.rollback();
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('commit persists the value', (done) => {
        Promise.resolve().
            then(async () => {
                const cache = igniteClient.getCache(CACHE_NAME);
                const tx = await igniteClient.beginTransaction();
                await cache.withTransaction(tx).put(1, 'committed-value');
                await tx.commit();
                const result = await cache.get(1);
                expect(result).toBe('committed-value');
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('rollback discards the value', (done) => {
        Promise.resolve().
            then(async () => {
                const cache = igniteClient.getCache(CACHE_NAME);
                const tx = await igniteClient.beginTransaction();
                await cache.withTransaction(tx).put(2, 'rolled-back-value');
                await tx.rollback();
                const result = await cache.get(2);
                expect(result).toBeNull();
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('commit twice throws', (done) => {
        Promise.resolve().
            then(async () => {
                const tx = await igniteClient.beginTransaction();
                await tx.commit();
                let threw = false;
                try {
                    await tx.commit();
                } catch (e) {
                    threw = true;
                }
                expect(threw).toBe(true);
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('rollback after commit throws', (done) => {
        Promise.resolve().
            then(async () => {
                const tx = await igniteClient.beginTransaction();
                await tx.commit();
                let threw = false;
                try {
                    await tx.rollback();
                } catch (e) {
                    threw = true;
                }
                expect(threw).toBe(true);
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('concurrent independent transactions do not interfere', (done) => {
        Promise.resolve().
            then(async () => {
                const cache = igniteClient.getCache(CACHE_NAME);
                const tx1 = await igniteClient.beginTransaction();
                const tx2 = await igniteClient.beginTransaction();

                await cache.withTransaction(tx1).put(10, 'tx1-value');
                await cache.withTransaction(tx2).put(11, 'tx2-value');

                // tx1 commits, tx2 rolls back
                await tx1.commit();
                await tx2.rollback();

                expect(await cache.get(10)).toBe('tx1-value');
                expect(await cache.get(11)).toBeNull();
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('withTransaction does not mutate original cache client', (done) => {
        Promise.resolve().
            then(async () => {
                const cache = igniteClient.getCache(CACHE_NAME);
                const tx = await igniteClient.beginTransaction();
                const txCache = cache.withTransaction(tx);
                // txCache is a different object
                expect(txCache).not.toBe(cache);
                await tx.rollback();
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('PESSIMISTIC REPEATABLE_READ transaction (default modes)', (done) => {
        Promise.resolve().
            then(async () => {
                const cache = igniteClient.getCache(CACHE_NAME);
                const tx = await igniteClient.beginTransaction(
                    TransactionConcurrency.PESSIMISTIC,
                    TransactionIsolation.REPEATABLE_READ
                );
                await cache.withTransaction(tx).put(20, 'pessimistic-value');
                await tx.commit();
                expect(await cache.get(20)).toBe('pessimistic-value');
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('OPTIMISTIC READ_COMMITTED transaction', (done) => {
        Promise.resolve().
            then(async () => {
                const cache = igniteClient.getCache(CACHE_NAME);
                const tx = await igniteClient.beginTransaction(
                    TransactionConcurrency.OPTIMISTIC,
                    TransactionIsolation.READ_COMMITTED
                );
                await cache.withTransaction(tx).put(21, 'optimistic-value');
                await tx.commit();
                expect(await cache.get(21)).toBe('optimistic-value');
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('transaction with a label', (done) => {
        Promise.resolve().
            then(async () => {
                const cache = igniteClient.getCache(CACHE_NAME);
                const tx = await igniteClient.beginTransaction(
                    TransactionConcurrency.PESSIMISTIC,
                    TransactionIsolation.REPEATABLE_READ,
                    0,
                    'my-label'
                );
                await cache.withTransaction(tx).put(22, 'labeled-value');
                await tx.commit();
                expect(await cache.get(22)).toBe('labeled-value');
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('TransactionConcurrency enum is exported', (done) => {
        Promise.resolve().
            then(async () => {
                expect(TransactionConcurrency).toBeDefined();
                expect(TransactionConcurrency.OPTIMISTIC).toBe(0);
                expect(TransactionConcurrency.PESSIMISTIC).toBe(1);
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('TransactionIsolation enum is exported', (done) => {
        Promise.resolve().
            then(async () => {
                expect(TransactionIsolation).toBeDefined();
                expect(TransactionIsolation.READ_COMMITTED).toBe(0);
                expect(TransactionIsolation.REPEATABLE_READ).toBe(1);
                expect(TransactionIsolation.SERIALIZABLE).toBe(2);
            }).
            then(done).
            catch(error => done.fail(error));
    });
});
