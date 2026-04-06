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
const {IgniteClient, CacheConfiguration, ContinuousQuery, ContinuousQueryHandle, CacheEntryEventType} = require('apache-ignite-client');

// Use a standalone config that has no dependency on JavaThinCompatibilityTest.CustomAffinity
const FEATURE_TEST_CONFIG = path.join(__dirname, 'configs', 'ignite-config-feature-tests.xml');
TestingHelper.getConfigPath = () => FEATURE_TEST_CONFIG;

const CACHE_NAME = '__test_continuous_query_cache';
const EVENT_WAIT_MS = 2000;

// Waits up to maxMs for predicate() to return true, polling every 50 ms.
async function waitFor(predicate, maxMs = EVENT_WAIT_MS) {
    const deadline = Date.now() + maxMs;
    while (Date.now() < deadline) {
        if (predicate()) return;
        await new Promise(resolve => setTimeout(resolve, 50));
    }
    throw new Error('Timed out waiting for condition');
}

describe('continuous query test suite >', () => {
    let igniteClient = null;

    beforeAll((done) => {
        Promise.resolve().
            then(async () => {
                await TestingHelper.init();
                igniteClient = TestingHelper.igniteClient;
                await igniteClient.destroyCache(CACHE_NAME).catch(() => {});
                await igniteClient.getOrCreateCache(CACHE_NAME);
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

    it('ContinuousQuery and CacheEntryEventType are exported', (done) => {
        Promise.resolve().
            then(async () => {
                expect(ContinuousQuery).toBeDefined();
                expect(ContinuousQueryHandle).toBeDefined();
                expect(CacheEntryEventType).toBeDefined();
                expect(CacheEntryEventType.CREATED).toBe(0);
                expect(CacheEntryEventType.UPDATED).toBe(1);
                expect(CacheEntryEventType.REMOVED).toBe(2);
                expect(CacheEntryEventType.EXPIRED).toBe(3);
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('queryContinuous returns a ContinuousQueryHandle', (done) => {
        Promise.resolve().
            then(async () => {
                const cache = igniteClient.getCache(CACHE_NAME);
                const query = new ContinuousQuery().setLocalListener(() => {});
                const handle = await cache.queryContinuous(query);
                expect(handle).toBeDefined();
                expect(handle).toBeInstanceOf(ContinuousQueryHandle);
                await handle.close();
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('listener receives CREATED event on put', (done) => {
        Promise.resolve().
            then(async () => {
                const cache = igniteClient.getCache(CACHE_NAME);
                const events = [];
                const query = new ContinuousQuery().setLocalListener((evts) => events.push(...evts));
                const handle = await cache.queryContinuous(query);

                await cache.put(100, 'hello');
                await waitFor(() => events.length >= 1);

                expect(events[0].getEventType()).toBe(CacheEntryEventType.CREATED);
                expect(events[0].getKey()).toBe(100);
                expect(events[0].getValue()).toBe('hello');

                await handle.close();
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('listener receives UPDATED event on second put for same key', (done) => {
        Promise.resolve().
            then(async () => {
                const cache = igniteClient.getCache(CACHE_NAME);
                const events = [];
                const query = new ContinuousQuery().setLocalListener((evts) => events.push(...evts));
                const handle = await cache.queryContinuous(query);

                await cache.put(200, 'first');
                await cache.put(200, 'second');
                await waitFor(() => events.length >= 2);

                const types = events.map(e => e.getEventType());
                expect(types).toContain(CacheEntryEventType.CREATED);
                expect(types).toContain(CacheEntryEventType.UPDATED);

                await handle.close();
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('listener receives REMOVED event on removeKey', (done) => {
        Promise.resolve().
            then(async () => {
                const cache = igniteClient.getCache(CACHE_NAME);
                await cache.put(300, 'to-remove');

                const events = [];
                const query = new ContinuousQuery().setLocalListener((evts) => events.push(...evts));
                const handle = await cache.queryContinuous(query);

                await cache.removeKey(300);
                await waitFor(() => events.some(e => e.getEventType() === CacheEntryEventType.REMOVED));

                const removed = events.find(e => e.getEventType() === CacheEntryEventType.REMOVED);
                expect(removed).toBeDefined();
                expect(removed.getKey()).toBe(300);

                await handle.close();
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('no events received after handle.close()', (done) => {
        Promise.resolve().
            then(async () => {
                const cache = igniteClient.getCache(CACHE_NAME);
                const events = [];
                const query = new ContinuousQuery().setLocalListener((evts) => events.push(...evts));
                const handle = await cache.queryContinuous(query);

                // Confirm it works before close
                await cache.put(400, 'before-close');
                await waitFor(() => events.length >= 1);

                await handle.close();
                const countAfterClose = events.length;

                // Events after close should not be delivered
                await cache.put(401, 'after-close');
                await TestingHelper.sleep(500);

                expect(events.length).toBe(countAfterClose);
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('close twice does not throw', (done) => {
        Promise.resolve().
            then(async () => {
                const cache = igniteClient.getCache(CACHE_NAME);
                const query = new ContinuousQuery().setLocalListener(() => {});
                const handle = await cache.queryContinuous(query);
                await handle.close();
                // Second close should be a no-op, not throw
                await handle.close();
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('bufferSize and timeInterval options are accepted', (done) => {
        Promise.resolve().
            then(async () => {
                const cache = igniteClient.getCache(CACHE_NAME);
                const events = [];
                const query = new ContinuousQuery()
                    .setLocalListener((evts) => events.push(...evts))
                    .setBufferSize(10)
                    .setTimeInterval(100);
                const handle = await cache.queryContinuous(query);
                await cache.put(500, 'buffered');
                await waitFor(() => events.length >= 1);
                expect(events.length).toBeGreaterThanOrEqualTo(1);
                await handle.close();
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('multiple independent continuous queries on same cache', (done) => {
        Promise.resolve().
            then(async () => {
                const cache = igniteClient.getCache(CACHE_NAME);
                const events1 = [];
                const events2 = [];
                const handle1 = await cache.queryContinuous(
                    new ContinuousQuery().setLocalListener((evts) => events1.push(...evts))
                );
                const handle2 = await cache.queryContinuous(
                    new ContinuousQuery().setLocalListener((evts) => events2.push(...evts))
                );

                await cache.put(600, 'multi');
                await waitFor(() => events1.length >= 1 && events2.length >= 1);

                expect(events1.length).toBeGreaterThanOrEqualTo(1);
                expect(events2.length).toBeGreaterThanOrEqualTo(1);

                await handle1.close();
                await handle2.close();
            }).
            then(done).
            catch(error => done.fail(error));
    });
});
