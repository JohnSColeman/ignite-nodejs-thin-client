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

import BinaryUtils from './internal/BinaryUtils';
import BinaryCommunicator from './internal/BinaryCommunicator';
import MessageBuffer from './internal/MessageBuffer';

/**
 * Cache entry event types delivered to a continuous query listener.
 * @typedef CacheEntryEventType
 * @enum
 * @readonly
 * @property CREATED 0 - A new entry was added to the cache.
 * @property UPDATED 1 - An existing entry was updated.
 * @property REMOVED 2 - An entry was removed from the cache.
 * @property EXPIRED 3 - An entry expired (requires includeExpired=true on the query).
 */
export const CacheEntryEventType = Object.freeze({
    CREATED: 0,
    UPDATED: 1,
    REMOVED: 2,
    EXPIRED: 3
});

/**
 * A single cache-entry change event delivered by a continuous query.
 */
export class CacheEntryEvent {
    private _key: any;
    private _oldValue: any;
    private _value: any;
    private _eventType: number;

    constructor(key: any, oldValue: any, value: any, eventType: number) {
        this._key = key;
        this._oldValue = oldValue;
        this._value = value;
        this._eventType = eventType;
    }

    /** Returns the cache key associated with this event. */
    getKey(): any { return this._key; }

    /** Returns the new (current) value, or null for REMOVED/EXPIRED events. */
    getValue(): any { return this._value; }

    /** Returns the old value before the change, or null for CREATED events. */
    getOldValue(): any { return this._oldValue; }

    /** Returns the event type — one of the {@link CacheEntryEventType} values. */
    getEventType(): number { return this._eventType; }
}

/**
 * Configuration for a continuous query.
 *
 * Create an instance, configure it with the fluent setters, and pass it to
 * {@link CacheClient#queryContinuous}.
 */
export class ContinuousQuery {
    private _localListener: ((events: CacheEntryEvent[]) => void) | null;
    private _bufferSize: number;
    private _timeInterval: number;
    private _includeExpired: boolean;

    constructor() {
        this._localListener = null;
        this._bufferSize = 1;
        this._timeInterval = 0;
        this._includeExpired = false;
    }

    /**
     * Sets the local listener that will be called each time the server sends a batch
     * of cache-entry events.
     *
     * @param {function} listener - callback receiving an array of {@link CacheEntryEvent}.
     * @return {ContinuousQuery} this instance (for chaining).
     */
    setLocalListener(listener: (events: CacheEntryEvent[]) => void): ContinuousQuery {
        this._localListener = listener;
        return this;
    }

    /**
     * Sets the number of events the server buffers before sending them to the client.
     * Default is 1 (send immediately on every change).
     *
     * @param {number} bufferSize
     * @return {ContinuousQuery} this instance (for chaining).
     */
    setBufferSize(bufferSize: number): ContinuousQuery {
        this._bufferSize = bufferSize;
        return this;
    }

    /**
     * Sets the maximum delay in milliseconds between event deliveries.
     * Default is 0 (no delay).
     *
     * @param {number} timeInterval - delay in milliseconds.
     * @return {ContinuousQuery} this instance (for chaining).
     */
    setTimeInterval(timeInterval: number): ContinuousQuery {
        this._timeInterval = timeInterval;
        return this;
    }

    /**
     * When set to true, the listener will also receive EXPIRED events for entries
     * that expire due to expiry policy.
     * Default is false.
     *
     * @param {boolean} includeExpired
     * @return {ContinuousQuery} this instance (for chaining).
     */
    setIncludeExpired(includeExpired: boolean): ContinuousQuery {
        this._includeExpired = includeExpired;
        return this;
    }

    /** @ignore */
    async _start(communicator: BinaryCommunicator, cacheId: number): Promise<ContinuousQueryHandle> {
        const handle = new ContinuousQueryHandle(communicator);
        const listener = this._localListener;

        await communicator.send(
            BinaryUtils.OPERATION.QUERY_CONTINUOUS,
            (payload) => {
                payload.writeInteger(cacheId);
                payload.writeByte(0);                    // flags
                payload.writeInteger(this._bufferSize);
                payload.writeLong(this._timeInterval);
                payload.writeBoolean(this._includeExpired);
                payload.writeByte(BinaryUtils.TYPE_CODE.NULL); // no server filter
            },
            async (payload) => {
                const handleId: string = payload.readLong().toString();
                handle._setHandleId(handleId);

                if (listener) {
                    communicator.registerContinuousQueryListener(
                        handleId,
                        async (buffer: MessageBuffer) => {
                            const events = await ContinuousQuery._readEvents(communicator, buffer);
                            listener(events);
                        }
                    );
                }
            }
        );

        return handle;
    }

    /** @ignore */
    private static async _readEvents(
        communicator: BinaryCommunicator,
        buffer: MessageBuffer
    ): Promise<CacheEntryEvent[]> {
        const count = buffer.readInteger();
        const events: CacheEntryEvent[] = new Array(count);
        for (let i = 0; i < count; i++) {
            const key = await communicator.readObject(buffer);
            const oldValue = await communicator.readObject(buffer);
            const value = await communicator.readObject(buffer);
            const eventType = buffer.readByte();
            events[i] = new CacheEntryEvent(key, oldValue, value, eventType);
        }
        return events;
    }
}

/**
 * A handle to an active continuous query. Used to stop the query.
 *
 * Obtained from {@link CacheClient#queryContinuous}.
 *
 * @hideconstructor
 */
export class ContinuousQueryHandle {
    private _communicator: BinaryCommunicator;
    private _handleId: string | null;
    private _closed: boolean;

    constructor(communicator: BinaryCommunicator) {
        this._communicator = communicator;
        this._handleId = null;
        this._closed = false;
    }

    /** @ignore */
    _setHandleId(handleId: string): void {
        this._handleId = handleId;
    }

    /**
     * Stops the continuous query and releases server-side resources.
     *
     * Calling close() more than once is a no-op.
     *
     * @async
     */
    async close(): Promise<void> {
        if (this._closed || this._handleId === null) {
            return;
        }
        this._closed = true;
        this._communicator.unregisterContinuousQueryListener(this._handleId);
        const handleId = this._handleId;
        await this._communicator.send(
            BinaryUtils.OPERATION.RESOURCE_CLOSE,
            (payload) => {
                // RESOURCE_CLOSE expects a requestId written as the cursor ID (Long)
                payload.writeLong(handleId);
            }
        );
    }
}
