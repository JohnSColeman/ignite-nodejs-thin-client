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
import { IgniteClientError } from './Errors';

/**
 * Transaction concurrency modes.
 * @typedef TransactionConcurrency
 * @enum
 * @readonly
 * @property OPTIMISTIC 0 - Optimistic concurrency (conflict detection at commit time).
 * @property PESSIMISTIC 1 - Pessimistic concurrency (locks acquired during operation).
 */
export const TransactionConcurrency = Object.freeze({
    OPTIMISTIC: 0,
    PESSIMISTIC: 1
});

/**
 * Transaction isolation levels.
 * @typedef TransactionIsolation
 * @enum
 * @readonly
 * @property READ_COMMITTED 0
 * @property REPEATABLE_READ 1
 * @property SERIALIZABLE 2
 */
export const TransactionIsolation = Object.freeze({
    READ_COMMITTED: 0,
    REPEATABLE_READ: 1,
    SERIALIZABLE: 2
});

/**
 * Represents an active thin-client transaction.
 *
 * Obtain an instance via {@link IgniteClient#beginTransaction}.
 * Associate cache operations with this transaction via {@link CacheClient#withTransaction}.
 * Finalize with {@link Transaction#commit} or {@link Transaction#rollback}.
 *
 * @hideconstructor
 */
export class Transaction {
    private _txId: number;
    private _communicator: BinaryCommunicator;
    private _ended: boolean;

    constructor(txId: number, communicator: BinaryCommunicator) {
        this._txId = txId;
        this._communicator = communicator;
        this._ended = false;
    }

    /** @ignore */
    get txId(): number {
        return this._txId;
    }

    /**
     * Commits the transaction, persisting all changes made within it.
     *
     * @throws {IgniteClientError} if the transaction has already been committed or rolled back.
     */
    async commit(): Promise<void> {
        await this._end(true);
    }

    /**
     * Rolls back the transaction, discarding all changes made within it.
     *
     * @throws {IgniteClientError} if the transaction has already been committed or rolled back.
     */
    async rollback(): Promise<void> {
        await this._end(false);
    }

    private async _end(commit: boolean): Promise<void> {
        if (this._ended) {
            throw IgniteClientError.illegalArgumentError('Transaction has already been committed or rolled back.');
        }
        this._ended = true;
        await this._communicator.send(
            BinaryUtils.OPERATION.TRANSACTION_END,
            (payload) => {
                payload.writeInteger(this._txId);
                payload.writeBoolean(commit);
            }
        );
    }
}
