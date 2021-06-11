/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.cstore;

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.ErrorType;

import static com.facebook.presto.spi.ErrorType.EXTERNAL;

public enum CStoreErrorCode
        implements ErrorCodeSupplier
{
    CSTORE_ERROR(0, EXTERNAL),
    CSTORE_EXTERNAL_BATCH_ALREADY_EXISTS(1, EXTERNAL),
    CSTORE_NO_HOST_FOR_SHARD(2, EXTERNAL),
    CSTORE_RECOVERY_ERROR(3, EXTERNAL),
    CSTORE_BACKUP_TIMEOUT(4, EXTERNAL),
    CSTORE_METADATA_ERROR(5, EXTERNAL),
    CSTORE_BACKUP_ERROR(6, EXTERNAL),
    CSTORE_BACKUP_NOT_FOUND(7, EXTERNAL),
    CSTORE_REASSIGNMENT_DELAY(8, EXTERNAL),
    CSTORE_REASSIGNMENT_THROTTLE(9, EXTERNAL),
    CSTORE_RECOVERY_TIMEOUT(10, EXTERNAL),
    CSTORE_CORRUPT_METADATA(11, EXTERNAL),
    CSTORE_LOCAL_DISK_FULL(12, EXTERNAL),
    CSTORE_BACKUP_CORRUPTION(13, EXTERNAL),
    CSTORE_NOT_ENOUGH_NODES(14, EXTERNAL),
    CSTORE_WRITER_DATA_ERROR(15, EXTERNAL),
    CSTORE_UNSUPPORTED_COMPRESSION_KIND(16, EXTERNAL),
    CSTORE_FILE_SYSTEM_ERROR(17, EXTERNAL),
    CSTORE_TOO_MANY_FILES_CREATED(18, EXTERNAL),
    CSTORE_PUSHDOWN_UNSUPPORTED_EXPRESSION(4, EXTERNAL);

    private final ErrorCode errorCode;

    CStoreErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0506_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
