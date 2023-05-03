package io.kestra.plugin.astradb;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.GettableByIndex;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.protocol.internal.ProtocolConstants;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;

public class AstraHelper {
    public static Map<String, Object> convertRow(Row row, ColumnDefinitions columnDefinitions) {
        Map<String, Object> map = new LinkedHashMap<>();

        for (int index = 0; index < columnDefinitions.size(); index++) {
            ColumnDefinition columnDefinition = columnDefinitions.get(index);

            map.put(
                    columnDefinition.getName().asInternal(),
                    convertCell(columnDefinition, row, index)
            );
        }

        return map;
    }

    public static Object convertCell(ColumnDefinition columnDefinition, GettableByIndex row, int index) {
        switch (columnDefinition.getType().getProtocolCode()) {
            case ProtocolConstants.DataType.COUNTER:
            case ProtocolConstants.DataType.BIGINT:
                return row.getLong(index);

            case ProtocolConstants.DataType.BLOB:
                ByteBuffer bytes = row.getBytesUnsafe(index);
                return bytes == null ? null : bytes.array();

            case ProtocolConstants.DataType.BOOLEAN:
                return row.getBoolean(index);

            case ProtocolConstants.DataType.DECIMAL:
                return row.getBigDecimal(index);

            case ProtocolConstants.DataType.DOUBLE:
                return row.getDouble(index);

            case ProtocolConstants.DataType.FLOAT:
                return row.getFloat(index);

            case ProtocolConstants.DataType.SMALLINT:
                return row.getShort(index);

            case ProtocolConstants.DataType.TINYINT:
                return row.getByte(index);

            case ProtocolConstants.DataType.INT:
                return row.getInt(index);

            case ProtocolConstants.DataType.VARINT:
                return row.getBigInteger(index);

            case ProtocolConstants.DataType.TIMESTAMP:
                return row.getInstant(index);

            case ProtocolConstants.DataType.TIMEUUID:
            case ProtocolConstants.DataType.UUID:
                UUID uuid = row.getUuid(index);
                return uuid == null ? null : uuid.toString();

            case ProtocolConstants.DataType.ASCII:
            case ProtocolConstants.DataType.VARCHAR:
                return row.getString(index);

            case ProtocolConstants.DataType.INET:
                InetAddress inetAddress = row.getInetAddress(index);
                return inetAddress == null ? null : inetAddress.toString();

            case ProtocolConstants.DataType.DATE:
                return row.getLocalDate(index);

            case ProtocolConstants.DataType.TIME:
                return row.getLocalTime(index);

            case ProtocolConstants.DataType.DURATION:
                CqlDuration cqlDuration = row.getCqlDuration(index);
                return cqlDuration == null ? null : Duration.ofNanos(cqlDuration.getNanoseconds());

            case ProtocolConstants.DataType.LIST:
            case ProtocolConstants.DataType.MAP:
            case ProtocolConstants.DataType.SET:
                return row.getObject(index);

            case ProtocolConstants.DataType.TUPLE:
                TupleValue tupleValue = row.getTupleValue(index);

                if (tupleValue == null) {
                    return null;
                }

                List<Object> list = new ArrayList<>();
                for (int i = 0; i < tupleValue.size(); i++) {
                    list.add(tupleValue.getObject(i));
                }
                return list;

            case ProtocolConstants.DataType.CUSTOM:
            case ProtocolConstants.DataType.UDT:
        }

        throw new IllegalArgumentException("Invalid datatype '" + columnDefinition.getType() + '"');
    }
}
