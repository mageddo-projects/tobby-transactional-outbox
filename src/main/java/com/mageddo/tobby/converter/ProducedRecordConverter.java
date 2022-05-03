package com.mageddo.tobby.converter;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import com.mageddo.tobby.ProducedRecord;
import com.mageddo.tobby.ProducedRecord.Status;
import com.mageddo.tobby.ProducerRecord;
import com.mageddo.tobby.UncheckedSQLException;
import com.mageddo.tobby.internal.utils.Base64;

public class ProducedRecordConverter {

  private ProducedRecordConverter() {
  }

  public static ProducedRecord map(ResultSet rs) {
    try {
      return ProducedRecord.builder()
          .key(getBytesFromBase64(rs, "TXT_KEY"))
          .value(getBytesFromBase64(rs, "TXT_VALUE"))
          .topic(rs.getString("NAM_TOPIC"))
          .createdAt(rs.getTimestamp("DAT_CREATED")
              .toLocalDateTime())
          .id(UUID.fromString(rs.getString("IDT_TTO_RECORD")))
          .partition(getInteger(rs, "NUM_PARTITION"))
          .headers(HeadersConverter.decodeFromBase64(rs.getString("TXT_HEADERS")))
          .status(mapStatus(rs))
          .sentPartition(getInteger(rs, "NUM_SENT_PARTITION"))
          .sentOffset(getLong(rs, "NUM_SENT_OFFSET"))
          .build();
    } catch (SQLException e) {
      throw new UncheckedSQLException(e);
    }
  }

  private static Status mapStatus(ResultSet rs) throws SQLException {
    final String status = rs.getString("IND_STATUS");
    if (rs.wasNull()) {
      return null;
    }
    return Status.valueOf(status);
  }

  private static Integer getInteger(ResultSet rs, String column) throws SQLException {
    final int v = rs.getInt(column);
    return rs.wasNull() ? null : v;
  }

  private static Long getLong(ResultSet rs, String column) throws SQLException {
    final long v = rs.getLong(column);
    return rs.wasNull() ? null : v;
  }

  private static byte[] getBytesFromBase64(ResultSet rs, String name) throws SQLException {
    final String base64 = rs.getString(name);
    return Base64.decode(base64);
  }

  public static ProducedRecord from(ProducerRecord record) {
    return ProducedRecord.builder()
        .id(record.getId())
        .topic(record.getTopic())
        .partition(record.getPartition())
        .key(record.getKey())
        .value(record.getValue())
        .headers(record.getHeaders())
        .createdAt(record.getCreatedAt())
        .build()
        ;
  }

  public static List<UUID> toIds(List<ProducedRecord> records) {
    return records.stream()
        .map(ProducedRecord::getId)
        .collect(Collectors.toList())
        ;
  }
}
