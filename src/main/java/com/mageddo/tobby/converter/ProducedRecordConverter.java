package com.mageddo.tobby.converter;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Base64;
import java.util.UUID;

import com.mageddo.tobby.Headers;
import com.mageddo.tobby.ProducedRecord;
import com.mageddo.tobby.ProducerRecord;
import com.mageddo.tobby.UncheckedSQLException;
import com.mageddo.tobby.internal.utils.StringUtils;

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
          .partition(rs.getInt("NUM_PARTITION"))
          .headers(parseHeaders(rs.getString("JSN_HEADERS")))
          .build();
    } catch (SQLException e) {
      throw new UncheckedSQLException(e);
    }
  }

  private static Headers parseHeaders(String json) {
    if(StringUtils.isBlank(json)){
      return new Headers();
    }
    throw new UnsupportedOperationException();
  }

  private static byte[] getBytesFromBase64(ResultSet rs, String name) throws SQLException {
    final String base64 = rs.getString(name);
    return Base64
        .getDecoder()
        .decode(base64);
  }

  public static ProducedRecord from(UUID id, ProducerRecord record) {
    return ProducedRecord.builder()
        .id(id)
        .topic(record.getTopic())
        .partition(record.getPartition())
        .key(record.getKey())
        .value(record.getValue())
        .headers(record.getHeaders())
        .build()
        ;
  }
}
