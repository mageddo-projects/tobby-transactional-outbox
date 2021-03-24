package com.mageddo.tobby.converter;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.mageddo.tobby.ProducedRecord;
import com.mageddo.tobby.internal.utils.Base64;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class RecordProcessedMapper {
  public static void map(PreparedStatement stm, ProducedRecord record) throws SQLException {
    stm.setString(1, record.getId().toString());
    stm.setString(2, record.getTopic());
    stm.setObject(3, record.getPartition());
    stm.setString(4, Base64.encodeToString(record.getKey()));
    stm.setString(5, Base64.encodeToString(record.getValue()));
    stm.setString(6, HeadersConverter.encodeBase64(record.getHeaders()));  }
}
