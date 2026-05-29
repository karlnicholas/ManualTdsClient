package com.example;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;
import java.util.function.BiFunction;

public class AllDataTypesMapper {
  public static final BiFunction<Row, RowMetadata, AllDataTypesRecord> allDataTypesMapper = (row, meta) -> new AllDataTypesRecord(
      row.get(0, Integer.class),
      row.get(1, Boolean.class),
      row.get(2, Byte.class),
      row.get(3, Short.class),
      row.get(4, Integer.class),
      row.get(5, Long.class),
      row.get(6, BigDecimal.class),
      row.get(7, BigDecimal.class),
      row.get(8, BigDecimal.class),
      row.get(9, Float.class),
      row.get(10, Double.class),
      row.get(11, LocalDate.class),
      row.get(12, LocalTime.class),
      row.get(13, LocalDateTime.class),
      row.get(14, LocalDateTime.class),
      row.get(15, LocalDateTime.class),
      row.get(16, String.class),
      row.get(17, String.class),
      row.get(18, String.class),
      row.get(19, String.class),
      row.get(20, String.class),
      row.get(21, String.class),
      row.get(22, String.class),
      row.get(23, byte[].class),
      row.get(24, byte[].class),
      row.get(25, byte[].class),
      row.get(26, byte[].class),
      UUID.fromString(row.get(27, String.class))
  );

}
