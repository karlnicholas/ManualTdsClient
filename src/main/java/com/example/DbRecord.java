package com.example;

import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 *                 CREATE TABLE dbo.users (
 *                     id          BIGINT          IDENTITY(1,1)   NOT NULL,
 *                     firstName   NVARCHAR(100)   NULL,
 *                     lastName    NVARCHAR(100)   NULL,
 *                     email       NVARCHAR(254)   NOT NULL,
 *                     dateJoined  DATE            NULL            DEFAULT CAST(GETDATE() AS DATE),
 *                     postCount   BIGINT          NULL            DEFAULT 0,
 *                     createdAt   DATETIME2(3)    NOT NULL        DEFAULT SYSUTCDATETIME(),
 *                     updatedAt   DATETIME2(3)    NULL,
 *
 *                     CONSTRAINT PK_users         PRIMARY KEY     (id),
 *                     CONSTRAINT UIX_users_email  UNIQUE          (email)
 *                 );
 * @param id
 * @param firstName
 * @param lastName
 * @param email
 * @param dateJoined
 * @param postCount
 * @param createdAt
 * @param updatedAt
 */
public record DbRecord(Long id, String firstName, String lastName, String email, LocalDate dateJoined, Long postCount, LocalDateTime createdAt, LocalDateTime updatedAt) {
}
