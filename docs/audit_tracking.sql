ALTER TABLE `v8claimextract`
ADD `InsertDate` DATETIME NULL DEFAULT NULL,
ADD `UpdateDate` DATETIME NULL DEFAULT NULL  AFTER `InsertDate`,
ADD `InsertAuditKey` INT NULL DEFAULT NULL  AFTER `UpdateDate`,
ADD `UpdateAuditKey` INT NULL DEFAULT NULL  AFTER `InsertAuditKey`;
