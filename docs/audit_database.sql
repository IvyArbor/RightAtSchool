-- --------------------------------------------------------
-- Host:                         127.0.0.1
-- Server version:               5.7.19-log - MySQL Community Server (GPL)
-- Server OS:                    Win64
-- HeidiSQL Version:             9.4.0.5125
-- --------------------------------------------------------

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET NAMES utf8 */;
/*!50503 SET NAMES utf8mb4 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;


-- Dumping database structure for ldiauditing
-- DROP DATABASE IF EXISTS `ldiauditing`;
-- CREATE DATABASE IF NOT EXISTS `ldiauditing` /*!40100 DEFAULT CHARACTER SET utf8 */;
USE `ldiauditing`;

-- Dumping structure for table ldiauditing.auditerrorlog
DROP TABLE IF EXISTS `auditwarninglog`;
CREATE TABLE IF NOT EXISTS `auditwarninglog` (
  `WarningLogKey` int(11) NOT NULL AUTO_INCREMENT,
  `TableProcessKey` bigint(20) DEFAULT NULL,
  `WarningDate` datetime DEFAULT NULL,
  `WarningExtractLine` int(11) DEFAULT NULL,
  `WarningRowContents` mediumtext,
  PRIMARY KEY (`WarningLogKey`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- Dumping structure for table ldiauditing.auditerrorlog
DROP TABLE IF EXISTS `auditerrorlog`;
CREATE TABLE IF NOT EXISTS `auditerrorlog` (
  `ErrorLogKey` int(11) NOT NULL AUTO_INCREMENT,
  `JobExecutionKey` bigint(20) DEFAULT NULL,
  `TableName` varchar(50) DEFAULT NULL,
  `UserName` varchar(128) NOT NULL,
  `ErrorDate` datetime DEFAULT NULL,
  `ErrorProcedure` varchar(126) CHARACTER SET utf8 DEFAULT NULL,
  `ErrorLine` int(11) DEFAULT NULL,
  `ErrorCode` int(11) DEFAULT NULL,
  `ErrorMessage` mediumtext,
  PRIMARY KEY (`ErrorLogKey`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- Data exporting was unselected.
-- Dumping structure for table ldiauditing.auditextractiontime
DROP TABLE IF EXISTS `auditextractiontime`;
CREATE TABLE IF NOT EXISTS `auditextractiontime` (
  `ExtractionTimeKey` int(11) NOT NULL AUTO_INCREMENT,
  `JobName` varchar(50) DEFAULT NULL,
  `ExtractStartDate` datetime NOT NULL,
  `ExtractStopDate` datetime DEFAULT NULL,
  `Comments` varchar(150) DEFAULT NULL,
  `LastUpdated` datetime DEFAULT NULL,
  PRIMARY KEY (`ExtractionTimeKey`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- Data exporting was unselected.
-- Dumping structure for table ldiauditing.auditjobexecution
DROP TABLE IF EXISTS `auditjobexecution`;
CREATE TABLE IF NOT EXISTS `auditjobexecution` (
  `JobExecutionKey` bigint(20) NOT NULL AUTO_INCREMENT,
  `JobName` varchar(50) NOT NULL,
  `ExecutionStartDate` datetime NOT NULL,
  `ExecutionStopDate` datetime DEFAULT NULL,
  `SuccessfulProcessingIndicator` char(1) DEFAULT NULL,
  `ParentJobExecutionKey` bigint(20) DEFAULT NULL,
  `ExtractionTimeKey` int(11) DEFAULT NULL,
  PRIMARY KEY (`JobExecutionKey`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- Data exporting was unselected.
-- Dumping structure for table ldiauditing.audittableprocessing
DROP TABLE IF EXISTS `audittableprocessing`;
CREATE TABLE IF NOT EXISTS `audittableprocessing` (
  `TableProcessKey` bigint(20) NOT NULL AUTO_INCREMENT,
  `JobExecutionKey` bigint(20) DEFAULT NULL,
  `SourceDatabase` varchar(50) DEFAULT NULL,
  `SourceTableName` varchar(150) DEFAULT NULL,
  `TargetDatabase` varchar(50) DEFAULT NULL,
  `TargetTableName` varchar(50) DEFAULT NULL,
  `InitialRowCount` int(11) DEFAULT NULL,
  `ExtractRowCount` int(11) DEFAULT NULL,
  `InsertRowCount` int(11) DEFAULT NULL,
  `UpdateRowCnt` int(11) DEFAULT NULL,
  `ErrorRowCount` int(11) DEFAULT NULL,
  `FinalRowCount` int(11) DEFAULT NULL,
  `SuccessfulProcessingIndicator` char(1) DEFAULT NULL,
  PRIMARY KEY (`TableProcessKey`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- Data exporting was unselected.
/*!40101 SET SQL_MODE=IFNULL(@OLD_SQL_MODE, '') */;
/*!40014 SET FOREIGN_KEY_CHECKS=IF(@OLD_FOREIGN_KEY_CHECKS IS NULL, 1, @OLD_FOREIGN_KEY_CHECKS) */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
