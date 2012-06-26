--
-- Table structure for table `tm3_tm`
--

DROP TABLE IF EXISTS `TM3_TM`;
CREATE TABLE `TM3_TM` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `type` smallint(6) NOT NULL,
  `tu_table` varchar(128) DEFAULT NULL,
  `tuv_table` varchar(128) DEFAULT NULL,
  `fuzzy_table` varchar(128) DEFAULT NULL,
  `attr_val_table` varchar(128) DEFAULT NULL,
  `srcLocaleId` bigint(20),
  `tgtLocaleId` bigint(20),
  `sharedStorageId` bigint(20),
  PRIMARY KEY (`id`),
  KEY(`sharedStorageId`)
) ENGINE=InnoDB DEFAULT CHARSET=UTF8;

--
-- Table structure for table `tm3_attr`
--

DROP TABLE IF EXISTS `TM3_ATTR`;
CREATE TABLE `TM3_ATTR` (
      `id` bigint(20) NOT NULL AUTO_INCREMENT,
      `tmId` bigint(20) NOT NULL,
      `name` varchar(128) NOT NULL,
      `columnName` varchar(32),
      `valueType` varchar(128) NOT NULL,
      `affectsIdentity` char(1) NOT NULL DEFAULT 'Y',
      PRIMARY KEY (`id`),
      UNIQUE KEY `tmId` (`tmId`,`name`),
      CONSTRAINT `tm3_attr_ibfk_1` FOREIGN KEY (`tmId`) REFERENCES `TM3_TM` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=UTF8;

--
-- Table structure for table `tm3_events`
--

DROP TABLE IF EXISTS `TM3_EVENTS`;
CREATE TABLE `TM3_EVENTS` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `time` datetime NOT NULL,
  `userName` varchar(128) NOT NULL,
  `tmId` bigint(20) NOT NULL,
  `type` smallint(6) NOT NULL,
  `arg` text DEFAULT NULL,
  PRIMARY KEY (`id`),
  CONSTRAINT `tm3_events_ibfk_1` FOREIGN KEY (`tmId`) REFERENCES `TM3_TM` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=UTF8;

DROP TABLE IF EXISTS `TM3_ID`;
CREATE TABLE `TM3_ID` (
    `tableName` varchar(128) NOT NULL,
    `nextId` bigint(20) NOT NULL DEFAULT 0,
    PRIMARY KEY (`tableName`)
) ENGINE=MyISAM DEFAULT CHARSET=UTF8;


