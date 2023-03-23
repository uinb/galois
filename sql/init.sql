CREATE TABLE `t_sequence` (
  `f_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `f_cmd` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  `f_status` int unsigned NOT NULL DEFAULT '0' COMMENT '0:pending,1: accept,2:reject',
  `f_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`f_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

CREATE TABLE IF NOT EXISTS `t_proof` (
  `f_event_id` bigint unsigned NOT NULL,
  `f_proof` blob NOT NULL,
  PRIMARY KEY (`f_event_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- DEPRECATED

CREATE TABLE `t_clearing_result` (
  `f_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `f_event_id` bigint unsigned NOT NULL,
  `f_order_id` bigint unsigned NOT NULL,
  `f_user_id` varchar(66) NOT NULL,
  `f_status` int unsigned NOT NULL,
  `f_role` int unsigned NOT NULL,
  `f_ask_or_bid` int unsigned NOT NULL,
  `f_price` varchar(20) NOT NULL,
  `f_quote_delta` varchar(20) NOT NULL,
  `f_base_delta` varchar(20) NOT NULL,
  `f_quote_available` varchar(20) NOT NULL,
  `f_base_available` varchar(20) NOT NULL,
  `f_quote_frozen` varchar(20) NOT NULL,
  `f_base_frozen` varchar(20) NOT NULL,
  `f_quote_charge` varchar(18) NOT NULL,
  `f_base_charge` varchar(18) NOT NULL,
  `f_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`f_id`),
  UNIQUE KEY `f_event_id` (`f_event_id`,`f_order_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

CREATE TABLE IF NOT EXISTS `t_stick` (
  `f_id` bigint NOT NULL,
  `f_open` varchar(20) NOT NULL,
  `f_close` varchar(20) NOT NULL,
  `f_high` varchar(20) NOT NULL,
  `f_low` varchar(20) NOT NULL,
  `f_amount` varchar(32) NOT NULL,
  `f_vol` varchar(36) NOT NULL,
  `f_last_cr` bigint NOT NULL,
  PRIMARY KEY (`f_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

CREATE TABLE IF NOT EXISTS `t_order` (
  `f_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `f_version` bigint unsigned NOT NULL default 0,
  `f_user_id` varchar(66) NOT NULL,
  `f_amount` decimal(20,8) NOT NULL,
  `f_price` decimal(20,8) NOT NULL,
  `f_order_type` int unsigned NOT NULL,
  `f_timestamp` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `f_status` int NOT NULL DEFAULT '0',
  `f_base_fee` decimal(5,4) NOT NULL DEFAULT '0.0000',
  `f_quote_fee` decimal(5,4) NOT NULL DEFAULT '0.0000',
  `f_last_cr` bigint NOT NULL DEFAULT 0,
  `f_matched_quote_amount` decimal(20,8) NOT NULL DEFAULT '0.00000000',
  `f_matched_base_amount` decimal(20,8) NOT NULL DEFAULT '0.00000000',
  PRIMARY KEY (`f_id`),
  KEY `idx_user_id` (`f_user_id`),
  KEY `idx_user_id_and_status` (`f_user_id`, `f_status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- initializing a new trading pair should create a table like t_clearing_result
-- CREATE TABLE `t_clearing_result_{base_currency_code}_{quote_currency_code}` like `t_clearing_result`;
