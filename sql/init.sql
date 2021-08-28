CREATE TABLE `t_sequence` (
  `f_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `f_cmd` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  `f_status` int unsigned NOT NULL DEFAULT '0' COMMENT '0:pending,1: accept,2:reject',
  `f_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`f_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- TODO data type
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

-- initializing a new trading pair should create a table like t_clearing_result
-- CREATE TABLE `t_clearing_result_{base_currency_code}_{quote_currency_code}` like `t_clearing_result`;
