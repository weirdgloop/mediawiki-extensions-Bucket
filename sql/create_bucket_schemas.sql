CREATE TABLE bucket_schemas (
	`bucket_name` VARCHAR(255) NOT NULL,
	`schema_json` TEXT NOT NULL,
	PRIMARY KEY (`bucket_name`)
);

-- Create entry for bucket_message Bucket
CREATE TABLE `bucket__bucket_message` (
  `_page_id` int NOT NULL,
  `_index` int NOT NULL,
  `page_name` text,
  `page_name_sub` text,
  `bucket` text,
  `property` text,
  `type` text,
  `message` text,
  PRIMARY KEY (`_page_id`,`_index`),
  KEY `page_name` (`page_name`(255)),
  KEY `page_name_sub` (`page_name_sub`(255)),
  KEY `bucket` (`bucket`(255)),
  KEY `property` (`property`(255)),
  KEY `type` (`type`(255)),
  KEY `message` (`message`(255))
);
INSERT INTO bucket_schemas (`bucket_name`, `schema_json`) VALUE ("bucket_message", '{\"_page_id\":{\"type\":\"INTEGER\",\"index\":false,\"repeated\":false},\"_index\":{\"type\":\"INTEGER\",\"index\":false,\"repeated\":false},\"page_name\":{\"type\":\"PAGE\",\"index\":true,\"repeated\":false},\"page_name_sub\":{\"type\":\"PAGE\",\"index\":true,\"repeated\":false},\"bucket\":{\"type\":\"PAGE\",\"index\":true,\"repeated\":false},\"property\":{\"type\":\"TEXT\",\"index\":true,\"repeated\":false},\"type\":{\"type\":\"TEXT\",\"index\":true,\"repeated\":false},\"message\":{\"type\":\"TEXT\",\"index\":true,\"repeated\":false}}');
