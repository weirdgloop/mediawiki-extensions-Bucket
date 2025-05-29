CREATE TABLE bucket_pages (
	`_page_id` INT NOT NULL,
	`table_name` VARCHAR(255) NOT NULL,
	`put_hash` VARCHAR(255) NOT NULL,
	PRIMARY KEY (`_page_id`,`table_name`)
);
