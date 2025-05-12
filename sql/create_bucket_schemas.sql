CREATE TABLE bucket_schemas (
	`table_name` VARCHAR(255) NOT NULL,
	`backing_table_name` VARCHAR(255),
	`schema_json` TEXT NOT NULL,
	PRIMARY KEY (`table_name`)
);