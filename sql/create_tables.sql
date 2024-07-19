CREATE TABLE bucket__drops (
	_page_id INT NOT NULL,
	monster_name VARCHAR(255),
	item_name VARCHAR(255),
	quantity INT,
	rarity INT
);

CREATE TABLE bucket_schemas (
	`table_name` VARCHAR(255) NOT NULL,
	`schema_json` TEXT NOT NULL,
	PRIMARY KEY (`table_name`)
);