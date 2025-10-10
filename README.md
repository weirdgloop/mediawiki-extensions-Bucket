# Bucket
Simple MediaWiki extension to store and retrieve structured data on articles.

Bucket was developed as a lightweight alternative to [Semantic MediaWiki](https://www.semantic-mediawiki.org/wiki/Semantic_MediaWiki), as there are several issues with SMW that can cause issues on wikis that use it quite extensively like the [RuneScape Wiki](https://runescape.wiki). For example, SMW frequently writes to the database even when no content is changed, has its own purging mechanism, and requires users to learn quite complicated syntax to use.

Bucket aims to be simpler in nature, by providing a very straight forward interface for wiki editors to store structured data, an easy-to-learn SQL-like syntax in Lua for accessing the data, and crucially, not redeveloping the wheel and riding on top of existing MediaWiki concepts where possible (such as RDBMS access, purging, etc).

## Requirements
* MediaWiki 1.43+
* MySQL 8.0.17+.
  * MariaDB, PostgreSQL and SQLite are currently not supported.
* [Scribunto](https://github.com/wikimedia/mediawiki-extensions-Scribunto)

## Installing
Bucket is still under active development, and the API and schema are unstable and may change at any time. We strongly recommend not using Bucket in production yet, but if you do, please let us know, so we can let you know about breaking changes when they happen.

1. Enable the extension using `wfLoadExtension( 'Bucket' );`
2. Run `update.php` to create the required database tables
3. Create a new database user for Bucket, e.g
```sql
CREATE USER 'bucket'@'<SERVER_HOSTNAME>' IDENTIFIED BY '<PASSWORD>';
```
4. Ensure the configuration variables are set correctly (see table below)
5. Run `php maintenance/run.php Bucket:SetupDBPermission` to setup required database permissions

## Configuration
 | Variable | Description | Default
 |----------|-------------|---------|
 | $wgBucketDBuser | The username Bucket will use to connect to the database. | None
 | $wgBucketDBpassword | The password for the Bucket database user. | None
 | $wgBucketDBhostname | The hostname for the Bucket database user. | `%`
 | $wgBucketMaxQueryExecutionTime | The maximum time in milliseconds that an individual query is allowed to run before timing out. | `500`
 | $wgBucketMaxPageExecutionTime | The maximum time in milliseconds that Bucket is allowed to execute per page parse. | `10,000`
 | $wgBucketMaxDataPerPage | The maximum amount of data storable by a single page, counted as the length of the JSON encoded data. | `1,000,000`
 | $wgBucketWriteEnabledNamespaces | An array of namespaces that Bucket will write data from. | (Main), User, Project, File, Help, and Category

## Usage
 [For detailed usage, see this page](https://meta.weirdgloop.org/w/Extension:Bucket).
