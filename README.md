# Bucket
MediaWiki extension to store and retrieve structured data on articles.

## Dependencies
Requires MediaWiki 1.43+ and MySQL 8.0.17+

## Installing
1. Enable the extension using `wfLoadExtension( 'Bucket' );'
2. Run `update.php` to create the required database tables
3. Create a new database user for Bucket, set the apropriate configuration variables (see table below)
4. Run `Bucket:SetupDBPermission`

## Configuration
 | Variable | Description | Default
 |----------|-------------|---------|
 | $wgBucketDBuser | The username Bucket will use to connect to the database. | None
 | $wgBucketDBpassword | The password for the Bucket database user. | None
 | $wgBucketDBhostname | The hostname for the Bucket database user. | `%`
 | $wgBucketMaxExecutionTime | The maximum time in milliseconds that an individual query is allowed to run before timing out. | `500`
 | $wgBucketWriteEnabledNamespaces | An array of namespaces that Bucket will write data from. | (Main), User, Project, File, Help, and Category

 ## Usage
 [See this page for usage information](https://meta.runescape.wiki/w/User:Cook_Me_Plox/Bucket)