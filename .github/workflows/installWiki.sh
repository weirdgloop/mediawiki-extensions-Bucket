#! /bin/bash

MW_BRANCH=$1

wget https://github.com/wikimedia/mediawiki/archive/$MW_BRANCH.tar.gz -nv

tar -zxf $MW_BRANCH.tar.gz
mv mediawiki-$MW_BRANCH mediawiki

cd mediawiki

composer install

# Temporarily commented out since we don't run any unit tests right now
: <<'COMMENT'
php maintenance/install.php --dbtype sqlite --dbuser root --dbname mw --dbpath $(pwd) --pass AdminPassword WikiName AdminUser

# echo 'error_reporting(E_ALL| E_STRICT);' >> LocalSettings.php
# echo 'ini_set("display_errors", 1);' >> LocalSettings.php
echo '$wgShowExceptionDetails = true;' >> LocalSettings.php
echo '$wgShowDBErrorBacktrace = true;' >> LocalSettings.php
echo '$wgDevelopmentWarnings = true;' >> LocalSettings.php

echo 'wfLoadExtension( "Bucket" );' >> LocalSettings.php

cat <<EOT >> composer.local.json
{
  "require": {

  },
	"extra": {
		"merge-plugin": {
			"merge-dev": true,
			"include": []
		}
	}
}
EOT

# Download phpunit.xml.dist or phpunit.xml.template as they're not in the tarballs
# Taken from https://github.com/StarCitizenTools/mediawiki-ci-workflows/blob/main/.github/workflows/test-php.yml
wget "https://raw.githubusercontent.com/wikimedia/mediawiki/${MW_BRANCH}/phpunit.xml.dist" -nv || \
  wget "https://raw.githubusercontent.com/wikimedia/mediawiki/${MW_BRANCH}/phpunit.xml.template" -nv
COMMENT
