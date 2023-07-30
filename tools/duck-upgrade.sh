#!/bin/sh -eu

if [ "$#" -ne 4 ] ; then
  echo "Usage: $(basename "$0") duckdb-cli-old duckdb-cli-new database-old database-new"
  exit 1
fi

OLD=$1
NEW=$2
OLDDB=$3
NEWDB=$4

$OLD --version >/dev/null
$NEW --version >/dev/null

TEMP=$(mktemp -d) || exit 2
trap 'rm -rf "$TEMP"' EXIT

set -x

$OLD "$OLDDB" -c "EXPORT DATABASE '$TEMP' (FORMAT PARQUET);"
$NEW "$NEWDB" -c "IMPORT DATABASE '$TEMP';"

[ "$(uname -s)" = "Linux" ] && chmod --reference="$OLDDB" "$NEWDB"
