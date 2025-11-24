package cmd

const defaultTableSchema = "public"

const leafPartitionBaseCTE = `
WITH leaf_partitions AS (
	SELECT
		child.relname::text AS tablename,
		child.oid AS table_oid
	FROM pg_inherits i
		JOIN pg_class child ON child.oid = i.inhrelid
		JOIN pg_namespace child_ns ON child_ns.oid = child.relnamespace
		JOIN pg_class parent ON parent.oid = i.inhparent
		JOIN pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace
	WHERE parent_ns.nspname = $1
		AND child_ns.nspname = $1
		AND parent.relname = $2
		AND child.relkind = 'r'
		AND NOT EXISTS (
			SELECT 1 FROM pg_inherits WHERE inhparent = child.oid
		)
)`

const leafPartitionListSQL = leafPartitionBaseCTE + `
SELECT tablename
FROM leaf_partitions
ORDER BY tablename;
`

const leafPartitionExistsSQL = leafPartitionBaseCTE + `
SELECT EXISTS (
	SELECT 1 FROM leaf_partitions
);
`

const leafPartitionPermissionSQL = leafPartitionBaseCTE + `
SELECT tablename
FROM leaf_partitions
WHERE has_table_privilege(table_oid, 'SELECT')
LIMIT 1;
`

// nonPartitionTableListSQL finds regular tables (not partitions) that match
// the partition naming pattern. This is useful when tables follow partition
// naming conventions but aren't actually PostgreSQL partitions.
const nonPartitionTableListSQL = `
SELECT t.tablename::text
FROM pg_tables t
	JOIN pg_class c ON c.relname = t.tablename
	JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE t.schemaname = $1
	AND n.nspname = $1
	AND t.tablename LIKE $2 || '_%'
	AND t.tablename != $2
	AND c.relkind = 'r'
	AND NOT EXISTS (
		SELECT 1 FROM pg_inherits WHERE inhrelid = c.oid
	)
ORDER BY t.tablename;
`
