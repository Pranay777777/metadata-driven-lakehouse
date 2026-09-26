-- ---------------------------------------------------------------------
-- Metadata control plane — mssql
--
-- GENERATED FILE. Do not edit.
-- Source of truth: src/lakehouse/metadata/models.py
-- Regenerate with: python scripts/generate_ddl.py
-- ---------------------------------------------------------------------

CREATE TABLE pipeline_run (
	run_id VARCHAR(64) NOT NULL, 
	pipeline_name VARCHAR(200) NOT NULL, 
	triggered_by VARCHAR(200) NULL, 
	status VARCHAR(20) NOT NULL, 
	started_at DATETIMEOFFSET NOT NULL DEFAULT CURRENT_TIMESTAMP, 
	ended_at DATETIMEOFFSET NULL, 
	PRIMARY KEY (run_id), 
	CONSTRAINT ck_status CHECK (status IN ('running', 'succeeded', 'failed', 'skipped', 'quarantined'))
);

CREATE TABLE source_system (
	id INTEGER NOT NULL IDENTITY, 
	name VARCHAR(100) NOT NULL, 
	kind VARCHAR(20) NOT NULL, 
	secret_name VARCHAR(200) NULL, 
	active BIT NOT NULL, 
	created_at DATETIMEOFFSET NOT NULL DEFAULT CURRENT_TIMESTAMP, 
	updated_at DATETIMEOFFSET NOT NULL DEFAULT CURRENT_TIMESTAMP, 
	PRIMARY KEY (id), 
	CONSTRAINT ck_kind CHECK (kind IN ('jdbc', 'rest', 'file', 'object_store')), 
	UNIQUE (name)
);

CREATE TABLE source_object (
	id INTEGER NOT NULL IDENTITY, 
	source_system_id INTEGER NOT NULL, 
	schema_name VARCHAR(100) NOT NULL, 
	object_name VARCHAR(200) NOT NULL, 
	target_path VARCHAR(400) NOT NULL, 
	load_strategy VARCHAR(20) NOT NULL, 
	incremental_column VARCHAR(100) NULL, 
	primary_key_columns VARCHAR(400) NULL, 
	cdc_operation_column VARCHAR(100) NULL, 
	cdc_delete_value VARCHAR(20) NOT NULL, 
	watermark_grace INTEGER NOT NULL, 
	load_order INTEGER NOT NULL, 
	active BIT NOT NULL, 
	scd2_enabled BIT NOT NULL, 
	freshness_sla_minutes INTEGER NULL, 
	owner VARCHAR(200) NULL, 
	created_at DATETIMEOFFSET NOT NULL DEFAULT CURRENT_TIMESTAMP, 
	updated_at DATETIMEOFFSET NOT NULL DEFAULT CURRENT_TIMESTAMP, 
	PRIMARY KEY (id), 
	CONSTRAINT uq_source_object UNIQUE (source_system_id, schema_name, object_name), 
	CONSTRAINT ck_load_strategy CHECK (load_strategy IN ('full', 'incremental', 'cdc')), 
	CONSTRAINT ck_incremental_needs_column CHECK (load_strategy <> 'incremental' OR incremental_column IS NOT NULL), 
	CONSTRAINT ck_cdc_needs_primary_key CHECK (load_strategy <> 'cdc' OR primary_key_columns IS NOT NULL), 
	FOREIGN KEY(source_system_id) REFERENCES source_system (id) ON DELETE CASCADE
);

CREATE INDEX ix_source_object_active_order ON source_object (active, load_order);

CREATE TABLE column_metadata (
	id INTEGER NOT NULL IDENTITY, 
	source_object_id INTEGER NOT NULL, 
	column_name VARCHAR(200) NOT NULL, 
	sensitivity VARCHAR(20) NOT NULL, 
	business_description TEXT NULL, 
	created_at DATETIMEOFFSET NOT NULL DEFAULT CURRENT_TIMESTAMP, 
	updated_at DATETIMEOFFSET NOT NULL DEFAULT CURRENT_TIMESTAMP, 
	PRIMARY KEY (id), 
	CONSTRAINT uq_column_metadata UNIQUE (source_object_id, column_name), 
	CONSTRAINT ck_sensitivity CHECK (sensitivity IN ('none', 'internal', 'pii', 'sensitive_pii')), 
	FOREIGN KEY(source_object_id) REFERENCES source_object (id) ON DELETE CASCADE
);

CREATE TABLE dq_rule (
	id INTEGER NOT NULL IDENTITY, 
	source_object_id INTEGER NOT NULL, 
	rule_type VARCHAR(30) NOT NULL, 
	column_name VARCHAR(200) NULL, 
	expression TEXT NULL, 
	severity VARCHAR(20) NOT NULL, 
	active BIT NOT NULL, 
	created_at DATETIMEOFFSET NOT NULL DEFAULT CURRENT_TIMESTAMP, 
	updated_at DATETIMEOFFSET NOT NULL DEFAULT CURRENT_TIMESTAMP, 
	PRIMARY KEY (id), 
	CONSTRAINT ck_rule_type CHECK (rule_type IN ('not_null', 'unique', 'range', 'allowed_values', 'regex', 'freshness', 'row_count', 'custom_sql')), 
	CONSTRAINT ck_severity CHECK (severity IN ('warn', 'quarantine', 'fail')), 
	FOREIGN KEY(source_object_id) REFERENCES source_object (id) ON DELETE CASCADE
);

CREATE INDEX ix_dq_rule_object ON dq_rule (source_object_id, active);

CREATE TABLE load_watermark (
	source_object_id INTEGER NOT NULL, 
	watermark_value VARCHAR(100) NOT NULL, 
	watermark_type VARCHAR(20) NOT NULL, 
	committed_run_id VARCHAR(64) NULL, 
	updated_at DATETIMEOFFSET NOT NULL DEFAULT CURRENT_TIMESTAMP, 
	PRIMARY KEY (source_object_id), 
	CONSTRAINT ck_watermark_type CHECK (watermark_type IN ('timestamp', 'integer', 'string')), 
	FOREIGN KEY(source_object_id) REFERENCES source_object (id) ON DELETE CASCADE
);

CREATE TABLE object_dependency (
	id INTEGER NOT NULL IDENTITY, 
	source_object_id INTEGER NOT NULL, 
	depends_on_id INTEGER NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT uq_object_dependency UNIQUE (source_object_id, depends_on_id), 
	CONSTRAINT ck_no_self_dependency CHECK (source_object_id <> depends_on_id), 
	FOREIGN KEY(source_object_id) REFERENCES source_object (id) ON DELETE CASCADE, 
	FOREIGN KEY(depends_on_id) REFERENCES source_object (id) ON DELETE CASCADE
);

CREATE TABLE schema_version (
	id INTEGER NOT NULL IDENTITY, 
	source_object_id INTEGER NOT NULL, 
	version INTEGER NOT NULL, 
	schema_json TEXT NOT NULL, 
	schema_hash VARCHAR(64) NOT NULL, 
	is_current BIT NOT NULL, 
	captured_at DATETIMEOFFSET NOT NULL DEFAULT CURRENT_TIMESTAMP, 
	PRIMARY KEY (id), 
	CONSTRAINT uq_schema_version UNIQUE (source_object_id, version), 
	FOREIGN KEY(source_object_id) REFERENCES source_object (id) ON DELETE CASCADE
);

CREATE INDEX ix_schema_version_current ON schema_version (source_object_id, is_current);

CREATE TABLE task_run (
	id INTEGER NOT NULL IDENTITY, 
	run_id VARCHAR(64) NOT NULL, 
	source_object_id INTEGER NOT NULL, 
	layer VARCHAR(20) NOT NULL, 
	status VARCHAR(20) NOT NULL, 
	rows_read INTEGER NOT NULL, 
	rows_written INTEGER NOT NULL, 
	rows_rejected INTEGER NOT NULL, 
	watermark_from VARCHAR(100) NULL, 
	watermark_to VARCHAR(100) NULL, 
	error_message TEXT NULL, 
	started_at DATETIMEOFFSET NOT NULL DEFAULT CURRENT_TIMESTAMP, 
	ended_at DATETIMEOFFSET NULL, 
	duration_seconds INTEGER NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT ck_status CHECK (status IN ('running', 'succeeded', 'failed', 'skipped', 'quarantined')), 
	CONSTRAINT ck_layer CHECK (layer IN ('bronze', 'silver', 'gold')), 
	FOREIGN KEY(run_id) REFERENCES pipeline_run (run_id) ON DELETE CASCADE, 
	FOREIGN KEY(source_object_id) REFERENCES source_object (id) ON DELETE CASCADE
);

CREATE INDEX ix_task_run_object_time ON task_run (source_object_id, started_at);

CREATE TABLE dq_result (
	id INTEGER NOT NULL IDENTITY, 
	task_run_id INTEGER NOT NULL, 
	dq_rule_id INTEGER NOT NULL, 
	passed BIT NOT NULL, 
	failed_row_count INTEGER NOT NULL, 
	detail TEXT NULL, 
	evaluated_at DATETIMEOFFSET NOT NULL DEFAULT CURRENT_TIMESTAMP, 
	PRIMARY KEY (id), 
	FOREIGN KEY(task_run_id) REFERENCES task_run (id) ON DELETE CASCADE, 
	FOREIGN KEY(dq_rule_id) REFERENCES dq_rule (id) ON DELETE CASCADE
);

CREATE INDEX ix_dq_result_task ON dq_result (task_run_id);
