CREATE TABLE IF NOT EXISTS bohm_queue_{{ .Name }} (
	id BIGSERIAL PRIMARY KEY,
	pop_count SMALLINT NOT NULL DEFAULT 0,
	concurrent_req BOOLEAN NOT NULL DEFAULT false,
	visibility_time TIMESTAMP NOT NULL DEFAULT NOW(),
	row_id BIGINT NOT NULL UNIQUE
);

CREATE INDEX IF NOT EXISTS idx_bohm_visibility_time ON bohm_queue_{{ .Name }} (visibility_time);
CREATE INDEX IF NOT EXISTS idx_bohm_row_id ON bohm_queue_{{ .Name }} (row_id);
CREATE INDEX IF NOT EXISTS idx_bohm_concurrent_req ON bohm_queue_{{ .Name }} (concurrent_req);
CREATE INDEX IF NOT EXISTS idx_bohm_queue_id ON bohm_queue_{{ .Name }} (id);
CREATE INDEX IF NOT EXISTS idx_bohm_pop_count ON bohm_queue_{{ .Name }} (pop_count);

{{- if .Table }}
CREATE OR REPLACE FUNCTION bohm_enqueue_from_{{ .Table }}_to_{{ .Name }}() RETURNS trigger AS $bohm_enqueue$
 BEGIN
	IF EXISTS (SELECT FROM bohm_queue_{{ .Name }} WHERE row_id = NEW.{{ .PKeyColumn }}) THEN
		UPDATE bohm_queue_{{ .Name }} SET concurrent_req = true WHERE row_id = NEW.{{ .PKeyColumn }};
	ELSE
		INSERT INTO bohm_queue_{{ .Name }} (row_id) VALUES (NEW.{{ .PKeyColumn }}) ON CONFLICT (row_id) DO UPDATE SET pop_count = 0;
	END IF;
	RETURN NEW;
 END;
$bohm_enqueue$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER bohm_enqueue_insert_{{ .Table }}_to_{{ .Name }}
AFTER INSERT ON {{ .Table }}
FOR EACH ROW EXECUTE FUNCTION bohm_enqueue_from_{{ .Table }}_to_{{ .Name }}();

CREATE OR REPLACE TRIGGER bohm_enqueue_update_{{ .Table }}_to_{{ .Name }}
AFTER UPDATE ON {{ .Table }}
FOR EACH ROW WHEN ({{ .RenderFilters }}) EXECUTE FUNCTION bohm_enqueue_from_{{ .Table }}_to_{{ .Name }}();

CREATE OR REPLACE TRIGGER bohm_enqueue_delete_{{ .Table }}_to_{{ .Name }}
AFTER DELETE ON {{ .Table }}
FOR EACH ROW EXECUTE FUNCTION bohm_enqueue_from_{{ .Table }}_to_{{ .Name }}();
{{- else }}
INSERT INTO bohm_queue_{{ .Name }} (row_id) VALUES (0) ON CONFLICT (row_id) DO NOTHING;
{{- end }}

CREATE OR REPLACE FUNCTION bohm_notify_{{ .Name }}() RETURNS trigger AS $bohm_notify$
 BEGIN
	NOTIFY bohm_writes_{{ .Name }};
	RETURN NEW;
 END;
$bohm_notify$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER bohm_notify_{{ .Name }}
AFTER INSERT OR UPDATE ON bohm_queue_{{ .Name }}
FOR EACH ROW EXECUTE FUNCTION bohm_notify_{{ .Name }}();

CREATE OR REPLACE FUNCTION bohm_timetravel_{{ .Name }}() RETURNS table (visibility_time interval) AS $bohm_timetravel$
	SELECT MIN(visibility_time) - NOW() FROM bohm_queue_{{ .Name }};
$bohm_timetravel$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION bohm_pop_{{ .Name }}(lockTTL interval) RETURNS table (row_id bigint, pop_count bigint) AS $bohm_pop$
    UPDATE bohm_queue_{{ .Name }} AS hq SET pop_count = hq.pop_count + 1, visibility_time = (NOW() + $1)
           WHERE id = (
                   SELECT id
                   FROM bohm_queue_{{ .Name }}
                   WHERE NOW() > visibility_time
                   ORDER BY id
                   FOR UPDATE SKIP LOCKED
                   LIMIT 1
           )
    RETURNING hq.row_id, hq.pop_count;
$bohm_pop$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION bohm_requeue_{{ .Name }}(requeueAfter interval, row_id bigint) RETURNS void AS $bohm_requeue$
 BEGIN
	IF EXISTS (SELECT FROM bohm_queue_{{ .Name }} AS hq WHERE hq.row_id = $2 AND hq.concurrent_req = true) THEN
		UPDATE bohm_queue_{{ .Name }} AS hq SET visibility_time = NOW(), pop_count = 0, concurrent_req = false WHERE hq.row_id = $2;
	ELSE
		UPDATE bohm_queue_{{ .Name }} AS hq SET visibility_time = (NOW() + $1) WHERE hq.row_id = $2;
	END IF;
 END;
$bohm_requeue$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bohm_drop_{{ .Name }}(row_id bigint) RETURNS void AS $bohm_drop$
 BEGIN
	IF EXISTS (SELECT FROM bohm_queue_{{ .Name }} AS hq WHERE hq.row_id = $1 AND hq.concurrent_req = true) THEN
		UPDATE bohm_queue_{{ .Name }} AS hq SET visibility_time = NOW(), pop_count = 0, concurrent_req = false WHERE hq.row_id = $1;
	ELSE
		DELETE FROM bohm_queue_{{ .Name }} AS hq WHERE hq.row_id = $1;
	END IF;
 END;
$bohm_drop$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bohm_keepalive_{{ .Name }}(row_id bigint, pop_count bigint, lockTTL interval) RETURNS void AS $bohm_keepalive$
	UPDATE bohm_queue_{{ .Name }} AS hq SET visibility_time = (NOW() - lockTTL) WHERE hq.row_id = $1 AND hq.pop_count = pop_count;
$bohm_keepalive$ LANGUAGE sql;
