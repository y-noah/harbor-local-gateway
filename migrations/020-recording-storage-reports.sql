CREATE TABLE harbor.recording_storage_reports(
 singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK(singleton),
 sampled_at BIGINT NOT NULL,expires_at BIGINT NOT NULL,
 payload JSONB NOT NULL CHECK(jsonb_typeof(payload)='object' AND octet_length(payload::text)<=16384),
 signature TEXT NOT NULL CHECK(signature~'^[a-f0-9]{64}$'),
 CHECK(sampled_at>=0 AND expires_at>sampled_at AND expires_at-sampled_at<=30000),
 CHECK(payload->>'sampled_at'=sampled_at::text AND payload->>'expires_at'=expires_at::text)
);
REVOKE ALL ON harbor.recording_storage_reports FROM PUBLIC;
CREATE FUNCTION harbor.validate_recording_storage_report() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE at_ms BIGINT:=floor(extract(epoch FROM clock_timestamp())*1000)::bigint;
BEGIN
 IF NEW.sampled_at>at_ms OR NEW.expires_at<=at_ms THEN
  RAISE EXCEPTION 'storage report is outside its database time window' USING ERRCODE='23514';
 END IF;
 IF TG_OP='UPDATE' AND (NEW.sampled_at<OLD.sampled_at OR NEW.sampled_at=OLD.sampled_at AND (NEW.payload<>OLD.payload OR NEW.signature<>OLD.signature)) THEN
  RAISE EXCEPTION 'storage report cannot replace a newer sample' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER recording_storage_report_monotonic BEFORE INSERT OR UPDATE ON harbor.recording_storage_reports
 FOR EACH ROW EXECUTE FUNCTION harbor.validate_recording_storage_report();
