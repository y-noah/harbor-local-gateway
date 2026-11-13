SELECT jsonb_build_object(
 'columns',(SELECT jsonb_agg(jsonb_build_array(a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull,a.attcollation::text,pg_get_expr(d.adbin,d.adrelid)) ORDER BY a.attnum) FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum WHERE a.attrelid=$1 AND a.attnum>0 AND NOT a.attisdropped),
 'constraints',(SELECT jsonb_agg(value ORDER BY value::text) FROM (SELECT jsonb_build_array(contype,pg_get_constraintdef(oid),condeferrable,condeferred,convalidated) value FROM pg_constraint WHERE conrelid=$1) x),
 'indexes',(SELECT jsonb_agg(value ORDER BY value::text) FROM (SELECT jsonb_build_array(indisunique,indisprimary,indkey::text,indclass::text,indcollation::text,pg_get_expr(indexprs,indrelid),pg_get_expr(indpred,indrelid)) value FROM pg_index WHERE indrelid=$1) x)
) value
