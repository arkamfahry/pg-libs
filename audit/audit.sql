create schema if not exists audit;

create extension if not exists "uuid-ossp";

create table if not exists audit.record_version
(
    id            bigserial primary key,
    new_record_id uuid,
    old_record_id uuid,
    operation     varchar(8)  not null,
    timestamp     timestamptz not null default now(),
    table_oid     oid         not null,
    table_schema  name        not null,
    table_name    name        not null,
    new_record    jsonb,
    old_record    jsonb
);

create index if not exists record_version_timestamp_idx
    on audit.record_version
        using brin (timestamp);

create index if not exists record_version_table_oid_idx
    on audit.record_version
        using btree (table_oid);

create or replace function audit.primary_key_columns(entity_oid oid)
    returns text[]
    stable
    security definer
    language sql
as $$
    select
        coalesce(
            array_agg(pa.attname::text order by pa.attnum),
            array[]::text[]
        ) column_names
    from
        pg_index pi
        join pg_attribute pa
            on pi.indrelid = pa.attrelid
            and pa.attnum = any(pi.indkey)
    where
        indrelid = $1
        and indisprimary
$$;

create or replace function audit.to_record_id(
		entity_oid oid,
		pkey_cols text[],
		rec jsonb
)
    returns uuid
    stable
    language sql
as $$
    select
        case
            when rec is null then null
						-- if no primary key exists, use a random uuid
            when pkey_cols = array[]::text[] then gen_random_uuid()
            else (
                select
                    uuid_generate_v5(
                        'fd62bc3d-8d6e-43c2-919c-802ba3762271',
                        (
													jsonb_build_array(to_jsonb($1))
													|| jsonb_agg($3 ->> key_)
												)::text
                    )
                from
                    unnest($2) x(key_)
            )
        end
$$;

create index if not exists record_version_new_record_id on audit.record_version (new_record_id)
where new_record_id is not null;

create index if not exists record_version_old_record_id on audit.record_version (old_record)
where old_record_id is not null;

create or replace function audit.insert_update_delete_trigger()
    returns trigger
    security definer
    language plpgsql
as $$
declare
    pkey_cols text[] = audit.primary_key_columns(TG_RELID);
    new_record_jsonb jsonb = to_jsonb(new);
    new_record_id uuid = audit.to_record_id(TG_RELID, pkey_cols, new_record_jsonb);
    old_record_jsonb jsonb = to_jsonb(old);
    old_record_id uuid = audit.to_record_id(TG_RELID, pkey_cols, old_record_jsonb);
begin

    insert into audit.record_version(
        new_record_id,
        old_record_id,
        operation,
        table_oid,
        table_schema,
        table_name,
        new_record,
        old_record
    )
    select
        new_record_id,
        old_record_id,
        TG_OP,
        TG_RELID,
        TG_TABLE_SCHEMA,
        TG_TABLE_NAME,
        new_record_jsonb,
        old_record_jsonb;

    return coalesce(new, old);
end;
$$;

create or replace function audit.enable_tracking(regclass)
    returns void
    volatile
    security definer
    language plpgsql
as $$
declare
    statement_row text = format('
        create trigger audit_insert_update_delete_operations
            before insert or update or delete
            on %I
            for each row
            execute procedure audit.insert_update_delete_trigger();',
        $1
    );

    pkey_cols text[] = audit.primary_key_columns($1);
begin
    if pkey_cols = array[]::text[] then
        raise exception 'Table % can not be audited because it has no primary key', $1;
    end if;

    if not exists(select 1 from pg_trigger where tgrelid = $1 and tgname = 'audit_insert_update_delete_operations') then
        execute statement_row;
    end if;
end;
$$;

create or replace function audit.disable_tracking(regclass)
    returns void
    volatile
    security definer
    language plpgsql
as $$
declare
    statement_row text = format(
        'drop trigger if exists audit_insert_update_delete_operations on %I;',
        $1
    );
begin
    execute statement_row;
end;
$$;
