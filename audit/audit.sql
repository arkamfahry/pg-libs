-- Namespace to "audit"
create schema if not exists audit;


-- Create enum type for SQL operations to reduce disk/memory usage vs text
do $$
begin
    -- Create the 'audit.operation' enum type only if it doesn't exist
    if not exists (select 1 from pg_type where typname = 'operation' and typnamespace = (select oid from pg_namespace where nspname = 'audit')) then
        create type audit.operation as enum (
            'INSERT',
            'UPDATE',
            'DELETE',
            'TRUNCATE'
        );
    end if;
end $$;


create table if not exists audit.record_version
(
    -- unique auto-incrementing id
    id            bigserial primary key,
    -- uniquely identifies a record by primary key [primary key + table_oid]
    new_record_id uuid,
    -- uniquely identifies a record before update/delete
    old_record_id uuid,
    -- INSERT/UPDATE/DELETE/TRUNCATE/SNAPSHOT
    operation     audit.operation not null,
    timestamp     timestamptz     not null default (now()),
    table_oid     oid             not null,
    table_schema  name            not null,
    table_name    name            not null,

    -- contents of the current record
    new_record    jsonb,
    -- previous record contents for UPDATE/DELETE
    old_record    jsonb,

    -- at least one of new_record_id or old_record_id is populated, except for truncates
    check (coalesce(new_record_id, old_record_id) is not null or operation = 'TRUNCATE'),

    -- new_record_id must be populated for insert and update
    check (operation in ('INSERT', 'UPDATE') = (new_record_id is not null)),
    check (operation in ('INSERT', 'UPDATE') = (new_record is not null)),

    -- old_record must be populated for update and delete
    check (operation in ('UPDATE', 'DELETE') = (old_record_id is not null)),
    check (operation in ('UPDATE', 'DELETE') = (old_record is not null))
);

create index if not exists record_version_record_id
    on audit.record_version (new_record_id)
    where new_record_id is not null;


create index if not exists record_version_old_record_id
    on audit.record_version (old_record_id)
    where old_record_id is not null;


create index if not exists record_version_ts
    on audit.record_version
        using brin (timestamp);


create index if not exists record_version_table_oid
    on audit.record_version (table_oid);


create or replace function audit.primary_key_columns(entity_oid oid)
    returns text[]
    stable
    security definer
    set search_path = ''
    language sql
as
$$
    -- Looks up the names of a table's primary key columns
select coalesce(
               array_agg(pa.attname::text order by pa.attnum),
               array []::text[]
       ) column_names
from pg_index pi
         join pg_attribute pa
              on pi.indrelid = pa.attrelid
                  and pa.attnum = any (pi.indkey)

where indrelid = $1
  and indisprimary
$$;


create or replace function audit.to_record_id(entity_oid oid, pkey_cols text[], rec jsonb)
    returns uuid
    stable
    language sql
as
$$
select case
           when rec is null then null
           when pkey_cols = array []::text[] then uuid_generate_v4()
           else (select uuid_generate_v5(
                                'fd62bc3d-8d6e-43c2-919c-802ba3762271',
                                (jsonb_build_array(to_jsonb($1)) || jsonb_agg($3 ->> key_))::text
                        )
                 from unnest($2) x(key_))
           end
$$;


create or replace function audit.insert_update_delete_trigger()
    returns trigger
    security definer
    -- can not use search_path = '' here because audit.to_record_id requires
    -- uuid_generate_v4, which may be installed in a user-defined schema
    language plpgsql
as
$$
declare
    pkey_cols        text[] = audit.primary_key_columns(TG_RELID);
    record_jsonb     jsonb  = to_jsonb(new);
    record_id        uuid   = audit.to_record_id(TG_RELID, pkey_cols, record_jsonb);
    old_record_jsonb jsonb  = to_jsonb(old);
    old_record_id    uuid   = audit.to_record_id(TG_RELID, pkey_cols, old_record_jsonb);
begin

    insert into audit.record_version(new_record_id,
                                     old_record_id,
                                     operation,
                                     table_oid,
                                     table_schema,
                                     table_name,
                                     new_record,
                                     old_record)
    select record_id,
           old_record_id,
           TG_OP::audit.operation,
           TG_RELID,
           TG_TABLE_SCHEMA,
           TG_TABLE_NAME,
           record_jsonb,
           old_record_jsonb;

    return coalesce(new, old);
end;
$$;


create or replace function audit.truncate_trigger()
    returns trigger
    security definer
    set search_path = ''
    language plpgsql
as
$$
begin
    insert into audit.record_version(operation,
                                     table_oid,
                                     table_schema,
                                     table_name)
    select TG_OP::audit.operation,
           TG_RELID,
           TG_TABLE_SCHEMA,
           TG_TABLE_NAME;

    return coalesce(old, new);
end;
$$;


create or replace function audit.enable_tracking(schema_table regclass)
    returns void
    volatile
    security definer
    set search_path = ''
    language plpgsql
as
$$
declare
    statement_row  text   = format('
        create trigger audit_insert_update_delete
            after insert or update or delete
            on %s
            for each row
            execute procedure audit.insert_update_delete_trigger();',
                                   schema_table
                            );
    statement_stmt text   = format('
        create trigger audit_truncate
            after truncate
            on %s
            for each statement
            execute procedure audit.truncate_trigger();',
                                   $1
                            );
    pkey_cols      text[] = audit.primary_key_columns(schema_table);
begin
    if pkey_cols = array []::text[] then
        raise exception 'Table % can not be audited because it has no primary key', schema_table;
    end if;

    if not exists(select 1 from pg_trigger where tgrelid = schema_table and tgname = 'audit_insert_update_delete') then
        execute statement_row;
    end if;

    if not exists(select 1 from pg_trigger where tgrelid = schema_table and tgname = 'audit_truncate') then
        execute statement_stmt;
    end if;
end;
$$;


create or replace function audit.disable_tracking(schema_table regclass)
    returns void
    volatile
    security definer
    set search_path = ''
    language plpgsql
as
$$
declare
    statement_row  text = format(
            'drop trigger if exists audit_insert_update_delete on %s;',
            schema_table
                          );
    statement_stmt text = format(
            'drop trigger if exists audit_truncate on %s;',
            schema_table
                          );
begin
    execute statement_row;
    execute statement_stmt;
end;
$$;